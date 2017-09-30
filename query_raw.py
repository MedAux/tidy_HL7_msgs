# TODO
# - input args as dictionary of field:col_name
# - flexible parsing using msg separators

# TO TEST:
# - run 'pytest -s' to disable capturing of stdout

import re
import itertools
import pandas as pd

def query_raw(query, store, limit=-1):
    '''
    Query IMAT for raw HL7 messages

    Args:
        q: string
            An IMAT query
        store: string
        limit: int
            Default value of -1 returns all hits

    Returns:
        List of raw HL7 messages
    '''
    hits_raw = Query(
        query,
        store=store,
        limit=limit,
        fields="rawrecord"
    ).raw_execute().decode("utf8")

    msgs = re.findall(r'(?si)<OriginalHL7>(.*?)</OriginalHL7>', hits_raw)
    return msgs

def check_lens_equal(*args):
    '''
    Ensure the length of lists are equal
    '''
    lengths = [len(x) for x in args]
    are_all_equal = len(set(lengths)) == 1
    if not are_all_equal:
        raise RuntimeError(
            "Length of lists are not equal.  Lists of unequal lengths "
            "invalidate the relationship between data elements of the zipped"
            "lists "
        )

def check_nested_lens_equal(lst1, lst2):
    '''
    Ensure the lengths of nested lists are equal
    '''
    check_lens_equal(lst1, lst2)
    for i in range(len(lst1)):
        if len(lst1[i]) != len(lst2[i]):
            raise RuntimeError(
                "Length of lists are not equal.  Lists of unequal lengths "
                "invalidate the relationship between data elements of the zipped"
                "lists "
            )

def flatten(lst):
    '''
    Flatten lists nested one level deep.

    Empty nested lists are not perserved.

    Example:
        >>> flatten([[1, 2], [3, 4], [5, 6]])
        [1, 2, 3, 4, 5, 6]

        >>> flatten([[1, 2], [3, 4], []])
        [1, 2, 3, 4]

        >>> flatten([[1, 2], [3, 4], [5, 6, [7, 8]]])
        [1, 2, 3, 4, 5, 6, [7, 8]]

    Args:
        lst: list

    Returns:
        A list
    '''
    return [item for sublist in lst for item in sublist]

def zip_nested(lst1, lst2):
    '''
    Zip nested lists.

    The length of the two lists must be equal.

    Examples:
        >>> zip_nested([['a', 'b']], [['y', 'z']])
        [[('a', 'y'), ('b', 'z')]]

        >>> zip_nested([['a', 'b'],['c', 'd']], [['w', 'x'],['y', 'z']])
        [[('a', 'w'), ('b', 'x')], [('c', 'y'), ('d', 'z')]]

    Args:
        lst1: list
        lst2: list

    Returns:
        List of nested lists whose elements are tuples.
    '''
    check_nested_lens_equal(lst1, lst2)
    return [list(zip(lst1[i], lst2[i])) for i in range(len(lst1))]

def concat(lsts):
    '''
    Concatinate lists of strings

    Examples:
        >>> concat([[['a', 'b']], [['y', 'z']], [['s', 't']]])
        ['a,y,s', 'b,z,t']

        >>> concat([[['a', 'b']]])
        ['a', 'b']

    Args:
        lsts: list of lists(strings)

    Returns:
        List(strings)
    '''
    if len(lsts) == 1 or not bool(lsts[1]):
        # either a single parsed field or the appended field is empty, in which
        # case the first element is the concatinated lists
        return flatten(lsts[0])
    else:
        zipped = zip_nested(lsts[0], lsts[1])
        concatted = [[",".join(pair) for pair in sublist] for sublist in zipped]
        to_concat = [concatted, flatten(lsts[2:])]
        return concat(to_concat)

def parse_msgs(field_txt, msgs):
    '''
    Parse messages for a given field

    Examples:
        >>> msg1 = '...AL1|3|DA|1545^MORPHINE^99HIC|||20080828|||\n...'
        >>> msg2 = '...AL1|1|DRUG|00000741^OXYCODONE||HYPOTENSION\n...'
        >>> parse_msgs("AL1.3.1", [msg1, msg2])
        >>> [['1545'], ['00000741']]

        >>> # multiple segments per message
        >>> msg3 = '...AL1|1|DRUG|00000741^OXYCODONE||HYPOTENSION\n...AL1|2|DRUG|00001433^TRAMADOL||SEIZURES~VOMITING\n'
        >>> parse_msgs("AL1.3.1", [msg1, msg3])
        >>> [['1545'], ['00000741', '00001433']]

    Args:
        field_txt: string
            Field to parse

            Must be a single location represented by a segment, a component
            and an optional subcomponent. A period (".") must separate
            segments, components, and subcomponents (ex. "PR1.3" or
            "DG1.3.1")

        msgs: list
            List of message to parse

    Returns:
        List of List(string)
    '''
    return list(map(parse_msg(field_txt), msgs))

def parse_msg(field_txt):
    '''
    Higher-order function to parse a field from an HL7 message

    Assumes field separator is a pipe ('|') and component separator is a
    caret ('^')

    Example:
        >>> msg = '...AL1|3|DA|1545^MORPHINE^99HIC|||20080828|||\n...'
        >>> parse_allergy_type = parse_msg("AL1.2")
        >>> parse_allergy_type(msg)
        >>> ['DA']

        >>> parse_allergy_code_text = parse_msg("AL1.3.2")
        >>> parse_allergy_code_text(msg)
        >>> ['MORPHINE']

        >>> msg_2 = '...AL1|3|DA|1545^MORPHINE^99HIC|||20080828|||\nAL1|4|DA|1550^CODEINE^99HIC|||20101027|||\n'
        >>> parse_allergy_code_text(msg_2)
        >>> ['MORPHINE', 'CODEINE']

    Args:
        field_txt: string
            Field to parse

            Must be a single location represented by a segment and 1 or 2
            subsequent components. A period (".") must separate segments and
            components (ex. "PR1.3" or "DG1.3.1")

    Returns:
        Function to parse HL7 message at the given location
    '''
    field = parse_field_txt(field_txt)

    def parser(msg):
        segs = re.findall(field['seg'] + '\|.*(?=\\n)', msg)
        data = []
        for seg in segs:
            comp = seg.split("|")[field['comp']]
            if field['depth'] == 2:
                datum = comp
            else:
                try:
                    datum = comp.split("^")[field['subcomp']]
                except IndexError:
                    datum = ''
            data.append(datum)
        return data
    return parser

def parse_msg_id(fields, msgs):
    '''
    Parse message ids from raw HL7 messages.

    The message identifier is a concatination of each field value, which must
    be a single value for each message (i.e. the field must be for a segment
    only is found once in a message). Returns a single string per message.
    The returned string has the syntax <field_1>,<field_2>,... Its value must
    be unique because it is used to join different data elements within a
    message.

    Example:
        >>> get_msg_id(msgs)
        ['Facility1,68188,1719801063', Facility2,588229,1721309017', ... ]

    Args:
        fields: List(string)
        msgs: List(string)

    Returns:
        List(string)
    '''
    parsed_fields = list(map(
        parse_msgs,
        fields,
        itertools.repeat(msgs)
    ))

    # NOTE: too restrictive?
    is_field_single_ids = (
        [all([len(ids) == 1 for ids in field]) for field in parsed_fields]
    )
    if not all(is_field_single_ids):
        raise RuntimeError(
            "One or more ID fields have multiple values per message: {fields}. ".format(
                fields=", ".join(itertools.compress(fields, is_field_single_ids))
            )
        )

    concatted = concat(parsed_fields)

    if len(set(concatted)) != len(msgs):
        raise RuntimeError("Messages IDs are not unique")

    return concatted

def parse_field_txt(field_txt):
    '''
    Parse the string value of an HL7 message field

    Examples:
        >>> parse_field_txt('PR1.3')
        {'seg': 'PR1', 'comp': 3, 'depth': 2}

        >>> parse_field_txt('DG1.3.1')
        {'seg': 'DG1', 'comp': 3, 'subcomp': 0, 'depth': 3}

    Args:
        field_txt: string
            Field name

    Returns:
        Dictionary of field attributes and parsed elements
    '''
    field = {}
    field['depth'] = len(field_txt.split("."))

    if field['depth'] not in [2, 3]:
        raise ValueError(
            "Syntax of location must be either '<segment>.<component>' or"
            "'<segment>.<component>.<subcomponent>'"
        )

    loc_split = field_txt.split(".")

    field['seg'] = loc_split[0]
    field['comp'] = int(loc_split[1])

    if field['seg'] == "MSH":
        field['comp'] -= 1

    if field['depth'] == 3:
        field['subcomp'] = int(loc_split[2]) - 1

    return field

def zip_msg_ids(lst, msg_ids):
    '''
    Zip but ensure list lengths are equal
    '''
    check_lens_equal(msg_ids, lst)
    return list(zip(msg_ids, lst))

def to_df(lst, field_txt):
    '''
    Convert parsed values to dataframe

    Args:
        lst: list
            List of tuples, where the first element is the message ID and the
            second element is a list of parsed values

        field_txt: string
            Field name

    Example:
        >>> to_df(
        ...    [('msg_id1', ['val1']), ('msg_id2', ['val1', 'val2'])],
        ...    "field_name")
        ... )
           msg_id   seg        field_name
        0  msg_id1  seg_0      val1
        1  msg_id2  seg_0      val1
        2  msg_id1  seg_1      None
        3  msg_id2  seg_1      val2

    '''
    df = pd.DataFrame.from_dict(
        dict(lst),
        orient="index"
    )

    n_cols = range(len(df.columns))
    df.columns = ["seg_{n}".format(n=n) for n in n_cols]

    df["msg_id"] = df.index
    df = pd.melt(df, id_vars=["msg_id"])
    df.rename(
        columns={
            "variable": "seg",
            "value": field_txt
        },
        inplace=True
    )
    return df

def join_dfs(dfs):
    '''
    Join dataframes
    '''
    if len(dfs) == 1:
        return dfs[0]
    else:
        df_join = pd.merge(
            dfs[0],
            dfs[1],
            how="inner",
            on=["msg_id", "seg"],
            sort=False
        )
        dfs_to_join = dfs[2:]
        dfs_to_join.append(df_join)
        return join_dfs(dfs_to_join)

def main(id_fields, report_fields, msgs):
    '''
    Parse and tidy fields from HL7 messages

    Args:
        id_fields: list(string)
            Fields to uniquely identify a message. Fields can be from
            different message segments, but each field must return in one
            value per message.

        report_fields: list(string)
            Fields to report.  Fields must be from the same segments.

        msgs: list(string)
            List of HL7 messages.

    Returns:
        A pandas dataframe, whose rows are message segments. Columns are
        message id fields, segment number, and reported fields.
    '''
    report_field_segs = [re.match('\w*', field).group() for field in report_fields]
    if len(set(report_field_segs)) != 1:
        raise RuntimeError("Reported fields must be from the same segment")

    parsed_report_fields = map(
        parse_msgs,
        report_fields,
        itertools.repeat(msgs)
    )

    msg_ids = parse_msg_id(id_fields, msgs)

    report_fields_w_ids = map(
        zip_msg_ids,
        parsed_report_fields,
        itertools.repeat(msg_ids)
    )

    field_dfs = list(map(
        to_df,
        report_fields_w_ids,
        report_fields
    ))

    df = join_dfs(field_dfs)

    df.dropna(
        axis=0,
        how='all',
        subset=report_fields,
        inplace=True
    )

    id_cols = df['msg_id'].str.split(",", expand=True)
    id_cols.columns = id_fields
    df_w_id_cols = pd.concat([id_cols, df], axis=1).drop('msg_id', axis=1)

    return df_w_id_cols
