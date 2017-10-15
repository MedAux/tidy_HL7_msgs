# pylint: disable = C0103, R1705, C0200, W1401, W0511

# TODO
# - check...() accepted style?
# - flexible parsing using msg separators
# - refactor query_raw() to streaming

# TO TEST:
# - run 'pytest -s' to disable capturing of stdout

import re
import itertools
import pandas as pd
import numpy as np

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

def are_lens_equal(*args):
    '''
    Are lengths equal

    Example:
        >>> are_lens_equal([1, 2], [1, 2])
        True
        >>> are_lens_equal([1, 2], [1, 2, 3])
        False

    Args:
        *args: one or more lists

    Returns:
        Boolean
    '''
    lens = [len(x) for x in args]
    return len(set(lens)) == 1

def are_nested_lens_equal(lst1, lst2):
    '''
    Are nested lengths equal

    Example:
        >>> are_nested_lens_equal(
        ...     [[1, 2], [1, 2], [1, 2]],
        ...     [[1, 2], [1, 2], [1, 2]]
        ... )
        True
        >>> are_nested_lens_equal(
        ...     [[1, 2], [1, 2], [1, 2]],
        ...     [[1, 2], [1, 2], [1, 2, 3]]
        ... )
        >>> are_nested_lens_equal(
        False
    Args:
        lst1: list
        lst2: list

    Returns:
        Boolean
    '''
    assert are_lens_equal(lst1, lst2), "List lengths are not equal"
    return all([len(lst1[i]) == len(lst2[i]) for i in range(len(lst1))]) is True

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
        lst1: list(string)
        lst2: list(string)

    Returns:
        list(list(tuple))
    '''
    assert are_nested_lens_equal(lst1, lst2), "Nested list lengths are not equal"
    return [list(zip(lst1[i], lst2[i])) for i in range(len(lst1))]

def concat(lsts):
    '''
    Concatinate lists of strings

    Examples:
        >>> concat([[['a', 'y']], [['b', 'z']]])
        ['a,b', 'y,z']

        >>> concat([[['a', 'w']], [['b', 'x']], [['c', 'y']], [['d', 'z']]])
        ['a,b,c,d', 'w,x,y,z']

    Args:
        lsts: list(list(list(string)))

    Returns:
        list(string)
    '''
    lsts = [flatten(lst) for lst in lsts]

    lst_lens = [len(lst) for lst in lsts]
    assert len(set(lst_lens)) == 1, "Message ID fields are unequal length"

    concatted = []
    for i in range(len(lsts[0])):
        concatted.append([",".join(el[i] for el in lsts)])

    return flatten(concatted)

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
            Field to parse. Must be a single location represented by a
            segment, a component and an optional subcomponent. A period (".")
            must separate segments, components, and subcomponents (ex.
            "PR1.3" or "DG1.3.1")

        msgs: list(string)
            Messages to parse

    Returns:
        list(list(string))
    '''
    field = parse_field_txt(field_txt)
    parser = get_parser(field)
    return list(map(parser, msgs))

def get_parser(field):
    '''
    Higher-order function to parse a field from an HL7 message

    Assumes field separator is a pipe ('|') and component separator is a
    caret ('^')

    Example:
        >>> msg = '...AL1|3|DA|1545^MORPHINE^99HIC|||20080828|||\n...'
        >>> parse_allergy_type = get_parser("AL1.2")
        >>> parse_allergy_type(msg)
        >>> ['DA']

        >>> parse_allergy_code_text = get_parser("AL1.3.2")
        >>> parse_allergy_code_text(msg)
        >>> ['MORPHINE']

        >>> msg_2 = '...AL1|3|DA|1545^MORPHINE^99HIC|||20080828|||\nAL1|4|DA|1550^CODEINE^99HIC|||20101027|||\n'
        >>> parse_allergy_code_text(msg_2)
        >>> ['MORPHINE', 'CODEINE']

    Args:
        field_txt: dict
            Field attributes and values

    Returns:
        Function to parse HL7 message at a given field
    '''
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
                    datum = np.nan
            data.append(datum)
        return data
    return parser

def parse_msg_id(fields, msgs):
    '''
    Parse message IDs from raw HL7 messages.

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
        fields: list(string)
        msgs: list(string)

    Returns:
        list(string)

    Raises:
        RuntimeError if a field has multiple values
        RuntimeError if message IDs are not unique
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
    Parse literal HL7 message field (i.e. 'DG1.3.1')

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

    Raises:
        ValueError if field syntax is incorrect
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
    Zip after checking list lengths are equal

    Args:
        lst: list
        msg_ids: list(string)

    Returns:
        list(tuple)
    '''
    assert are_lens_equal(msg_ids, lst), "List lengths are not equal"
    return list(zip(msg_ids, lst))

def to_df(lst, field_txt):
    '''
    Convert list to dataframe

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

    Args:
        lst: list
            List of tuples, where the first element is the message ID and the
            second element is a list of parsed values

        field_txt: string
            Field name

    Returns:
        dataframe
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
    Join a list of dataframes

    Args:
        dfs: list(dataframes)

    Returns:
        dataframe
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

def are_segs_identical(fields):
    '''
    Check if all fields are from the same segment

    Example:
    >>> are_segs_identical(['DG1.3.1', 'DG1.3.2', 'DG1.6'])
    True
    >>> are_segs_identical(['DG1.3.1', 'DG1.3.2', 'PID.3.4'])
    False

    Args:
        fields: list(string)

    Returns:
        boolean
    '''
    segs = [re.match('\w*', field).group() for field in fields]
    return len(set(segs)) == 1

def tidy_HL7_msg_segs(msg_id_fields, report_fields, msgs):
    '''
    Parse and tidy fields from HL7 messages

    Args:
        id_fields: list or able to be converted to one

            Fields to uniquely identify a message. Fields can be from
            different message segments, but each field must return in one
            value per message.

            If argument is a dict-like, its keys must be HL7 field(s) to
            parse and values will be column names for the returned dataframe.

        report_fields: list or able to be converted to one

            Fields to report.  Fields must be from the same segments.

            If argument is dict-like, its keys must be HL7 field(s) to
            parse and values will be column names for the returned dataframe.

        msgs: list(string)
            List of HL7 messages.

    Returns:
        Dataframe whose rows are message segments and whose columns are
        message id fields, segment number, and reported fields.

    Raises:
        ValueError if all fields are not from the same segment
    '''
    if not are_segs_identical(report_fields):
        raise ValueError("All fields must be from the same segment")

    # parse message ids
    msg_ids = parse_msg_id(list(msg_id_fields), msgs)

    # parse report fields
    report_fields_vals = map(
        parse_msgs,
        list(report_fields),
        itertools.repeat(msgs)
    )

    # zip report field values and message ids
    report_fields_vals_w_msg_ids = map(
        zip_msg_ids,
        report_fields_vals,
        itertools.repeat(msg_ids)
    )

    # convert to dataframes
    dfs = list(map(
        to_df,
        report_fields_vals_w_msg_ids,
        report_fields
    ))

    # join dataframes
    df = join_dfs(dfs)

    # remove segments lacking data
    df.dropna(
        axis=0,
        how='all',
        subset=report_fields,
        inplace=True
    )

    # split concatted fields used for a message id into individual columns;
    # combine this id dataframe with that for reported fields, dropping
    # old (i.e. concatted) message id column
    id_cols = df['msg_id'].str.split(",", expand=True)
    id_cols.columns = msg_id_fields
    df_w_id_cols = pd.concat([id_cols, df], axis=1).drop('msg_id', axis=1)

    # TODO: more specific exception handling?
    try:
        df_w_id_cols.rename(columns=msg_id_fields, inplace=True)
        df_w_id_cols.rename(columns=report_fields, inplace=True)
    except Exception:
        pass

    return df_w_id_cols
