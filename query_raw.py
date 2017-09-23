import re
import pandas as pd
# from test.test_data import msgs 

def query_raw(q, store, limit=-1):
    '''
    Query IMAT for raw HL7 messages

    Args:
        q: string
            an IMAT query
        store: string
        limit: int

    Returns:
        List of raw HL7 messages
    '''
    hits_raw = Query(
        q,
        store=store,
        limit=limit,
        fields="rawrecord"
    ).raw_execute().decode("utf8")

    msgs = re.findall(r'(?si)<OriginalHL7>(.*?)</OriginalHL7>', hits_raw)
    return msgs

def check_lens_equal(*args):
    '''
    Ensure list lengths are equal
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
    Flatten lists nested one level deep. Empty nested lists (i.e. sublists)
    are not perserved.

    Example:
        >>> flatten([[1, 2], [3, 4], [5, 6]])
        [1, 2, 3, 4, 5, 6]

        >>> flatten([[1, 2], [3, 4], []])
        [1, 2, 3, 4]

        >>> flatten([[1, 2], [3, 4], [5, 6, [7, 8]]])
        [1, 2, 3, 4, 5, 6, [7, 8]]

    Args:
        l: list

    Returns:
        A list
    '''
    return [item for sublist in lst for item in sublist]

def zip_nested_lists(lst1, lst2):
    '''
    Zip nested lists.

    Examples:
        >>> zip_nested_lists([['a', 'b']], [['y', 'z']])
        [[('a', 'y'), ('b', 'z')]]

        >>> zip_nested_lists([['a', 'b'],['c', 'd']], [['w', 'x'],['y', 'z']])
        [[('a', 'w'), ('b', 'x')], [('c', 'y'), ('d', 'z')]]

    Args:
        l1: list
            Length and length of nested lists must be the same length as l2.
        l2: list
            Length and length of nested lists must be the same length as l1.

    Returns:
        List of nested lists whose elements are tuples.
    '''
    check_nested_lens_equal(lst1, lst2)
    return [list(zip(lst1[i], lst2[i])) for i in range(len(lst1))]

def concat_fields(lst_parsed):
    '''
    Examples:
        >>> concat_fields([[['a', 'b']], [['y', 'z']], [['s', 't']]])
        ['a,y,s', 'b,z,t']

        >>> concat_fields([[['a', 'b']]])
        ['a', 'b']

    '''
    if len(lst_parsed) == 1 or not bool(lst_parsed[1]):
        # either a single parsed field or the appended field is empty, in which
        # case the first element is the concatinated lists
        return flatten(lst_parsed[0])
    else:
        zipped = zip_nested_lists(lst_parsed[0], lst_parsed[1])
        concatted = [[",".join(pair) for pair in sublist] for sublist in zipped]
        to_concat = [concatted, flatten(lst_parsed[2:])]
        return concat_fields(to_concat)

def parse_msg_id(fields, msgs):
    '''
    Parse message ids from raw HL7 messages.

    The message identifier is a concatination of each field, returning a
    single string per message. The returned string has the syntax
    <field_1>,<field_2>,... Its value should be unique as it is used to join
    different data elements within a message.

    Example:
        >>> get_msg_id(msgs)
        ['Facility1,68188,1719801063', Facility2,588229,1721309017', ... ]

    Args:
        fields: List(string)
        msgs: List(string)

    Returns:
        List(string)
    '''
    fields_parsed = list(map(
        parse_msgs,
        fields,
        [msgs for i in enumerate(fields)]
    ))

    msg_ids = concat_fields(fields_parsed)
    return msg_ids

def parse_loc(loc_txt):
    '''
    Parse an HL7 message location

    Examples:
        >>> parse_loc('PR1.3')
        {'seg': 'PR1', 'comp': 3, 'depth': 2}

        >>> parse_loc('DG1.3.1')
        {'seg': 'DG1', 'comp': 3, 'subcomp': 1, 'depth': 3}

    Args:
        loc_txt: string

    Returns:
        Dictionary of attributes and parsed elements of location
    '''
    loc = {}
    loc['depth'] = len(loc_txt.split("."))

    if loc['depth'] not in [2, 3]:
        raise ValueError(
            "Syntax of location must be either '<segment>.<component>' or"
            "'<segment>.<component>.<subcomponent>'"
        )

    loc_split = loc_txt.split(".")

    loc['seg'] = loc_split[0]
    loc['comp'] = int(loc_split[1])

    if loc['depth'] == 3:
        loc['subcomp'] = int(loc_split[2])

    return loc

def parse_el(loc_txt):
    '''
    Parse HL7 messages at a given location.

    Example:
        >>> msg = '...AL1|3|DA|1545^MORPHINE^99HIC|||20080828|||\n...'
        >>> parse_allergy_type = parse_element("AL1.2")
        >>> parse_allergy_type(msg)
        >>> ['DA']

        >>> parse_allergy_code_text = parse_element("AL1.3.2")
        >>> parse_allergy_code_text(msg)
        >>> ['MORPHINE']

        >>> msg_2 = '...AL1|3|DA|1545^MORPHINE^99HIC|||20080828|||\nAL1|4|DA|1550^CODEINE^99HIC|||20101027|||\n'
        >>> parse_allergy_code_text(msg_2)
        >>> ['MORPHINE', 'CODEINE']

    Args:
        loc (string): HL7 location to be parsed.
            Must be a single location represented by a segment and 1 or 2
            subsequent components. A period (".") must separate segments and
            components (ex. "PR1.3" or "DG1.3.1")

    Returns:
        Function to parse HL7 message at the given location
    '''
    loc = parse_loc(loc_txt)

    def parser(msg):
        segs = re.findall(loc['seg'] + '\|.*(?=\\n)', msg)
        data = []
        # TODO: functional approach
        for seg in segs:
            if loc['depth'] == 2:
                datum = seg.split("|")[loc['comp']]
            else:
                try:
                    comp = seg.split("|")[loc['comp']]
                    datum = comp.split("^")[loc['subcomp'] - 1]
                except IndexError:
                    datum = ''          # empty component (i.e. '||')
            data.append(datum)
        return data
    return parser

def zip_msg_ids(lst, msg_ids):
    '''
    Zip but also ensures list lengths are equal.
    '''
    check_lens_equal(msg_ids, lst)
    zipped = list(zip(msg_ids, lst))
    return zipped

def parse_msgs(field, msgs):
    return list(map(parse_el(field), msgs))

def to_df(lst, field):
    df = pd.DataFrame.from_dict(
        dict(lst),
        orient="index"
    )

    n_cols = range(len(df.columns))
    df.columns = ["seg_{n}".format(n=n) for n in n_cols]

    df["msg_id"] = df.index
    df = pd.melt(df, id_vars=["msg_id"])
    df.rename(
        columns = {
            "variable": "seg",
            "value": field
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
            how = "inner",
            on=["msg_id", "seg"],
            sort=False
        )
        dfs_to_join = dfs[2:]
        dfs_to_join.append(df_join)
        return join_dfs(dfs_to_join)

def build_report(fields, msg_ids, msgs):
    parsed_fields = list(map(
        parse_msgs,
        fields,
        [msgs for i in enumerate(fields)]
    ))

    parsed_fields_w_msg_ids = list(map(
        zip_msg_ids,
        parsed_fields,
        [msg_ids for i in enumerate(fields)]
    ))

    field_dfs = list(map(
        to_df,
        parsed_fields_w_msg_ids,
        fields
    ))

    df = join_dfs(field_dfs)

    df.dropna(
        axis=0,
        how='all',
        subset=fields,
        inplace=True
    )
    return df

def main():

    # TODO: msg_ids and report fields can be dict of field:col_name

    query_broad = '()s.sending_facility:"COVH"()NOT ()s.attending_doctor_id: IN("1952","2620","2489","1131","1604","3654","2216","1265","2511","3071","3050","3557","746","52118","2507","52715","51071","1080","2716","1948","51644","1321","1066")()s.patient_class:"Inpatient"()s.patient_type: IN("IPB","IPF","IPL","IPM","IPP","IPR","IPS")()d.discharge_time:[2017-08-06 TO 2017-09-20]()s.record_type:"ADT A08"'
    # msgs = query_raw(query_broad, 'ADTs', 20)

    # replace ids w/ msg_id_fields
    msg_ids = parse_msg_id(
        ['PID.3.4', 'PID.3.1', 'PID.18.1'], 
        msgs
    )

    # replace fields w/ report_fields
    df = build_report(
        ['DG1.3.1', 'DG1.3.2', 'DG1.6', 'DG1.15'], 
        msg_ids,
        msgs
    )

    print(df)

# main()