# TODO
# - check that all fields are from the same segment
# - sep msg ID into fields 
# - input args as dictionary of field:col_name

# To test:
# - pytest: comment out 'import msgs' and main()
# - example output: keep 'import msgs' and main(); load module; reload module
#     w/ 'import imp', imp.reload(query_raw) 

from test.test_data import msgs 
from itertools import repeat
import re
import pandas as pd

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

def zip_nested(lst1, lst2):
    '''
    Zip nested lists.

    Examples:
        >>> zip_nested([['a', 'b']], [['y', 'z']])
        [[('a', 'y'), ('b', 'z')]]

        >>> zip_nested([['a', 'b'],['c', 'd']], [['w', 'x'],['y', 'z']])
        [[('a', 'w'), ('b', 'x')], [('c', 'y'), ('d', 'z')]]

    Args:
        lst1: list
            Length and length of nested lists must be the same length as lst2.
        lst2: list
            Length and length of nested lists must be the same length as lst1.

    Returns:
        List of nested lists whose elements are tuples.
    '''
    check_nested_lens_equal(lst1, lst2)
    return [list(zip(lst1[i], lst2[i])) for i in range(len(lst1))]

def concat(lsts):
    '''
    Examples:
        >>> concat([[['a', 'b']], [['y', 'z']], [['s', 't']]])
        ['a,y,s', 'b,z,t']

        >>> concat([[['a', 'b']]])
        ['a', 'b']

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
    TODO
    '''
    return list(map(parse_msg(field_txt), msgs))

def parse_msg(field_txt):
    '''
    TODO: problem in past parsing MSH segment?

    Higher-order function to parse a field from an HL7 message

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
        field: string
            HL7 field to be parsed.

            Must be a single location represented by a segment and 1 or 2
            subsequent components. A period (".") must separate segments and
            components (ex. "PR1.3" or "DG1.3.1")

    Returns:
        Function to parse HL7 message at the given location
    '''
    field = parse_field(field_txt)

    def parser(msg):
        segs = re.findall(field['seg'] + '\|.*(?=\\n)', msg)
        data = []
        # TODO: functional approach; DRY
        for seg in segs:
            if field['depth'] == 2:
                datum = seg.split("|")[field['comp']]
            else:
                try:
                    comp = seg.split("|")[field['comp']]
                    datum = comp.split("^")[field['subcomp']]
                except IndexError:
                    datum = ''
            data.append(datum)
        return data
    return parser

def parse_msg_id(fields, msgs):
    '''
    Parse message ids from raw HL7 messages.

    The message identifier is a concatination of each field, returning a
    single string per message. The returned string has the syntax
    <field_1>,<field_2>,... Its value should be unique because it is used to
    join different data elements within a message.

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
        repeat(msgs)
    ))

    return concat(fields_parsed)

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

    if field['seg'] == "MSH":
        field['comp'] = int(loc_split[1]) - 1
    else:
        field['comp'] = int(loc_split[1])

    if field['depth'] == 3:
        field['subcomp'] = int(loc_split[2]) - 1

    return field

def zip_msg_ids(lst, msg_ids):
    '''
    Zip but ensures list lengths are equal.
    '''
    check_lens_equal(msg_ids, lst)
    return list(zip(msg_ids, lst))

def to_df(lst, field_txt):
    '''
    TODO
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
        columns = {
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
            how = "inner",
            on=["msg_id", "seg"],
            sort=False
        )
        dfs_to_join = dfs[2:]
        dfs_to_join.append(df_join)
        return join_dfs(dfs_to_join)

def parse_report_fields_and_tidy(fields, msg_ids, msgs):
    '''
    TODO
    '''
    parsed_fields = map(
        parse_msgs,
        fields,
        repeat(msgs)
    )

    parsed_fields_w_msg_ids = map(
        zip_msg_ids,
        parsed_fields,
        repeat(msg_ids)
    )

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

    # query_broad = '()s.sending_facility:"COVH"()NOT ()s.attending_doctor_id: IN("1952","2620","2489","1131","1604","3654","2216","1265","2511","3071","3050","3557","746","52118","2507","52715","51071","1080","2716","1948","51644","1321","1066")()s.patient_class:"Inpatient"()s.patient_type: IN("IPB","IPF","IPL","IPM","IPP","IPR","IPS")()d.discharge_time:[2017-08-06 TO 2017-09-20]()s.record_type:"ADT A08"'
    # msgs = query_raw(query_broad, 'ADTs', 20)

    # replace ids w/ msg_id_fields
    msg_ids = parse_msg_id(
        ['PID.3.4', 'PID.3.1', 'PID.18.1'], 
        msgs
    )

    # replace fields w/ report_fields
    df = parse_report_fields_and_tidy(
        ['DG1.3.1', 'DG1.3.2', 'DG1.6', 'DG1.15'], 
        msg_ids,
        msgs
    )

    print(df)
    return df

main()