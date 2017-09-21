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
    Ensure the lengths two or more lists are equal
    '''
    lengths = [len(x) for x in args]
    is_all_equal = len(set(lengths)) == 1
    if not is_all_equal:
        raise RuntimeError(
            "Length of lists are not equal.  Lists of unequal length will "
            "invalidate the relationship of data elements between lists "
            "when zipping"
        )

def check_nested_lens_equal(lst1, lst2):
    '''
    Ensure the lengths of nested lists are equal
    '''
    check_lens_equal(lst1, lst2)
    for i in range(0, len(lst1)):
        if len(lst1[i]) != len(lst2[i]):
            raise RuntimeError(
                "Length of nested lists is not equal.  Nested lists of unequal "
                "length will invalidate the relationship between data elements "
                "from the same segment."
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
        l1: List(String)
            Length and length of nested lists must be the same length as l2.
        l2: List(String)
            Length and length of nested lists must be the same length as l1.

    Returns:
        List of nested lists whose elements are tuples.
    '''
    check_nested_lens_equal(lst1, lst2)
    return [list(zip(lst1[i], lst2[i])) for i in range(0, len(lst1))]

def concat_fields(lst_parsed):
    '''
    Examples: 
    >>> concat_fields2([[['a', 'b']]])
    ['a', 'b']
    >>> concat_fields2([[['a', 'b']], [['y', 'z']], [['s', 't']]])
    ['a,y,s', 'b,z,t']
    '''
    if len(lst_parsed) == 1 or not bool(lst_parsed[1]):
        # either list of single parsed field or appended field is empty 
        # (i.e the first element in list is complete concatination) 
        return flatten(lst_parsed[0])
    else:
        zipped = zip_nested_lists(lst_parsed[0], lst_parsed[1])
        concatted = [[",".join(pair) for pair in sublist] for sublist in zipped]
        to_concat = [concatted, flatten(lst_parsed[2:])]
        return concat_fields(to_concat)

# def concat_fields(*fields):
#     '''
#     Concatinate nested list of strings
#     NOTE: Input is likely a list of fields and this function may not be appropriate 
#     Examples:
#         >>> concat_fields([['a', 'b']])
#         ['a', 'b']

#         >>> concat_fields([['a', 'b']], [['y', 'z']])
#         [['a,y', 'b,z']]

#         >>> concat_fields([['a', 'b'],['c', 'd']], [['w', 'x'],['y', 'z']])
#         [['a,w', 'b,x'], ['c,y', 'd,z']]

#         >>> concat_fields([['a', 'b']], [['y', 'z']], [['s', 't']])
#         ['a,y,s', 'b,z,t']

#     Args:
#         TODO: List of strings

#     Returns:
#         List of nested lists whose elements are concatinated.

#     Note:
#         In the context of parsed HL7 messages, each nested list corresponds
#         to a message and each element corresponds to data parsed from a
#         message segment.
#     '''
#     if len(fields) == 1 or not bool(fields[1]):
#         # single field passed or second arg (i.e 'to_concat') is emtpy list
#         return flatten(fields[0])
#     else:
#         zipped = zip_nested_lists(fields[0], fields[1])
#         concatted = [[",".join(pair) for pair in sublist] for sublist in zipped]
#         to_concat = flatten([f for f in fields[2:]])
#         return concat_fields(concatted, to_concat)

def parse_msg_id(fields, msgs):
    '''
    Parse message identifier from raw HL7 messages.

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
    def parse_msg(field):
        return list(map(parse_el(field), msgs))
    # fields_parsed = fields.map(lambda field: list(map(parse_el(field), msgs)))
    fields_parsed = list(map(parse_msg, fields))
    msg_ids = concat_fields(fields_parsed)

    return msg_ids

def parse_el(loc):
    '''
    Parse HL7 messages at a given location.

    Example:
        >>> msg = '...AL1|3|DA|1545^MORPHINE^99HIC|||20080828|||\n...'
        >>> parse_allergy_type = parse_element("AL1.2")
        >>> parse_allergy_type(msg)
        >>> ['DA']
        >>>
        >>> parse_allergy_code_text = parse_element("AL1.3.2")
        >>> parse_allergy_code_text(msg)
        >>> ['MORPHINE']
        >>>
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
    # TODO: separate function?
    depth = len(loc.split("."))
    if depth == 2:
        segment, comp = loc.split(".")
    elif depth == 3:
        segment, comp, subcomp = loc.split(".")
        subcomp = int(subcomp)
    else:
        raise ValueError(
            "Syntax of location must be either 'segment.component' or"
            "'segment.component.subcomponent'"
        )
    
    comp = int(comp)

    def parser(msg):
        segs = re.findall(segment + '\|.*(?=\\n)', msg)
        data = []
        for seg in segs:
            if depth == 2:
                datum = seg.split("|")[comp]
            else:
                try:
                    datum = seg.split("|")[comp].split("^")[subcomp - 1]
                except IndexError:          
                    # handle empty component (i.e. '||')
                    datum = ''          
            data.append(datum)
        return data
    return parser

def main():

    query_broad = '()s.sending_facility:"COVH"()NOT ()s.attending_doctor_id: IN("1952","2620","2489","1131","1604","3654","2216","1265","2511","3071","3050","3557","746","52118","2507","52715","51071","1080","2716","1948","51644","1321","1066")()s.patient_class:"Inpatient"()s.patient_type: IN("IPB","IPF","IPL","IPM","IPP","IPR","IPS")()d.discharge_time:[2017-08-06 TO 2017-09-20]()s.record_type:"ADT A08"'

    msgs = query_raw(query_broad, "ADTs", 50)

    msg_ids = parse_msg_id(
        [
            "PID.3.4",
            "PID.3.1",
            "PID.18.1",
        ],
        msgs
    )
    print(msg_ids)

main()