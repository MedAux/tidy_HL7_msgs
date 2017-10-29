'''
Parsers
'''

import re
import itertools
import numpy as np
from .helpers import concat

def get_parser(field):
    '''
    Higher-order function to parse a field from an HL7 message

    Example:
        >>> msg = '...AL1|3|DA|1545^MORPHINE^99HIC|||20080828|||\n...'
        >>> parse_allergy_type = get_parser("AL1.2")
        >>> parse_allergy_type(msg)
        >>> ['DA']

        >>> parse_allergy_code_text = get_parser("AL1.3.2")
        >>> parse_allergy_code_text(msg)
        >>> ['MORPHINE']

        >>> seg_1 = '...AL1|3|DA|1545^MORPHINE^99HIC|||20080828|||\n'
        >>> seg_2 = 'AL1|4|DA|1550^CODEINE^99HIC|||20101027|||\n...'
        >>> msg_2 = seg_1 + seg_2
        >>> parse_allergy_code_text(msg_2)
        >>> ['MORPHINE', 'CODEINE']

    Args:
        field: dict
            Field attributes and values

    Returns:
        Function to parse an HL7 message at a given field
    '''
    def parser(msg):
        '''
        Parse an HL7 message

        Args:
            msg: string

        Returns:
            list(string)
        '''
        field_sep = list(msg)[3]
        comp_sep = list(msg)[4]

        seg_regex = field['seg'] + re.escape(field_sep) + '.*(?=\\n)'
        segs = re.findall(seg_regex, msg)

        data = []
        for seg in segs:
            comp = seg.split(field_sep)[field['comp']]
            if field['depth'] == 2:
                datum = comp
            else:
                try:
                    datum = comp.split(comp_sep)[field['subcomp']]
                except IndexError:
                    datum = np.nan
            data.append(datum)
        return data
    return parser

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

def parse_msgs(field_txt, msgs):
    '''
    Parse messages for a given field

    Examples:
        >>> msg1 = '...AL1|3|DA|1545^MORPHINE^99HIC|||20080828|||\n...'
        >>> msg2 = '...AL1|1|DRUG|00000741^OXYCODONE||HYPOTENSION\n...'
        >>> parse_msgs("AL1.3.1", [msg1, msg2])
        >>> [['1545'], ['00000741']]

        >>> # multiple segments per message
        >>> seg_1 = '...AL1|1|DRUG|00000741^OXYCODONE||HYPOTENSION\n'
        >>> seg_2 = 'AL1|2|DRUG|00001433^TRAMADOL||SEIZURES~VOMITING\n...'
        >>> msg3 = seg_1 + seg_2
        >>> parse_msgs("AL1.3.1", [msg1, msg3])
        >>> [['1545'], ['00000741', '00001433']]

    Args:
        field_txt: string
            Field to parse. Must be a single location represented by a
            segment, a component and an optional subcomponent. A period (".")
            must separate segments, components, and subcomponents (ex.
            "PR1.3" or "DG1.3.1")

        msgs: list(string)

    Returns:
        list(list(string))
    '''
    field = parse_field_txt(field_txt)
    parser = get_parser(field)
    return list(map(parser, msgs))
