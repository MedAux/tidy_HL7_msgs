# pylint: disable=W0614,C0111

import pytest
from ..query_raw import tidy_segs
from ..parsers import parse_field_txt, parse_msgs, parse_msg_id
from ..helpers import (
    concat, flatten, zip_nested, are_lens_equal,
    are_segs_identical, are_nested_lens_equal,
)

MSG_1 = '''
    MSH|^~\\&||^Facility A|||20170515104040||ADT^A08^ADT A08
    PID|1||123^^^FACILITY A||DOE^JOHN
    DG1|1||D53.9^Nutritional anemia, unspecified^I10|||AM
    DG1|2||D53.9^Nutritional anemia, unspecified^I10|||F
    DG1|3||C80.1^Malignant (primary) neoplasm, unspecified^I10|||F
    DG1|4||N30.00^Acute cystitis without hematuria^I10|||F
'''.lstrip()

MSG_2 = '''
    MSH|^~\\&||^Facility B|||20170711123256||ADT^A08^ADT A08
    PID|1||456^^^FACILITY B||SMITH^JANE
    DG1|1||M43.16^Spondylolisthesis, lumbar region^I10|||AM
    DG1|2||M43.16^Spondylolisthesis, lumbar region^I10|||F
    DG1|3||I10^Essential (primary) hypertension^I10|||F
    DG1|4||M48.06^Spinal stenosis, lumbar region^I10|||F
'''.lstrip()

TEST_MSGS = [MSG_1, MSG_2]

def test_are_lens_equal():
    assert are_lens_equal([1, 2, 3], [1, 2, 3]) is True
    assert are_lens_equal([1, 2, 3], [1, 2, 3, 4]) is False

def test_are_nested_lens_equal():
    assert are_nested_lens_equal(
        [[1], [1], [1]],
        [[1], [1], [1]]
    ) is True

    assert are_nested_lens_equal(
        [[1], [1], [1]],
        [[1], [1], [2, 2]]
    ) is False

def test_flatten():
    assert flatten([[1, 2], [3, 4]]) == [1, 2, 3, 4]
    assert flatten([[1, 2, [3, 4]]]) == [1, 2, [3, 4]]
    assert flatten([[1, 2], []]) == [1, 2]
    assert flatten([]) == []

def test_zip_nested():
    assert zip_nested([['a', 'b']], [['y', 'z']]) == (
        [[('a', 'y'), ('b', 'z')]]
    )
    assert zip_nested([['a', 'b'], ['c', 'd']], [['w', 'x'], ['y', 'z']]) == (
        [[('a', 'w'), ('b', 'x')], [('c', 'y'), ('d', 'z')]]
    )

def test_concat():
    assert concat([[['a', 'b']]]) == ['a', 'b']
    assert concat([[['a', 'b']], [['y', 'z']], [['s', 't']]]) == (
        ['a,y,s', 'b,z,t']
    )

    with pytest.raises(AssertionError):
        concat([[['a', 'b']], [['x', 'y', 'z']]])

def test_parse_msgs():
    assert parse_msgs('DG1.6', TEST_MSGS) == [
        ['AM', 'F', 'F', 'F'],
        ['AM', 'F', 'F', 'F']
    ]
    assert parse_msgs('DG1.3.1', TEST_MSGS) == [
        ['D53.9', 'D53.9', 'C80.1', 'N30.00'],
        ['M43.16', 'M43.16', 'I10', 'M48.06']
    ]

def test_parse_msg_ids():
    assert parse_msg_id(['PID.3.1', 'PID.3.4', 'MSH.7'], TEST_MSGS) == (
        ['123,FACILITY A,20170515104040', '456,FACILITY B,20170711123256']
    )

    # field w/ multiple segments and therefore multiple values
    with pytest.raises(RuntimeError):
        parse_msg_id(['DG1.3.1'], TEST_MSGS)

    non_unique_msg_ids = TEST_MSGS + [TEST_MSGS[0]]
    with pytest.raises(RuntimeError):
        parse_msg_id(['PID.3.1', 'PID.3.4'], non_unique_msg_ids)


def test_parse_field_txt():
    field_d2 = parse_field_txt('PR1.3')
    assert field_d2['depth'] == 2
    assert field_d2['seg'] == 'PR1'
    assert field_d2['comp'] == 3

    field_d3 = parse_field_txt('DG1.3.1')
    assert field_d3['depth'] == 3
    assert field_d3['seg'] == 'DG1'
    assert field_d3['comp'] == 3
    assert field_d3['subcomp'] == 0

    msh_d3 = parse_field_txt('MSH.3.1')
    assert msh_d3['depth'] == 3
    assert msh_d3['seg'] == 'MSH'
    assert msh_d3['comp'] == 2
    assert msh_d3['subcomp'] == 0

    with pytest.raises(ValueError):
        parse_field_txt('DG1')
    with pytest.raises(ValueError):
        parse_field_txt('DG1.2.3.4')

def test_are_segs_identical():
    identical_segs = ['DG1.3.1', 'DG1.3.2', 'DG1.6']
    assert are_segs_identical(identical_segs) is True

    non_identical_segs = ['DG1.3.1', 'DG1.3.2', 'PID.3.4']
    assert are_segs_identical(non_identical_segs) is False

def test_tidy_segs():
    # pylint: disable=C0103

    # id_fields_lst = ['PID.3.4', 'PID.3.1', 'PID.18.1']
    # report_fields_lst = ['DG1.3.1', 'DG1.3.2', 'DG1.6', 'DG1.15'],

    # df = tidy_HL7_msg_segs(
    #     id_fields_lst,
    #     report_fields_lst,
    #     test_data.msgs
    # )

    id_fields_dict = {
        'PID.3.1': 'id_field_2',
        'PID.3.4': 'id_field_3',
        'MSH.7': 'id_field_1',
    }

    report_fields_dict = {
        'DG1.3.1': 'report_field_1',
        'DG1.3.2': 'report_field_2',
        'DG1.3.3': 'report_field_3',
        'DG1.6': 'report_field_4',
    }

    df = tidy_segs(
        id_fields_dict,
        report_fields_dict,
        TEST_MSGS
    )

    col_names = list(id_fields_dict.values()) + list(report_fields_dict.values())
    assert all([col_name in df.columns.values for col_name in col_names]) is True

    print('\n')
    print(df)

    # segments of report fields not the same
    with pytest.raises(ValueError):
        tidy_segs(
            ['PID.3.1', 'PID.3.4'],
            ['DG1.3.1', 'DG1.3.2', 'AL.15'],
            TEST_MSGS
        )
