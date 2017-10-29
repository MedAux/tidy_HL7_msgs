# pylint: disable=W0614,C0111

import pytest
from . import test_data
from ..query_raw import tidy_segs
from ..helpers import (
    are_lens_equal, are_nested_lens_equal, are_segs_identical,
    flatten, zip_nested, concat
)
from ..parsers import parse_field_txt, parse_msgs, parse_msg_id

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
    assert parse_msgs('DG1.6', test_data.msgs) == test_data.DG_1_6

def test_parse_msg_ids():
    assert parse_msg_id(['PID.3.4', 'PID.3.1', 'PID.18.1'], test_data.msgs) == (
        test_data.msg_ids
    )

    # field w/ multiple segments and therefore multiple values
    with pytest.raises(RuntimeError):
        parse_msg_id(['AL1.1.1'], test_data.msg_ids)

    non_unique_msg_ids = test_data.msgs + [test_data.msgs[0]]
    with pytest.raises(RuntimeError):
        parse_msg_id(['PID.3.4', 'PID.3.1', 'PID.18.1'], non_unique_msg_ids)


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
        'MSH.7': 'id_field_1',
        'PID.3.1': 'id_field_2',
        'PID.3.4': 'id_field_3',
        'PID.18.1': 'id_field_4'
    }

    report_fields_dict = {
        'DG1.3.1': 'report_field_1',
        'DG1.3.2': 'report_field_2',
        'DG1.6': 'report_field_3',
        'DG1.15': 'report_field_4'
    }

    df = tidy_segs(
        id_fields_dict,
        report_fields_dict,
        test_data.msgs
    )

    print('\n')
    print(df)

    col_names = list(id_fields_dict.values()) + list(report_fields_dict.values())
    assert all([col_name in df.columns.values for col_name in col_names]) is True

    # segments of report fields not the same
    with pytest.raises(ValueError):
        tidy_segs(
            ['PID.3.4', 'PID.3.1', 'PID.18.1'],
            ['DG1.3.1', 'DG1.3.2', 'AL.15'],
            test_data.msgs
        )
