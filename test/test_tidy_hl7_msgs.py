'''
Unit Testing
'''
# pylint: disable=missing-docstring

import pytest
import numpy as np
import pandas as pd
from tidy_hl7_msgs.main import tidy_segs
from tidy_hl7_msgs.parsers import (
    parse_msgs,
    parse_msg_id,
    parse_loc_txt,
)
from tidy_hl7_msgs.helpers import (
    concat,
    flatten,
    zip_nested,
    are_lens_equal,
    are_segs_identical,
    are_nested_lens_equal,
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

MSGS = [MSG_1, MSG_2]

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

def test_are_segs_identical():
    identical_segs = ['DG1.3.1', 'DG1.3.2', 'DG1.6']
    assert are_segs_identical(identical_segs) is True

    non_identical_segs = ['DG1.3.1', 'DG1.3.2', 'PID.3.4']
    assert are_segs_identical(non_identical_segs) is False

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
    assert parse_msgs('DG1.6', MSGS) == [
        ['AM', 'F', 'F', 'F'],
        ['AM', 'F', 'F', 'F'],
    ]
    assert parse_msgs('DG1.3.1', MSGS) == [
        ['D53.9', 'D53.9', 'C80.1', 'N30.00'],
        ['M43.16', 'M43.16', 'I10', 'M48.06'],
    ]

    # TODO
    # assert parse_msgs('PR1.5', MSGS) == [np.nan] * 2

    assert parse_msgs('DG1.16', MSGS) == [
        [np.nan] * 4,
        [np.nan] * 4,
    ]

def test_parse_loc_txt():
    field_d2 = parse_loc_txt('PR1.3')
    assert field_d2['depth'] == 2
    assert field_d2['seg'] == 'PR1'
    assert field_d2['field'] == 3

    field_d3 = parse_loc_txt('DG1.3.1')
    assert field_d3['depth'] == 3
    assert field_d3['seg'] == 'DG1'
    assert field_d3['field'] == 3
    assert field_d3['comp'] == 0

    msh_d3 = parse_loc_txt('MSH.3.1')
    assert msh_d3['depth'] == 3
    assert msh_d3['seg'] == 'MSH'
    assert msh_d3['field'] == 2
    assert msh_d3['comp'] == 0

    with pytest.raises(ValueError):
        parse_loc_txt('DG1')
    with pytest.raises(ValueError):
        parse_loc_txt('DG1.2.3.4')

def test_parse_msg_ids():
    assert parse_msg_id(['PID.3.1', 'PID.3.4', 'MSH.7'], MSGS) == (
        ['123,FACILITY A,20170515104040', '456,FACILITY B,20170711123256']
    )

    # field w/ multiple segments and therefore multiple values
    with pytest.raises(RuntimeError):
        parse_msg_id(['DG1.3.1'], MSGS)

    non_unique_msg_ids = MSGS + [MSGS[0]]
    with pytest.raises(RuntimeError):
        parse_msg_id(['PID.3.1', 'PID.3.4'], non_unique_msg_ids)

def test_tidy_segs():
    # pylint: disable=invalid-name

    id_locs = {
        'MSH.7': 'id_loc_1',
        'PID.3.1': 'id_loc_2',
        'PID.3.4': 'id_loc_3',
    }

    report_locs = {
        'DG1.3.1': 'report_loc_1',
        'DG1.3.2': 'report_loc_2',
        'DG1.3.3': 'report_loc_3',
        'DG1.6': 'report_loc_4',
        'DG1.16': 'report_loc_5',
    }

    df = tidy_segs(
        id_locs,
        report_locs,
        MSGS
    )

    # expected values
    assert all(df['id_loc_1'].values == (
        ['20170515104040'] * 4 + ['20170711123256'] * 4
    ))
    assert all(df['id_loc_2'].values == (
        ['123'] * 4 + ['456'] * 4
    ))
    assert all(df['id_loc_3'].values == (
        ['FACILITY A'] * 4 + ['FACILITY B'] * 4
    ))
    assert all(df['seg'].values == (
        ['seg_' + str(n) for n in list(range(4)) * 2]
    ))
    assert all(df['report_loc_1'].values == [
        'D53.9', 'D53.9', 'C80.1', 'N30.00',
        'M43.16', 'M43.16', 'I10', 'M48.06',
    ])
    assert all(df['report_loc_2'].values == [
        'Nutritional anemia, unspecified',
        'Nutritional anemia, unspecified',
        'Malignant (primary) neoplasm, unspecified',
        'Acute cystitis without hematuria',
        'Spondylolisthesis, lumbar region',
        'Spondylolisthesis, lumbar region',
        'Essential (primary) hypertension',
        'Spinal stenosis, lumbar region',
    ])
    assert all(df['report_loc_3'].values == 'I10')
    assert all(df['report_loc_4'].values == ['AM', 'F', 'F', 'F'] * 2)
    assert all(pd.isnull(df['report_loc_5']))

    # columns renamed
    col_names = list(id_locs.values()) + list(report_locs.values())
    assert all([col in df.columns.values for col in col_names]) is True

    # report fields within the same segment
    with pytest.raises(ValueError):
        tidy_segs(
            ['PID.3.1', 'PID.3.4'],
            ['DG1.3.1', 'DG1.3.2', 'AL.15'],
            MSGS
        )

    # for '-s' pytest arg
    print('\n')
    print(df)
