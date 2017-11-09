# pylint: disable=missing-docstring

import pytest
import numpy as np
import pandas as pd
from tidy_hl7_msgs.helpers import (
    concat, flatten, zip_nested, are_lens_equal, are_segs_identical,
    are_nested_lens_equal, zip_msg_ids, trim_rows, to_df, join_dfs
)

def test_are_lens_equal():
    assert are_lens_equal([1, 2, 3], [1, 2, 3]) is True
    assert are_lens_equal([1, 2, 3], [1, 2, 3, 4]) is False

def test_are_nested_lens_equal():
    assert are_nested_lens_equal(
        [[1], [1]],
        [[1], [1]]
    ) is True

    assert are_nested_lens_equal(
        [[1], [1]],
        [[1], [1, 2]]
    ) is False

def test_are_segs_identical():
    identical_segs_lst = ['DG1.3.1', 'DG1.3.2', 'DG1.6']
    assert are_segs_identical(identical_segs_lst) is True

    identical_segs_dict = {
        'DG1.3.1': 'loc_1',
        'DG1.3.2': 'loc_2',
        'DG1.6': 'loc_3',
    }
    assert are_segs_identical(identical_segs_dict) is True

    non_identical_segs_lst = ['DG1.3.1', 'DG1.3.2', 'PID.3.4']
    assert are_segs_identical(non_identical_segs_lst) is False

    non_identical_segs_dict = {
        'DG1.3.1': 'loc_1',
        'DG1.3.2': 'loc_2',
        'PID.3.4': 'loc_3',
    }
    assert are_segs_identical(non_identical_segs_dict) is False

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
    with pytest.raises(AssertionError):
        zip_nested([['a', 'b']], [['x', 'y', 'z']])

def test_concat():
    assert concat([[['a', 'b']]]) == ['a', 'b']
    assert concat([[['a', 'b']], [['y', 'z']], [['s', 't']]]) == (
        ['a,y,s', 'b,z,t']
    )
    with pytest.raises(AssertionError):
        concat([[['a', 'b']], [['x', 'y', 'z']]])

def test_zip_msg_ids():
    with pytest.raises(AssertionError):
        zip_msg_ids(['a', 'b', 'c'], ['y', 'z'])

def test_trim_rows():
    # pylint: disable=invalid-name
    d = {
        'msg_id': ['123', '123', '123'],
        'col1': [1, 2, np.nan],
        'col2': [3, 4, np.nan],
    }
    df = pd.DataFrame(data=d)
    n_segs = {'123': 2}
    assert len(trim_rows(df, n_segs)) == 2


def test_to_df():
    # pylint: disable=invalid-name
    d = {
        'msg_id': ['msg_id1', 'msg_id2', 'msg_id2'],
        'seg': ['1', '1', '2'],
        'report_loc': ['val1', 'val1', 'val2'],
    }
    expected_df = pd.DataFrame(data=d)
    df = to_df([('msg_id1', ['val1']), ('msg_id2', ['val1', 'val2'])], "report_loc")

    assert all(df['msg_id'].values == expected_df['msg_id'].values)
    assert all(df['seg'].values == expected_df['seg'].values)
    assert all(df['report_loc'].values == expected_df['report_loc'].values)


def test_join_dfs():
    # pylint: disable=invalid-name
    d1 = {
        'msg_id': ['msg_id1', 'msg_id2', 'msg_id2'],
        'seg': ['seg1', 'seg1', 'seg2'],
        'report_loc1': ['a', 'b', 'c'],
    }
    df1 = pd.DataFrame(data=d1)

    d2 = {
        'msg_id': ['msg_id1', 'msg_id2', 'msg_id2'],
        'seg': ['seg1', 'seg1', 'seg2'],
        'report_loc2': ['x', 'y', 'z'],
    }
    df2 = pd.DataFrame(data=d2)

    df_join = join_dfs([df1, df2])
    msg1_seg1 = df_join[df_join['msg_id'] == 'msg_id1'].to_dict('list')
    msg2 = df_join[df_join['msg_id'] == 'msg_id2']
    msg2_seg1 = msg2[msg2['seg'] == 'seg1'].to_dict('list')
    msg2_seg2 = msg2[msg2['seg'] == 'seg2'].to_dict('list')

    expected_msg1_seg1 = {
        'msg_id': ['msg_id1'],
        'seg': ['seg1'],
        'report_loc1': ['a'],
        'report_loc2': ['x'],
    }
    expected_msg2_seg1 = {
        'msg_id': ['msg_id2'],
        'seg': ['seg1'],
        'report_loc1': ['b'],
        'report_loc2': ['y'],
    }
    expected_msg2_seg2 = {
        'msg_id': ['msg_id2'],
        'seg': ['seg2'],
        'report_loc1': ['c'],
        'report_loc2': ['z'],
    }

    assert msg1_seg1 == expected_msg1_seg1
    assert msg2_seg1 == expected_msg2_seg1
    assert msg2_seg2 == expected_msg2_seg2
