'''
Unit Testing
'''
# pylint: disable=missing-docstring

from test.mock_data import MSGS
import pytest
import numpy as np
from tidy_hl7_msgs.main import tidy_segs

MSG_ID_LOCS = {
    'MSH.7': 'msg_date_time',
    'PID.3.1': 'facility_code',
}

REPORT_LOCS_DG1 = {
    'DG1.3.1': 'diag_code',
    'DG1.6': 'diag_type',
    'DG1.16': 'diag_dr',
}

def test_locs_not_empty():
    # id locs
    with pytest.raises(ValueError):
        tidy_segs({}, {'DG1.1': 'report_loc_1'}, MSGS)
    with pytest.raises(ValueError):
        tidy_segs([], ['DG1.1'], MSGS)

    # report locs
    with pytest.raises(ValueError):
        tidy_segs({'MSH.7': 'id_loc_1'}, {}, MSGS)
    with pytest.raises(ValueError):
        tidy_segs(['MSH.7'], [], MSGS)

    # messages
    with pytest.raises(ValueError):
        tidy_segs(['MSH.7'], ['DG1.1'], [])

def test_report_locs_from_same_seg():
    diff_report_segs_dict = dict(REPORT_LOCS_DG1).update({'AL.3': 'allergen_code'})
    with pytest.raises(ValueError):
        tidy_segs(MSG_ID_LOCS, diff_report_segs_dict, MSGS)

    diff_report_segs_lst = list(REPORT_LOCS_DG1) + ['AL.3']
    with pytest.raises(ValueError):
        tidy_segs(MSG_ID_LOCS, diff_report_segs_lst, MSGS)

def test_id_loc_not_na():
    with pytest.raises(RuntimeError):
        tidy_segs(['PID.3.2'], REPORT_LOCS_DG1, MSGS)

def test_id_loc_not_missing_seg():
    with pytest.raises(RuntimeError):
        tidy_segs(['EVN.2'], REPORT_LOCS_DG1, MSGS)

def test_id_loc_not_multi_vals():
    with pytest.raises(RuntimeError):
        tidy_segs(['DG1.3.2'], REPORT_LOCS_DG1, MSGS)

def test_df_vals():
    # pylint: disable=invalid-name
    def are_segs_equal(seg1, seg2):
        def are_vals_equal(k):
            return seg1[k][0] == seg2[k][0]
        def are_vals_na(k):
            return np.isnan(seg1[k][0]) and np.isnan(seg2[k][0])

        are_keys_equal = set(seg1) == set(seg2)
        are_all_vals_equal = all([are_vals_equal(key) or are_vals_na(key) for key in seg1])

        return are_keys_equal and are_all_vals_equal

    msg_1_seg_1 = {
        'msg_date_time': ['20170515104040'],
        'facility_code': ['123'],
        'seg': [1.0],
        'diag_dr': [np.nan],
        'diag_type': ['AM'],
        'diag_code': ['D53.9'],
    }
    msg_1_seg_2 = {
        'msg_date_time': ['20170515104040'],
        'facility_code': ['123'],
        'seg': [2.0],
        'diag_dr': [np.nan],
        'diag_type': [np.nan],
        'diag_code': [np.nan],
    }
    msg_2_seg_1 = {
        'msg_date_time': ['20170711123256'],
        'facility_code': ['456'],
        'seg': [1.0],
        'diag_dr': [np.nan],
        'diag_type': ['AM'],
        'diag_code': ['M43.16'],
    }
    msg_3_seg_1 = {
        'msg_date_time': ['20170322123231'],
        'facility_code': ['789'],
        'seg': [np.nan],
        'diag_dr': [np.nan],
        'diag_type': [np.nan],
        'diag_code': [np.nan],
    }

    df = tidy_segs(MSG_ID_LOCS, REPORT_LOCS_DG1, MSGS)
    df_msg_1 = df.loc[df['msg_date_time'] == '20170515104040']
    df_msg_1_seg_1 = df_msg_1.loc[df['seg'] == 1].to_dict('list')
    df_msg_1_seg_2 = df_msg_1.loc[df['seg'] == 2].to_dict('list')
    df_msg_2_seg_1 = df.loc[df['msg_date_time'] == '20170711123256'].to_dict('list')
    df_msg_3_seg_1 = df.loc[df['msg_date_time'] == '20170322123231'].to_dict('list')

    assert are_segs_equal(df_msg_1_seg_1, msg_1_seg_1) is True
    assert are_segs_equal(df_msg_1_seg_2, msg_1_seg_2) is True
    assert are_segs_equal(df_msg_2_seg_1, msg_2_seg_1) is True
    assert are_segs_equal(df_msg_3_seg_1, msg_3_seg_1) is True

def test_df_cols_renamed():
    # pylint: disable=invalid-name
    df = tidy_segs(MSG_ID_LOCS, REPORT_LOCS_DG1, MSGS)
    col_names = (
        list(MSG_ID_LOCS.values())
        + list(REPORT_LOCS_DG1.values())
        + ['seg']
    )
    assert all([col in df.columns.values for col in col_names]) is True

def test_print_tidy_segs():
    # pylint: disable=invalid-name
    df = tidy_segs(MSG_ID_LOCS, REPORT_LOCS_DG1, MSGS)
    print('\n\n')
    print(df)
