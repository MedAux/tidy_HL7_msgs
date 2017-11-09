# pylint: disable=missing-docstring, invalid-name

from test.mock_data import MSGS
from tidy_hl7_msgs.parsers import parse_msgs, parse_msg_id, parse_loc_txt
import pytest
import numpy as np

def test_parse_msgs():
    assert parse_msgs('DG1.6', MSGS) == [['AM', np.nan], ['AM'], ['no_seg']]
    assert parse_msgs('DG1.3.1', MSGS) == [['D53.9', np.nan], ['M43.16'], ['no_seg']]
    assert parse_msgs('PR1.5', MSGS) == [['no_seg'], ['no_seg'], [np.nan]]
    assert parse_msgs('PR1.5.1', MSGS) == [['no_seg'], ['no_seg'], [np.nan]]

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
        [
            '123,FACILITY A,20170515104040',
            '456,FACILITY B,20170711123256',
            '789,FACILITY C,20170322123231',
        ]
    )

    # id values NA
    with pytest.raises(RuntimeError):
        parse_msg_id(['PID.3.2'], MSGS)

    # id segment missing
    with pytest.raises(RuntimeError):
        parse_msg_id(['EVN.2'], MSGS)

    # multiple id values
    with pytest.raises(RuntimeError):
        parse_msg_id(['DG1.3.1'], MSGS)

    # non-unique msg ids
    non_unique_msg_ids = MSGS + [MSGS[0]]
    with pytest.raises(RuntimeError):
        parse_msg_id(['PID.3.1', 'PID.3.4'], non_unique_msg_ids)
