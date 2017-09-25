import pytest
from query_raw.query_raw import *
from query_raw.test import test_data

def test_lst_lens_equal():
    with pytest.raises(RuntimeError):
        check_lens_equal([1, 2, 3], [1, 2, 3, 4])

def test_nested_lst_lens_equal():
    with pytest.raises(RuntimeError):
        check_nested_lens_equal(
            [[1], [2], [3, 4]],
            [[1], [2], [3, 4, 5]]
        )

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

def test_concat_fields():
    assert concat([[['a', 'b']]]) == ['a', 'b']
    assert concat([[['a', 'b']], [['y', 'z']], [['s', 't']]]) == (
        ['a,y,s', 'b,z,t']
    )

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

def test_parse_msg_ids():
    # TODO: test single field
    assert parse_msg_id(['PID.3.4', 'PID.3.1', 'PID.18.1'], test_data.msgs) == (
        test_data.msg_ids
    )
