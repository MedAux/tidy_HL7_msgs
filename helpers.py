'''
Helpers
'''

import re
import pandas as pd

def are_lens_equal(*args):
    '''
    Are lengths equal?

    Example:
        >>> are_lens_equal([1, 2], [1, 2])
        True
        >>> are_lens_equal([1, 2], [1, 2, 3])
        False

    Args:
        *args: one or more lists

    Returns:
        Boolean
    '''
    lens = [len(x) for x in args]
    return len(set(lens)) == 1

def are_nested_lens_equal(lst1, lst2):
    '''
    Are nested lengths equal?

    Example:
        >>> are_nested_lens_equal(
        ...     [[1, 2], [1, 2], [1, 2]],
        ...     [[1, 2], [1, 2], [1, 2]]
        ... )
        True
        >>> are_nested_lens_equal(
        ...     [[1, 2], [1, 2], [1, 2]],
        ...     [[1, 2], [1, 2], [1, 2, 3]]
        ... )
        >>> are_nested_lens_equal(
        False
    Args:
        lst1: list
        lst2: list

    Returns:
        Boolean
    '''
    assert are_lens_equal(lst1, lst2), "List lengths are not equal"
    return all([len(lst1[i]) == len(lst2[i]) for i in range(len(lst1))]) is True

def are_segs_identical(fields):
    '''
    Check if all fields are from the same segment

    Example:
    >>> are_segs_identical(['DG1.3.1', 'DG1.3.2', 'DG1.6'])
    True
    >>> are_segs_identical(['DG1.3.1', 'DG1.3.2', 'PID.3.4'])
    False

    Args:
        fields: list(string)

    Returns:
        boolean
    '''
    segs = [re.match('\\w*', field).group() for field in fields]
    return len(set(segs)) == 1

def flatten(lst):
    '''
    Flatten lists nested one level deep

    Empty nested lists are not perserved

    Example:
        >>> flatten([[1, 2], [3, 4], [5, 6]])
        [1, 2, 3, 4, 5, 6]

        >>> flatten([[1, 2], [3, 4], []])
        [1, 2, 3, 4]

        >>> flatten([[1, 2], [3, 4], [5, 6, [7, 8]]])
        [1, 2, 3, 4, 5, 6, [7, 8]]

    Args:
        lst: list

    Returns:
        list
    '''
    return [item for sublist in lst for item in sublist]

def zip_nested(lst1, lst2):
    '''
    Zip nested lists

    The length of the two lists must be equal

    Examples:
        >>> zip_nested([['a', 'b']], [['y', 'z']])
        [[('a', 'y'), ('b', 'z')]]

        >>> zip_nested([['a', 'b'],['c', 'd']], [['w', 'x'],['y', 'z']])
        [[('a', 'w'), ('b', 'x')], [('c', 'y'), ('d', 'z')]]

    Args:
        lst1: list(string)
        lst2: list(string)

    Returns:
        list(list(tuple))
    '''
    assert are_nested_lens_equal(lst1, lst2), "Nested list lengths are not equal"
    return [list(zip(lst1[i], lst2[i])) for i in range(len(lst1))]

def concat(lsts):
    '''
    Concatinate lists of strings

    Examples:
        >>> concat([[['a', 'y']], [['b', 'z']]])
        ['a,b', 'y,z']

        >>> concat([[['a', 'w']], [['b', 'x']], [['c', 'y']], [['d', 'z']]])
        ['a,b,c,d', 'w,x,y,z']

    Args:
        lsts: list(list(list(string)))

    Returns:
        list(string)
    '''
    lsts = [flatten(lst) for lst in lsts]

    lst_lens = [len(lst) for lst in lsts]
    assert len(set(lst_lens)) == 1, "Message ID fields are unequal length"

    concatted = []
    for i in range(len(lsts[0])):
        concatted.append([",".join(el[i] for el in lsts)])

    return flatten(concatted)

def zip_msg_ids(lst, msg_ids):
    '''
    Zip after checking list lengths are equal

    Args:
        lst: list
        msg_ids: list(string)

    Returns:
        list(tuple)
    '''
    assert are_lens_equal(msg_ids, lst), "List lengths are not equal"
    return list(zip(msg_ids, lst))

def to_df(lst, field_txt):
    '''
    Convert list to dataframe

    Example:
        >>> to_df(
        ...    [('msg_id1', ['val1']), ('msg_id2', ['val1', 'val2'])],
        ...    "field_name")
        ... )
           msg_id   seg        field_name
        0  msg_id1  seg_0      val1
        1  msg_id2  seg_0      val1
        2  msg_id1  seg_1      None
        3  msg_id2  seg_1      val2

    Args:
        lst: list(tuple(string))
            List of tuples, where the first element is the message ID and the
            second element is a list of parsed values

        field_txt: string
            Field name

    Returns:
        dataframe
    '''
    # pylint: disable=invalid-name

    df = pd.DataFrame.from_dict(
        dict(lst),
        orient="index"
    )

    n_cols = range(len(df.columns))
    df.columns = ["seg_{n}".format(n=n) for n in n_cols]

    df["msg_id"] = df.index
    df = pd.melt(df, id_vars=["msg_id"])
    df.rename(
        columns={
            "variable": "seg",
            "value": field_txt
        },
        inplace=True
    )
    return df

def join_dfs(dfs):
    '''
    Join a list of dataframes

    Args:
        dfs: list(dataframes)

    Returns:
        dataframe
    '''
    # pylint: disable=no-else-return

    if len(dfs) == 1:
        return dfs[0]
    else:
        df_join = pd.merge(
            dfs[0],
            dfs[1],
            how="inner",
            on=["msg_id", "seg"],
            sort=False
        )
        dfs_to_join = dfs[2:]
        dfs_to_join.append(df_join)
        return join_dfs(dfs_to_join)
