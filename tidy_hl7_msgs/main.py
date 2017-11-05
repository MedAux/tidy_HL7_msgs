'''
Tidy HL7 message segments
'''

import itertools
import pandas as pd
from .helpers import (
    to_df,
    join_dfs,
    zip_msg_ids,
    are_segs_identical,
)
from .parsers import (
    parse_msgs,
    parse_msg_id
)

def tidy_segs(msg_id_locs, report_locs, msgs):
    ''' Tidy HL7 message segments

    Parameters
    ----------
    msg_id_locs : list or able to be converted to one

        Locations (i.e. HL7 message fields and/or components) that in
        combination uniquely identify a message. Locations can be from
        different message segments, but each location must return one
        value per message. Message IDs must uniquely identify messages.

        Location syntax must be either '<segment>.<field>' or
        '<segment>.<field>.<component>', delinated by a period ('.')
        (ex. 'MSH.4' or 'MSH.4.1')

        If passed a dictionary, its keys will be parsed locations and its
        values will be column names.

    report_loc : list or able to be converted to one

        Locations (i.e. fields and/or components) to report. Locations
        must be from the same segment.

        Location syntax must be either '<segment>.<field>' or
        '<segment>.<field>.<component>', delinated by a period ('.')
        (ex. 'DG1.2' or 'DG1.2.1')

        If passed a dictionary, its keys will be parsed locations and its
        values will be column names.

    msgs : list(string) of HL7 messages

    Returns
    -------
    Dataframe

        For each unique message, rows for each segment and columns for
        message ids, segment number, and report locations.

        Missing values are reported as numpy NaN.

    Raises
    ------
    ValueError if report locations are not from the same segment
    '''
    # pylint: disable=invalid-name

    if not are_segs_identical(report_locs):
        raise ValueError("Report locations must be from the same segment")

    msgs_unique = set(msgs)

    # parse message ids
    msg_ids = parse_msg_id(list(msg_id_locs), msgs_unique)

    # parse report locations
    report_vals = map(
        parse_msgs,
        list(report_locs),
        itertools.repeat(msgs_unique)
    )

    # zip values for each report location w/ message ids
    zipped = map(
        zip_msg_ids,
        report_vals,
        itertools.repeat(msg_ids)
    )

    # convert each zipped reported value + message id to a dataframe
    dfs = list(map(
        to_df,
        zipped,
        report_locs
    ))

    # join dataframes
    df = join_dfs(dfs)

    # remove segments w/o data
    df.dropna(
        axis=0,
        how='all',
        subset=report_locs,
        inplace=True
    )

    df.sort_values(by=['msg_id'], inplace=True)

    # tidy message ids
    id_cols = df['msg_id'].str.split(",", expand=True)
    id_cols.columns = msg_id_locs
    df = pd.concat([id_cols, df], axis=1).drop(['msg_id'], axis=1)

    try:
        df.rename(columns=msg_id_locs, inplace=True)
        df.rename(columns=report_locs, inplace=True)
    except TypeError:
        pass

    return df
