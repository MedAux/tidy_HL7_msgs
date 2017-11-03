'''
Tidy HL7 message segments
'''
# pylint: disable=fixme

# TODO
# update var names
# - create README
#   - example and installation

import itertools
import pandas as pd
from .helpers import are_segs_identical, zip_msg_ids, to_df, join_dfs
from .parsers import parse_msgs, parse_msg_id

def tidy_segs(msg_id_locs, report_locs, msgs):
    '''
    Tidy HL7 message segments

    Args:
        msg_id_locs: list or able to be converted to one

            Locations (i.e. fields and/or components) to uniquely identify a
            message. Locations can be from different message segments, but
            each location must return one value per message. Messages must be
            uniquely identified by their message IDs.

            Location syntax must be either '<segment>.<field>' or
            '<segment>.<field>.<component>' (ex. 'PID.3' or 'PID.3.1')

            If passed a dictionary, its keys will be locations to parse
            and values will be the column names of the returned dataframe.

        report_loc: list or able to be converted to one

            Locations (i.e. fields and/or components) to report. Locations
            must be from the same segment.

            Location syntax must be either '<segment>.<field>' or
            '<segment>.<field>.<component>' (ex. 'DG1.2' or 'DG1.2.1')

            If passed a dictionary, its keys will be locations to parse
            and values will be the column names of the returned dataframe.

        msgs: list of HL7 messages

    Returns:
        Dataframe of unique messages whose rows are message segments and
        whose columns are message ids, segment number, and report
        locations. Missing values are reported as numpy NaN.

    Raises:
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

    # TODO: more specific exception handling?
    try:
        df.rename(columns=msg_id_locs, inplace=True)
        df.rename(columns=report_locs, inplace=True)
    except Exception:
        pass

    return df
