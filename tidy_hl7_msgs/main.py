'''
Tidy HL7 v2 message segments
'''

import itertools
import pandas as pd
from tidy_hl7_msgs.helpers import (
    to_df, join_dfs, zip_msg_ids, are_segs_identical
)
from tidy_hl7_msgs.parsers import parse_msgs, parse_msg_id

def tidy_segs(msg_id_locs, report_locs, msgs):
    ''' Tidy HL7 message segments

    Parameters
    ----------
    msg_id_locs : list or dict

        Locations (i.e. HL7 message fields or components) that taken together
        uniquely identify messages after de-duplication. Locations can be
        from different message segments, but each location must return one
        value per message. Values must not be missing. Message IDs must
        uniquely identify messages.

        Location syntax must be either <segment>.<field> or
        <segment>.<field>.<component>, delinated by a period (ex. 'MSH.4'
        or 'MSH.4.1')

        If passed a dictionary, its keys must be ID locations and its values
        will be corresponding column names in the returned dataframe.

    report_loc : list or dict

        Locations (i.e. HL7 message fields or components) to report.
        Locations must be from the same segment.

        Location syntax must be either <segment>.<field> or
        <segment>.<field>.<component>, delinated by a period (ex. 'DG1.4'
        or 'DG1.4.1')

        If passed a dictionary, its keys must be report locations and its
        values will be corresponding column names in the returned dataframe.

    msgs : list(string) of HL7 v2 messages

    Returns
    -------
    Dataframe

        Columns: one for each message ID/report location and for segment number

        Rows: one per segment

            Missing values are reported as NAs

            If message is missing segment, a single row for this message is
            returned with a segment number of NA and NAs for report locations.

    Raises
    ------
    ValueError if any parameter is empty
    ValueError if report locations are not from the same segment
    '''
    # pylint: disable=invalid-name
    if not msg_id_locs:
        raise ValueError("One or more message ID locations required")

    if not report_locs:
        raise ValueError("One or more report locations required")

    if not msgs:
        raise ValueError("One of more HL7 v2 messages required")

    if not are_segs_identical(report_locs):
        raise ValueError("Report locations must be from the same segment")

    msgs_unique = set(msgs)

    # parse message id locations
    msg_ids = parse_msg_id(list(msg_id_locs), msgs_unique)

    # parse report locations
    report_vals = map(parse_msgs, list(report_locs), itertools.repeat(msgs_unique))

    # zip values for each report location w/ message ids
    zipped = map(zip_msg_ids, report_vals, itertools.repeat(msg_ids))

    # convert each zipped message id + report value to a dataframe
    dfs = list(map(to_df, zipped, report_locs))

    # join dataframes
    df = join_dfs(dfs)

    # for natural sorting by segment, then for pretty printing
    df['seg'] = df['seg'].astype('float32')
    df.sort_values(by=['msg_id', 'seg'], inplace=True)
    df['seg'] = df['seg'].astype('object')

    # cleanup index
    df.reset_index(drop=True, inplace=True)

    # tidy message ids
    id_cols = df['msg_id'].str.split(",", expand=True)
    id_cols.columns = msg_id_locs
    df = pd.concat([id_cols, df], axis=1).drop(['msg_id'], axis=1)

    # rename columns if locs are dicts
    try:
        df.rename(columns=msg_id_locs, inplace=True)
    except TypeError:
        pass

    try:
        df.rename(columns=report_locs, inplace=True)
    except TypeError:
        pass

    return df
