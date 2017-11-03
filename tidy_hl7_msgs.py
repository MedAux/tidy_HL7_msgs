'''
Tidy HL7 message segments
'''
# pylint: disable=W0511

# TODO
# update var names
# remove poss. duplicate messages
# - create README
#   - example and installation

import re
import itertools
import pandas as pd
from .helpers import are_segs_identical, zip_msg_ids, to_df, join_dfs
from .parsers import parse_msgs, parse_msg_id

def tidy_segs(msg_id_fields, report_fields, msgs):
    '''
    Tidy HL7 message segments

    Args:
        msg_id_fields: list or able to be converted to one

            Fields to uniquely identify a message. Fields can be from
            different message segments, but each field must return in one
            value per message.

            If argument is a dict-like, its keys must be HL7 field(s) to
            parse and values will be column names for the returned dataframe.

        report_fields: list or able to be converted to one

            Fields to report.  Fields must be from the same segments.

            If argument is dict-like, its keys must be HL7 field(s) to
            parse and values will be column names for the returned dataframe.

        msgs: list(string)
            List of HL7 messages.

    Returns:
        Dataframe whose rows are message segments and whose columns are
        message id fields, segment number, and reported fields.  Missing values
        are reported a numpy NaN.

    Raises:
        ValueError if all fields are not from the same segment
    '''
    # pylint: disable=C0103

    if not are_segs_identical(report_fields):
        raise ValueError("All fields must be from the same segment")

    msgs_unique = set(msgs)

    # parse message ids
    msg_ids = parse_msg_id(list(msg_id_fields), msgs_unique)

    # parse report fields
    report_fields_vals = map(
        parse_msgs,
        list(report_fields),
        itertools.repeat(msgs_unique)
    )

    # zip report field values and message ids
    report_fields_vals_w_msg_ids = map(
        zip_msg_ids,
        report_fields_vals,
        itertools.repeat(msg_ids)
    )

    # convert to dataframes
    dfs = list(map(
        to_df,
        report_fields_vals_w_msg_ids,
        report_fields
    ))

    # join dataframes
    df = join_dfs(dfs)

    # remove segments missing data for all reported fields
    df.dropna(
        axis=0,
        how='all',
        subset=report_fields,
        inplace=True
    )

    # sort by msg_id to group segs by msg
    # NOTE: order will differ from that of the input messages
    df.sort_values(by=['msg_id'], inplace=True)


    # split concatted fields used for a message id into individual columns;
    # combine this id dataframe with that for reported fields, dropping
    # old (i.e. concatted) message id column
    id_cols = df['msg_id'].str.split(",", expand=True)
    id_cols.columns = msg_id_fields
    df_w_id_cols = pd.concat([id_cols, df], axis=1).drop('msg_id', axis=1)

    # TODO: more specific exception handling?
    try:
        df_w_id_cols.rename(columns=msg_id_fields, inplace=True)
        df_w_id_cols.rename(columns=report_fields, inplace=True)
    except Exception:
        pass

    return df_w_id_cols
