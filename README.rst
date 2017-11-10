Tidy HL7 Messages
=================
A simple Python 3.x utility to parse and tidy_ HL7 v2 message segments

.. _tidy: http://vita.had.co.nz/papers/tidy-data.html

Example
-------

.. code-block:: python

    >>> from tidy_hl7_msgs import tidy_segs
    >>> 
    >>> msg_1 = '''
    ...    MSH|^~\\&||^Facility A|||20170515104040||ADT^A08^ADT A08|123
    ...    PID|1||123^^^FACILITY A||DOE^JOHN
    ...    DG1|1||D53.9^Nutritional anemia, unspecified^I10|||AM
    ...    DG1|2||C80.1^Malignant (primary) neoplasm, unspecified^I10|||F
    ... '''.lstrip()
    >>>
    >>> msg_2 = '''
    ...     MSH|^~\\&||^Facility B|||20170711123256||ADT^A08^ADT A08|456
    ...     PID|1||456^^^FACILITY B||SMITH^JANE
    ...     DG1|1||M43.16^Spondylolisthesis, lumbar region^I10|||AM
    ...     DG1|2||M48.06^Spinal stenosis, lumbar region^I10|||F
    ... '''.lstrip()
    >>>
    >>> msgs = [msg_1, msg_2]
    >>> 
    >>> # Message ID locations
    >>> # (fields and/or components that together uniquely ID messages)
    >>> id_locs = ['MSH.7', 'PID.5.1']
    >>> 
    >>> # Report locations
    >>> # (fields and/or components to report)
    >>> report_locs = ['DG1.3.1', 'DG1.6']
    >>> 
    >>> df = tidy_segs(id_locs, report_locs, msgs)
    >>> df
       MSH.7          PID.5.1    seg DG1.3.1 DG1.6
    0  20170515104040     DOE      1   D53.9    AM
    1  20170515104040     DOE      2   C80.1     F
    2  20170711123256   SMITH      1  M43.16    AM
    3  20170711123256   SMITH      2  M48.06     F
    >>>
    >>>
    >>> # pass locations as dicts to rename columns
    >>> id_locs_dict = {'MSH.7':'Msg Datetime', 'PID.5.1':'Patient Last Name'}
    >>> report_locs_dict = {'DG1.3.1': 'Diagnosis Code', 'DG1.6': 'Diagnosis Type'}
    >>> df_renamed = tidy_segs(id_locs_dict, report_locs_dict, msgs)
    >>> df_renamed
          Msg Datetime Patient Last Name seg Diagnosis Code Diagnosis Type 
    0   20170515104040               DOE   1          D53.9             AM 
    1   20170515104040               DOE   2          C80.1              F 
    2   20170711123256             SMITH   1         M43.16             AM 
    3   20170711123256             SMITH   2         M48.06              F 

Usage
-----

Locations must be either fields (i.e *segment.field*) or components (i.e. *segment.field.component*). Subcomponents are currently not supported.

ID locations, taken together, must uniquely identify messages after deduplication.

Report locations must be within the same segment.

Missing data is represented as NA.  Messages missing the report location segment are represented as a single row with NA for their seg column. 

Note that the order of the messages is not maintained.


Installation
------------

Install for Python 3.x using ``pip`` or ``pip3``

.. code-block:: bash

    $ pip install git+https://github.com/feyderm/tidy_HL7_msgs.git@v0.1.0
    
Contributing
------------
Pull requests welcome

Testing
-------
To run unit tests:

.. code-block:: bash

    $ python -m pytest
    $ python -m pytest -s         # to print dataframe

License
-------
MIT