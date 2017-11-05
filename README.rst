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
    >>> # ID locations (fields and/or components that together uniquely ID messages)
    >>> id_locs = {
    ...    'MSH.7': 'Message Date-Time',
    ...    'MSH.10': 'Message Control ID',
    ...    'PID.3.1': 'Facility Code',
    ... }
    >>> 
    >>> # Report locations (fields and/or components to report)
    >>> report_locs = {
    ...    'DG1.3.1': 'Diagnosis Code ID',
    ...    'DG1.3.2': 'Diagnosis Code Text',
    ...    'DG1.3.3': 'Diagnosis Coding System',
    ...    'DG1.6': 'Diagnosis Type',
    ...}
    >>> 
    >>> df = tidy_segs(id_locs, report_locs, msgs)
    >>> df
      Message Control ID Message Date-Time Facility Code    seg  \
    1                123    20170515104040           123  seg_0   
    3                123    20170515104040           123  seg_1   
    0                456    20170711123256           456  seg_0   
    2                456    20170711123256           456  seg_1   
    
                             Diagnosis Code Text Diagnosis Code ID Diagnosis Type  \
    1            Nutritional anemia, unspecified             D53.9             AM   
    3  Malignant (primary) neoplasm, unspecified             C80.1              F   
    0           Spondylolisthesis, lumbar region            M43.16             AM   
    2             Spinal stenosis, lumbar region            M48.06              F   
    
      Diagnosis Coding System  
    1                     I10  
    3                     I10  
    0                     I10  
    2                     I10  

Usage
-----

Locations must be either be a message field or component with the syntax
*segment.field* or *segment.field.component*, respectively. Subcomponents are
currently not supported.

Locations can either be passed as a list or a dictionary. If passed as a
list, the column names of the returned dataframe will the the
field/component. If passed a dictionary, its keys must be the field/component
and its values will be the column names.

ID locations taken together must uniquely identify messages after deduplication.

Report locations must all be within the same segment.

Note that the order of the messages may not be maintained.

Installation
------------

Install for Python 3.x using ``pip`` or ``pip3``

.. code-block:: bash

    $ pip install git+https://github.com/feyderm/tidy_HL7_msgs.git@v0.1.0
    
Contributing
------------
Pull requests more than welcome

Testing
-------
To run unit tests:

.. code-block:: bash

    $ python -m pytest
    $ python -m pytest -s         # to print dataframe

License
-------
MIT