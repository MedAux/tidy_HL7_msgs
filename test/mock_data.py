'''
Mock HL7 V2 messages for unit tests
'''

MSG_1 = '''
    MSH|^~\\&||^Facility A|||20170515104040||ADT^A08^ADT A08
    PID|1||123^^^FACILITY A||DOE^JOHN
    DG1|1||D53.9^Nutritional anemia, unspecified^I10|||AM
    DG1|2||^Perforation of intestine (nontraumatic)^I10
'''.lstrip()

MSG_2 = '''
    MSH|^~\\&||^Facility B|||20170711123256||ADT^A08^ADT A08
    PID|1||456^^^FACILITY B||SMITH^JANE
    DG1|1||M43.16^Spondylolisthesis, lumbar region^I10|||AM
'''.lstrip()

MSG_3 = '''
    MSH|^~\\&||^Facility C|||20170322123231||ADT^A08^ADT A08
    PID|1||789^^^FACILITY C||BROWN^JOAN
    PR1|1|I10P|0W9L0ZX|Drainage of Lower Back, Open Approach, Diagnostic
'''.lstrip()

MSGS = [MSG_1, MSG_2, MSG_3]
