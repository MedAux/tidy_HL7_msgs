'''
Setup
'''

from setuptools import setup

setup(
    name='tidy_hl7_msgs',
    version='0.1.0',
    description='A simply utility to parse and tidy HL7 messages',
    url='https://github.com/feyderm/tidy_HL7_msgs.git',
    author='Michael Feyder',
    author_email='feyderm@gmail.com',
    license='MIT',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3 :: Only',
        'Intended Audience :: Healthcare Industry',
        'License :: OSI Approved :: MIT License',
    ],
    keywords='healthcare HL7',
    packages=['tidy_hl7_msgs'],
    install_requires=[
        'pandas',
        'numpy',
    ],
)
