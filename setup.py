from setuptools import setup


setup(
    name='Tidy HL7 Messages',
    version='0.1.0',
    description='A simply utility to parse and tidy HL7 messages',
    # url='',
    author='Michael Feyder',
    author_email='feyderm@gmail.com',
    # license='',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Healthcare Industry',
        # 'License :: OSI Approved :: MIT License'
        'Programming Language :: Python :: 3.5',
    ],
    keywords='healthcare HL7',
    packages=['tidy_hl7_msgs'],
    install_requires=[
        'pandas',
        'numpy',
    ],
)