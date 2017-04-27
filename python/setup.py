#!/usr/bin/env python
import os
from distutils.core import setup
PACKAGE_NAME = 'pyapex'
JAR_DIRECTORY = os.path.join(PACKAGE_NAME, 'jars')
PACKAGE_VERSION = '1.0.0'
PYTHON_REQUIREMENTS = [
    # argparse is part of python2.7 but must be declared for python2.6
    'argparse',
    'mock'

]
REMOTE_MAVEN_PACKAGES = [
        # (group id, artifact id, version),
        ('org.apache.apex', 'apex-engine', '3.4.0'),
]


setup(
        name=PACKAGE_NAME,
        version=PACKAGE_VERSION,
        description='A python interface for the Apex Development',
        license='Apache Apex Software License',
        #packages=[PACKAGE_NAME, PACKAGE_NAME + "/v2", 'samples'],
        #scripts=glob.glob('samples/*py'),
        package_data={
            '': ['*.txt', '*.md'],
            PACKAGE_NAME: ['jars/*'],
            'samples': ['sample.properties'],
        },
        install_requires=PYTHON_REQUIREMENTS,
        url="https://github.com/awslabs/amazon-kinesis-client-python",
        keywords="Apex ",
        zip_safe=False,
)
