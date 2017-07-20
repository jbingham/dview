"""setuptools-based installation script.
"""

from setuptools import find_packages
from setuptools import setup

setup(
    name='dview',
    author='Google',
    author_email='binghamj@google.com',
    license='Apache',
    url='https://github.com/jbingham/dview',
    packages=find_packages(),
    install_requires=[
        'dsub',
        'grpcio==1.3.5',
        'apache-beam',
        'apache-beam[gcp]'
    ],
    entry_points={
        'console_scripts': [
            'dview=dview:main',
        ],
    },)
