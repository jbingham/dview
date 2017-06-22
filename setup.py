"""setuptools-based installation script.
"""

from setuptools import find_packages
from setuptools import setup

setup(
    name='dview',
    author='Google',
    license='Apache',
    packages=find_packages(),
    install_requires=[
        'dsub',
        'pyyaml',
        'apache-beam',
        'apache-beam[gcp]'
    ],
    entry_points={
        'console_scripts': [
            'dview=dview:main',
        ],
    },)
