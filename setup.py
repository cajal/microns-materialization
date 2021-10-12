#!/usr/bin/env python

from setuptools import setup, find_packages
from os import path

here = path.abspath(path.dirname(__file__))

setup(
    name='minniemat',
    version='0.0.1',
    description='Datajoint schemas and related methods for minnie materialization',
    author='Stelios Papadopoulos',
    author_email='spapadop@bcm.edu',
    packages=find_packages(exclude=[]),
    install_requires=['numpy', 'pandas', 'scipy', 'datajoint==0.12.9', 'decorator', 'matplotlib', 'caveclient', 'nglui', 'tqdm']
)