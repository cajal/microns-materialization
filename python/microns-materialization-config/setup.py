#!/usr/bin/env python
from setuptools import setup, find_packages
from os import path

here = path.abspath(path.dirname(__file__))

with open(path.join(here, '..', 'version.py')) as f:
    exec(f.read())

setup(
    name="microns-materialization-config",
    version=__version__,
    description="configuration for microns-materialization",
    author="Stelios Papadopoulos, Brendan Celii, Christos Papadopoulos",
    packages=find_packages()
)