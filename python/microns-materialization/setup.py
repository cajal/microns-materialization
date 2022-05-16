#!/usr/bin/env python

from setuptools import setup, find_packages
from os import path

def find_api(name):
    return f"{name} @ file://localhost/{here}/../{name}#egg={name}"

here = path.abspath(path.dirname(__file__))

with open(path.join(here, '..', 'version.py')) as f:
    exec(f.read())

with open(path.join(here, 'requirements.txt')) as f:
    requirements = f.read().split()

requirements += [find_api('microns-materialization-api')]

setup(
    name='microns_materialization',
    version=__version__,
    description='Datajoint schemas for importing external data for the MICrONS project',
    author='Stelios Papadopoulos',
    author_email='spapadop@bcm.edu',
    packages=find_packages(exclude=[]),
    install_requires=requirements
)