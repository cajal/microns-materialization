#!/usr/bin/env python

from setuptools import setup, find_packages
from os import path

here = path.abspath(path.dirname(__file__))

with open(path.join(here, '..', 'version.py')) as f:
    exec(f.read())
    
def find_config_package(name):
    return f"{name} @ file://localhost/{here}/../{name}#egg={name}"

#config_package = find_config_package('microns-morphology-config')
config_package = find_config_package('microns-materialization-config')


setup(
    name='microns_materialization',
    version='0.0.1',
    description='Datajoint schemas for importing external data for the MICrONS project',
    author='Stelios Papadopoulos',
    author_email='spapadop@bcm.edu',
    packages=find_packages(exclude=[]),
    install_requires=['numpy', 'pandas', 'scipy', 'decorator', 'matplotlib', 'caveclient', 'nglui', 'tqdm', config_package]
)