#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='ootp',
      version='0.0.1',
      # Modules to import from other scripts:
      packages=find_packages(),
      # Executables
      scripts=["src/main.py"],
     )
