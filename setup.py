# -*- coding: utf-8 -*-

## see https://packaging.python.org/distributing/#setup-py

from setuptools import setup
from codecs import open
import os.path

# h5py mocked out in doc/conf.py
install_requires = ['six', 'numpy', 'scipy', 'pandas', 'tables'] # , 'h5py'
extras_require = {} 


pwd = os.path.abspath(os.path.dirname(__file__))

# Get the long description from the README file
try:
	with open(os.path.join(pwd, 'README.rst'), encoding='utf-8') as f:
		long_description = f.read()
except OSError:
	long_description = ''

setup(
	name = 'rwa-python',
	version = '0.1',
	description = 'HDF5-based serialization library for Python datatypes',
	long_description = long_description,
	url = 'https://github.com/DecBayComp/RWA-python',
	author = 'François Laurent',
	author_email = 'francois.laurent@pasteur.fr',
	license = 'Apache 2.0',
	classifiers = [
		'License :: OSI Approved :: Apache Software License',
		'Programming Language :: Python :: 2',
		'Programming Language :: Python :: 2.7',
		'Programming Language :: Python :: 3',
		'Programming Language :: Python :: 3.5',
	],
	packages = ['rwa'],
	install_requires = install_requires,
	extras_require = extras_require,
)
