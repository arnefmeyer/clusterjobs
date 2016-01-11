.. -*- mode: rst -*-

clusterjobs
===========

Some helper classes to run (embarassingly) parallel jobs on an HPC cluster. 

Most of the code should work under Linux, specifically the SLURM backend (
haven't worked with Grid Engine for some while but it worked about one year 
ago).

TODO:
	- add documentation
    - remove some obsolete variables
    - replace 'shell' by 'executable'
    - use 'somepattern'.format(...) instead of old 'somepattern' % (...) style
    - make it work under windows


Dependencies
============

Works under Python 2.7 (Python 2.6 and Python 3.x have not been tested). All
used modules are part of the standard library that is distributed with Python.


Installation
============

Linux
-----

1. Install the latest version of Python 2.7

2. Install package by changing into the package directory and typing

	python setup.py install

