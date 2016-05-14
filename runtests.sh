#! /bin/bash

python setup.py install && (cd tests; py.test -v $@)