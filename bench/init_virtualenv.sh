#!/usr/bin/env bash

rm -rf testgres

# create python virtualenv and activate it
virtualenv testgres
source testgres/bin/activate

# install testgres
pip install testgres

# deactivate virtualenv
deactivate
