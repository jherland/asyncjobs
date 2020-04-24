#!/bin/sh

set -e

nox
git ls-files \*.py | xargs python3 -m flake8 --ignore=E203
