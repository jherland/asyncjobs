#!/bin/sh

set -e

nox
python3 -m black --target-version=py36 --line-length=79 --skip-string-normalization .
git ls-files \*.py | xargs python3 -m flake8 --ignore=E203
