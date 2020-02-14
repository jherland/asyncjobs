#!/bin/sh

set -e

python3 -m pytest -x --log-level=debug "$@"
python3 -m black --target-version=py36 --line-length=79 --skip-string-normalization .
git ls-files \*.py | xargs python3 -m flake8 --ignore=E203
