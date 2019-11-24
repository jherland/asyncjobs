#!/bin/sh

set -e

python3 -m pytest "$@"
python3 -m black --target-version=py36 --line-length=79 --skip-string-normalization .
python3 -m flake8 .
