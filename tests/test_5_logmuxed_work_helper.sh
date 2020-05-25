#!/bin/sh

echo "$1"
if [ -n "$3" ]; then
    sleep "$3"
fi
echo "$2" >&2
