#!/bin/bash
set -e

for example in `find $(dirname $(realpath $0)) -type f -name '*.go'`
do
    echo -e "Running example: $(basename $example)\n"
    go run $example
done
