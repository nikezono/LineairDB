#! /usr/bin/env bash

cd $(dirname $0)/working
for file in `find . -name "result*.csv"`; do
    echo "plotting $file"
    ../plot.py $file "result"
done
