#! /usr/bin/env bash

for file in `find . -name "result*.csv"`; do
    echo "plotting $file"
    ./plot.py $file "result"
done
