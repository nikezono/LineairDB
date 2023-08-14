#! /usr/bin/env bash

cd $(dirname $0)/..
git submodule update --init --recursive
mkdir -p build
cd $_
cmake .. -DCMAKE_BUILD_TYPE=Release -G Ninja
ninja bench/index_with_phantom

cd ..; mkdir -p result;cd $_
cp ../build/bench/index_with_phantom bench
pwd
rm result*

for populated in Populated NotPopulated; do
    # create CSV file
    # $1. scan or epoch
    # $2. tag
    function append() {
        if [ ! -e result$populated$3.csv ]; then
            touch result$populated$3.csv
            echo "name,threads,cps,abort_insert_ps,abort_scan_ps,abort_rate,$2,tag" >>result$populated$3.csv
        fi
        cat indexbench_result.json | jq -r "[.structure,.threads,.cps,.abort_insert_ps,.abort_scan_ps,.abort_rate,$1,\"$2\"]|@csv" >>result$populated$3.csv
    }

    # varying threads
    for structure in OpenBw+PLI OpenBw+OPLI OpenBwTree; do
        for thread in 1 2 3 4 8 16 32 48 64 80 96 112 128 144; do
            for scan in 0 5 20 50 95 100; do
                for iteration in 1 2 3; do
                    if test $populated = "Populated"; then
                        env EPOCH=40 ./bench -s $structure -t $thread -d 5000 -p $scan -P true
                    else
                        env EPOCH=40 ./bench -s $structure -t $thread -d 5000 -p $scan
                    fi
                    append $scan threads forThreadsScan$scan
                done
            done
        done
    done

    # varying scan proportion
    for structure in OpenBw+PLI OpenBw+OPLI OpenBwTree; do
        for scan in 0 10 20 30 40 50 60 70 80 90 100; do
            for iteration in 1 2 3; do
                if test $populated = "Populated"; then
                    env EPOCH=40 ./bench -s $structure -t 72 -d 5000 -p $scan -P true
                else
                    env EPOCH=40 ./bench -s $structure -t 72 -d 5000 -p $scan
                fi
                append $scan scan forScan
            done
        done
    done

    # varying epoch
    for structure in OpenBw+PLI OpenBw+OPLI OpenBwTree; do
        for epoch in 1 10 20 40 100 200 400; do
            for iteration in 1 2 3; do
                if test $populated = "NotPopulated"; then
                    env EPOCH=$epoch ./bench -s $structure -t 72 -d 5000 -p 95
                    append $epoch epoch forEpoch
                fi
            done
        done
    done

    # varying logic_time
    for structure in OpenBw+PLI OpenBw+OPLI OpenBwTree; do
        for logictime in 0 1 5 10 20 40 60 80 100; do
            for iteration in 1 2 3; do
                if test $populated = "NotPopulated"; then
                    env EPOCH=40 ./bench -s $structure -t 72 -d 5000 -p 95 -l $logictime
                    append $logictime logictime forLogicTime
                fi
            done
        done
    done

    # varying scan limit
    for structure in OpenBw+PLI OpenBw+OPLI OpenBwTree; do
        for scanlimit in 1 2 4 8 16 32 48 64 72 100 250 500 750 1000; do
            for iteration in 1 2 3; do
                if test $populated = "NotPopulated"; then
                    env EPOCH=40 ./bench -s $structure -t  -d 5000 -p 95 -L $scanlimit
                    append $scanlimit scanlimit forScanLimit
                fi
            done
        done
    done
done
