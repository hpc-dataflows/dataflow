#!/bin/bash

if [[ $# -ne 6 ]]; then
    echo "usage: test_scaling.sh <server> <app> <nodes> <src> <dst> <data_size>"
    echo "   ex: test_spark.sh cc075 /home/camc/code/dataflow/code/spark/simple_map.py /projects/visualization/cam/vectors/8gb /projects/visualization/cam/experiments/simple_map_strong 8"
    exit -1
fi

app=${2}
nodes=$(($3*12))
src=${4}
dst=${5}
echo "test_scaling ${1} ${app} cores=${nodes} src=${src} dst=${dst} size=${6}"
time $HOME/code/spark/bin/spark-submit --master spark://${1}:7077 ${app} --src ${src} --dst ${dst} -n ${3} -z ${6} >> /projects/visualization/cam/experiment-logs/test_scaling-${nodes}nodes-${6}gb.out 2>&1 

