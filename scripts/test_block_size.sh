#!/bin/bash

if [[ $# -ne 7 ]]; then
    echo "usage: test_block_size.sh <server> <app> <nodes> <dst> <nblocks> <block_size (mb)> <nparts>"
    echo "   ex: test_spark.sh cc075 /home/camc/code/dataflow/code/spark/simple_map.py 8 /projects/visualization/cam/experiments/simple_map_strong 128 1024 2"
    exit -1
fi

app=${2}
nodes=$(($3*12))
dst=${4}
nblocks=${5}
block_sz=${6}
nparts=${7}
echo "nblocks: $nblocks"
echo "block_sz: $block_sz"
size=$((nblocks*block_sz))
echo "size: $size"
echo "nparts: ${nparts}"
echo "test_block_size ${1} ${app} cores=${nodes} dst=${dst} nblocks=${nblocks} block_size=${block_sz} partition_multiplier=${nparts}"
time $HOME/code/spark/bin/spark-submit --master spark://${1}:7077 ${app} --generate ${nblocks} ${block_sz} --dst ${dst} -n ${3} -z ${size} -p ${nparts} >> /projects/visualization/cam/experiment-logs/test_block_size-${nodes}nodes-${size}mb.out 2>&1 
