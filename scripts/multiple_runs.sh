#!/bin/bash

# Test weak scaling.

server=${1}
nodes=${2}
nruns=${3}
multiplier=${4}
for ((n=1;n<=${nruns};n++)); do
  test_block_size.sh ${server} /home/camc/code/dataflow/code/spark/simple_map.py ${nodes} /projects/visualization/cam/experiments/multiple_runs 256 4  ${multiplier}
done
