#!/bin/bash

# Test weak scaling.

server=${1}
maxnodes=${2}
for ((n=1;n<=${maxnodes};n*=2)); do
  test_block_size.sh ${server} /home/camc/code/dataflow/code/spark/simple_map.py ${n} /projects/visualization/cam/experiments/weak_scaling 12 64  1
  test_block_size.sh ${server} /home/camc/code/dataflow/code/spark/simple_map.py ${n} /projects/visualization/cam/experiments/weak_scaling 24 32  1
  test_block_size.sh ${server} /home/camc/code/dataflow/code/spark/simple_map.py ${n} /projects/visualization/cam/experiments/weak_scaling 48 16  1
  test_block_size.sh ${server} /home/camc/code/dataflow/code/spark/simple_map.py ${n} /projects/visualization/cam/experiments/weak_scaling 96 8   1
  test_block_size.sh ${server} /home/camc/code/dataflow/code/spark/simple_map.py ${n} /projects/visualization/cam/experiments/weak_scaling 192 4  1
  test_block_size.sh ${server} /home/camc/code/dataflow/code/spark/simple_map.py ${n} /projects/visualization/cam/experiments/weak_scaling 384 2  1
  test_block_size.sh ${server} /home/camc/code/dataflow/code/spark/simple_map.py ${n} /projects/visualization/cam/experiments/weak_scaling 768 1  1
done
