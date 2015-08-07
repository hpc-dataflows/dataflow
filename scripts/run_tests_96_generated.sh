#!/bin/bash

server=${1}
nodes=${2}
test_block_size.sh ${server} /home/camc/code/dataflow/code/spark/simple_map.py ${2} /projects/visualization/cam/experiments/scaling_generated 16 64 4
test_block_size.sh ${server} /home/camc/code/dataflow/code/spark/simple_map.py ${2} /projects/visualization/cam/experiments/scaling_generated 32 64 4
test_block_size.sh ${server} /home/camc/code/dataflow/code/spark/simple_map.py ${2} /projects/visualization/cam/experiments/scaling_generated 64 64 4
test_block_size.sh ${server} /home/camc/code/dataflow/code/spark/simple_map.py ${2} /projects/visualization/cam/experiments/scaling_generated 128 64 4
test_block_size.sh ${server} /home/camc/code/dataflow/code/spark/simple_map.py ${2} /projects/visualization/cam/experiments/scaling_generated 256 64 4
test_block_size.sh ${server} /home/camc/code/dataflow/code/spark/simple_map.py ${2} /projects/visualization/cam/experiments/scaling_generated 512 64 4
test_block_size.sh ${server} /home/camc/code/dataflow/code/spark/simple_map.py ${2} /projects/visualization/cam/experiments/scaling_generated 1024 64 4
test_block_size.sh ${server} /home/camc/code/dataflow/code/spark/simple_map.py ${2} /projects/visualization/cam/experiments/scaling_generated 2048 64 4
test_block_size.sh ${server} /home/camc/code/dataflow/code/spark/simple_map.py ${2} /projects/visualization/cam/experiments/scaling_generated 4096 64 4
test_block_size.sh ${server} /home/camc/code/dataflow/code/spark/simple_map.py ${2} /projects/visualization/cam/experiments/scaling_generated 8192 64 4
test_block_size.sh ${server} /home/camc/code/dataflow/code/spark/simple_map.py ${2} /projects/visualization/cam/experiments/scaling_generated 16384 64 4
test_block_size.sh ${server} /home/camc/code/dataflow/code/spark/simple_map.py ${2} /projects/visualization/cam/experiments/scaling_generated 32768 64 4
