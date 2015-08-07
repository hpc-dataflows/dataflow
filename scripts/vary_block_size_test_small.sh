#!/bin/bash

# Total data size = 64 GiB, designed for smaller node counts

server=${1}
nodes=${2}
for ((i=1;i<=10;i++)); do
  test_block_size.sh ${server} /home/camc/code/dataflow/code/spark/simple_map.py ${nodes} /projects/visualization/cam/experiments/block_size 4096 16  ${i}
  test_block_size.sh ${server} /home/camc/code/dataflow/code/spark/simple_map.py ${nodes} /projects/visualization/cam/experiments/block_size 2048 32  ${i}
  test_block_size.sh ${server} /home/camc/code/dataflow/code/spark/simple_map.py ${nodes} /projects/visualization/cam/experiments/block_size 1024 64 ${i}
  test_block_size.sh ${server} /home/camc/code/dataflow/code/spark/simple_map.py ${nodes} /projects/visualization/cam/experiments/block_size 512 128  ${i}
  test_block_size.sh ${server} /home/camc/code/dataflow/code/spark/simple_map.py ${nodes} /projects/visualization/cam/experiments/block_size 256 256  ${i}
  test_block_size.sh ${server} /home/camc/code/dataflow/code/spark/simple_map.py ${nodes} /projects/visualization/cam/experiments/block_size 128 512 ${i}
  test_block_size.sh ${server} /home/camc/code/dataflow/code/spark/simple_map.py ${nodes} /projects/visualization/cam/experiments/block_size 64 1024  ${i}
  test_block_size.sh ${server} /home/camc/code/dataflow/code/spark/simple_map.py ${nodes} /projects/visualization/cam/experiments/block_size 32 2048  ${i}
  test_block_size.sh ${server} /home/camc/code/dataflow/code/spark/simple_map.py ${nodes} /projects/visualization/cam/experiments/block_size 16 4096  ${i}
done