#!/bin/bash

server=${1}
nodes=${2}
nruns=${3}
for ((i=0;i<${nruns};i++)); do
  test_block_size_reduce.sh ${server} /home/camc/code/dataflow/code/spark/simple_reduce.py ${2} /projects/visualization/cam/experiments/scaling_generated_reduce 16 64 4
done
for ((i=0;i<${nruns};i++)); do
  test_block_size_reduce.sh ${server} /home/camc/code/dataflow/code/spark/simple_reduce.py ${2} /projects/visualization/cam/experiments/scaling_generated_reduce 32 64 4
done
for ((i=0;i<${nruns};i++)); do
  test_block_size_reduce.sh ${server} /home/camc/code/dataflow/code/spark/simple_reduce.py ${2} /projects/visualization/cam/experiments/scaling_generated_reduce 64 64 4
done
for ((i=0;i<${nruns};i++)); do
  test_block_size_reduce.sh ${server} /home/camc/code/dataflow/code/spark/simple_reduce.py ${2} /projects/visualization/cam/experiments/scaling_generated_reduce 128 64 4
done
for ((i=0;i<${nruns};i++)); do
  test_block_size_reduce.sh ${server} /home/camc/code/dataflow/code/spark/simple_reduce.py ${2} /projects/visualization/cam/experiments/scaling_generated_reduce 256 64 4
done
for ((i=0;i<${nruns};i++)); do
  test_block_size_reduce.sh ${server} /home/camc/code/dataflow/code/spark/simple_reduce.py ${2} /projects/visualization/cam/experiments/scaling_generated_reduce 512 64 4
done
for ((i=0;i<${nruns};i++)); do
  test_block_size_reduce.sh ${server} /home/camc/code/dataflow/code/spark/simple_reduce.py ${2} /projects/visualization/cam/experiments/scaling_generated_reduce 1024 64 4
done
for ((i=0;i<${nruns};i++)); do
  test_block_size_reduce.sh ${server} /home/camc/code/dataflow/code/spark/simple_reduce.py ${2} /projects/visualization/cam/experiments/scaling_generated_reduce 2048 64 4
done
for ((i=0;i<${nruns};i++)); do
  test_block_size_reduce.sh ${server} /home/camc/code/dataflow/code/spark/simple_reduce.py ${2} /projects/visualization/cam/experiments/scaling_generated_reduce 4096 64 4
done
for ((i=0;i<${nruns};i++)); do
  test_block_size_reduce.sh ${server} /home/camc/code/dataflow/code/spark/simple_reduce.py ${2} /projects/visualization/cam/experiments/scaling_generated_reduce 8192 64 4
done
for ((i=0;i<${nruns};i++)); do
  test_block_size_reduce.sh ${server} /home/camc/code/dataflow/code/spark/simple_reduce.py ${2} /projects/visualization/cam/experiments/scaling_generated_reduce 16384 64 4
done
for ((i=0;i<${nruns};i++)); do
  test_block_size_reduce.sh ${server} /home/camc/code/dataflow/code/spark/simple_reduce.py ${2} /projects/visualization/cam/experiments/scaling_generated_reduce 32768 64 4
done