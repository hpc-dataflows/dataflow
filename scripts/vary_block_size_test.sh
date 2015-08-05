#!/bin/bash

# Total data size = 1024 GiB, designed for smaller node counts

server=${1}
nodes=${2}
test_block_size.sh ${server} /home/camc/code/dataflow/code/spark/simple_map.py ${nodes} /projects/visualization/cam/experiments/block_size/1024gb 128 8192    
test_block_size.sh ${server} /home/camc/code/dataflow/code/spark/simple_map.py ${nodes} /projects/visualization/cam/experiments/block_size/1024gb 256 4096  
test_block_size.sh ${server} /home/camc/code/dataflow/code/spark/simple_map.py ${nodes} /projects/visualization/cam/experiments/block_size/1024gb 512 2048  
test_block_size.sh ${server} /home/camc/code/dataflow/code/spark/simple_map.py ${nodes} /projects/visualization/cam/experiments/block_size/1024gb 1024 1024 
test_block_size.sh ${server} /home/camc/code/dataflow/code/spark/simple_map.py ${nodes} /projects/visualization/cam/experiments/block_size/1024gb 2048 512  
test_block_size.sh ${server} /home/camc/code/dataflow/code/spark/simple_map.py ${nodes} /projects/visualization/cam/experiments/block_size/1024gb 4096 256   
test_block_size.sh ${server} /home/camc/code/dataflow/code/spark/simple_map.py ${nodes} /projects/visualization/cam/experiments/block_size/1024gb 8192 128  
test_block_size.sh ${server} /home/camc/code/dataflow/code/spark/simple_map.py ${nodes} /projects/visualization/cam/experiments/block_size/1024gb 16384 64  
test_block_size.sh ${server} /home/camc/code/dataflow/code/spark/simple_map.py ${nodes} /projects/visualization/cam/experiments/block_size/1024gb 32768 32  
test_block_size.sh ${server} /home/camc/code/dataflow/code/spark/simple_map.py ${nodes} /projects/visualization/cam/experiments/block_size/1024gb 65536 16  
test_block_size.sh ${server} /home/camc/code/dataflow/code/spark/simple_map.py ${nodes} /projects/visualization/cam/experiments/block_size/1024gb 64 16384  
test_block_size.sh ${server} /home/camc/code/dataflow/code/spark/simple_map.py ${nodes} /projects/visualization/cam/experiments/block_size/1024gb 32 32768  
test_block_size.sh ${server} /home/camc/code/dataflow/code/spark/simple_map.py ${nodes} /projects/visualization/cam/experiments/block_size/1024gb 16 65536  
