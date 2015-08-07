#!/bin/bash

server=${1}
test_scaling.sh ${server} /home/camc/code/dataflow/code/spark/simple_map.py 4 /projects/visualization/cam/vectors/1gb /projects/visualization/cam/experiments/scaling/1gb       1   
test_scaling.sh ${server} /home/camc/code/dataflow/code/spark/simple_map.py 4 /projects/visualization/cam/vectors/2gb /projects/visualization/cam/experiments/scaling/2gb       2   
test_scaling.sh ${server} /home/camc/code/dataflow/code/spark/simple_map.py 4 /projects/visualization/cam/vectors/4gb /projects/visualization/cam/experiments/scaling/4gb       4   
test_scaling.sh ${server} /home/camc/code/dataflow/code/spark/simple_map.py 4 /projects/visualization/cam/vectors/8gb /projects/visualization/cam/experiments/scaling/8gb       8   
test_scaling.sh ${server} /home/camc/code/dataflow/code/spark/simple_map.py 4 /projects/visualization/cam/vectors/16gb /projects/visualization/cam/experiments/scaling/16gb     16  
test_scaling.sh ${server} /home/camc/code/dataflow/code/spark/simple_map.py 4 /projects/visualization/cam/vectors/32gb /projects/visualization/cam/experiments/scaling/32gb     32  
test_scaling.sh ${server} /home/camc/code/dataflow/code/spark/simple_map.py 4 /projects/visualization/cam/vectors/64gb /projects/visualization/cam/experiments/scaling/64gb     64  
test_scaling.sh ${server} /home/camc/code/dataflow/code/spark/simple_map.py 4 /projects/visualization/cam/vectors/128gb /projects/visualization/cam/experiments/scaling/128gb   128 
test_scaling.sh ${server} /home/camc/code/dataflow/code/spark/simple_map.py 4 /projects/visualization/cam/vectors/256gb /projects/visualization/cam/experiments/scaling/256gb   256 
test_scaling.sh ${server} /home/camc/code/dataflow/code/spark/simple_map.py 4 /projects/visualization/cam/vectors/512gb /projects/visualization/cam/experiments/scaling/512gb   512 
test_scaling.sh ${server} /home/camc/code/dataflow/code/spark/simple_map.py 4 /projects/visualization/cam/vectors/1024gb /projects/visualization/cam/experiments/scaling/1024gb 1024
test_scaling.sh ${server} /home/camc/code/dataflow/code/spark/simple_map.py 4 /projects/visualization/cam/vectors/2048gb /projects/visualization/cam/experiments/scaling/2048gb 2048
