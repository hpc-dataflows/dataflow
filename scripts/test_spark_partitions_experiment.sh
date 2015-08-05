#!/bin/bash

if [[ $# -ne 4 ]]; then
    echo "usage: test_spark.sh <master> <filelist> <pct_thresh> <sigma>"
    echo "   ex: test_spark.sh cc075 /projects/visualization/cam/tilt_corrected_2-images/imagelist.txt 0.3 5"
    exit -1
fi

#perform experiment twice, but only keep the second result (so optimal system caching is being used. Otherwise, previous runs can affect result)
for ((i=130;i>=0;i--)); do 
for ((j=0;j<2;j++)); do 
  time $HOME/code/spark/bin/spark-submit --master spark://${1}:7077 /home/camc/code/dataflow/code/spark/aps_threshold.py --dst /projects/visualization/cam/output -p ${3} -s ${4} --filelist ${2} -n ${i} >  /projects/visualization/cam/experiments/partitions-n${i}-p0.3-s5.out 2>&1 
done
done

