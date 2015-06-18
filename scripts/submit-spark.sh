#! /bin/bash

# Usage:   submit-spark.sh <allocation> <time> <num_nodes> [enable_x11]
# Example: submit-spark.sh SDAV 08:00:00 12

if [ $# -lt 3 ]; then
    echo "Usage: submit-spark.sh <allocation> <time> <num_nodes> [enable_x11]"
    exit -1
fi

allocation=$1
time=$2
nodes=$3

enable_x11="-nox11"
if [ $# -gt 3 ]; then
    enable_x11=""
fi

#make sure spark-hostname file isn't still kicking around
if [ -e ~/visus-hostname ]; then
  rm ~/visus-hostname
fi

# submit
qsub -n $nodes -t $time -A $allocation -q pubnet${enable_x11} /home/camc/bin/start-spark.sh

count=0
while [ ! -e ~/spark-hostname ]; do 
  echo "Waiting for Spark to launch..."; sleep 3
  count=$((count+1))
  if [ $count -gt 20 ]
  then
    echo "Spark failed to launch within one minute."
    break
  fi
done

cat ~/spark-hostname
rm ~/spark-hostname


