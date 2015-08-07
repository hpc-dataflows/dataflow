#! /bin/bash

# Usage:   submit-spark.sh <allocation> <time> <num_nodes> <queue>
# Example: submit-spark.sh SDAV 08:00:00 12 pubnet-nox11

if [ $# -lt 4 ]; then
    echo "Usage: submit-spark.sh <allocation> <time> <num_nodes> <queue>"
    echo "Example: submit-spark.sh SDAV 08:00:00 12 pubnet-nox11"
    exit -1
fi

allocation=$1
time=$2
nodes=$3
queue=$4

#make sure spark-hostname file isn't still kicking around
if [ -e ~/spark-hostname ]; then
  rm ~/spark-hostname
fi

# submit
qsub -n $nodes -t $time -A $allocation -q ${queue} /home/camc/bin/start-spark.sh

while [ ! -e ~/spark-hostname ]; do 
  echo "Waiting for Spark to launch..."; sleep 5
done

cat ~/spark-hostname
rm ~/spark-hostname


