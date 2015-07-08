"""
[A,A] -> B reducemap microbenchmark.

Computes dot product of successive pairs of vectors

Requires NumPy (http://www.numpy.org/).
"""

from __future__ import print_function
import sys
import numpy as np
from pyspark import SparkContext
from glob import glob

def parseVector(partitionIndex,iterator,num_partitions):
    for i,line in enumerate(iterator) :
        arr= np.fromstring(line,dtype=np.float64)
        yield (i*num_partitions+partitionIndex,arr)

def parseVectorFunctor(num_partitions):
    return lambda j,k: parseVector(j,k,num_partitions)

def dot_vec3(a,b):
    return a[0]*b[0] + a[1]*b[1] + a[2]*b[2]

def savebin(iterator):
    basedir='/tmp'  #'/mnt'
    idx=len(glob(basedir+'/reducemap_binary_output*.bin'))
    outfilename=basedir+"/reducemap_binary_output-"+str(idx).zfill(2)+".bin"
    outfile=open(outfilename,'w')
    for x in iterator:
        outfile.write(str(x[1].data))

if __name__ == "__main__":

    if len(sys.argv) != 3:
        print("Usage: simple_reducemap <fileA> <fileB>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="SimpleReduceMap")
#todo: https://spark.apache.org/docs/latest/configuration.html#spark-properties
#  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    linesA = sc.binaryRecords(sys.argv[1],24)
    A = linesA.mapPartitionsWithIndex(parseVectorFunctor(linesA.getNumPartitions()),True).cache()
    A.getStorageLevel()
    print(A.getStorageLevel())

    linesB = sc.binaryRecords(sys.argv[2],24)
    B = linesB.mapPartitionsWithIndex(parseVectorFunctor(linesB.getNumPartitions()),True).cache()

    C = A.union(B).cache()

    D = C.reduceByKey(dot_vec3).cache()
    print("numPartitions(%d,%s): %d"%(D.id(),D.name(),D.getNumPartitions()))
    #D.foreach(lambda v: print(str(v)))
    D.getStorageLevel()
    print(D.getStorageLevel())

    D.foreachPartition(savebin)

    # pause execution to examine memory usage
    import time
    while True:
        time.sleep(5)
        pass

    sc.stop()
