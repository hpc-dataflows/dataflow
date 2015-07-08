"""
[A,A,A,A,A] -> B reduce microbenchmark.

Calculates average of a set of binary vectors.

Requires NumPy (http://www.numpy.org/).
"""

from __future__ import print_function
from __future__ import division
import sys
import numpy as np
from pyspark import SparkContext
from glob import glob

def parseVector(line):
    arr= np.fromstring(line,dtype=np.float64)
    return arr

def avg_vec3(a,b):
    a[0] = (a[0]+b[0])/2.0
    a[1] = (a[1]+b[1])/2.0
    a[2] = (a[2]+b[2])/2.0
    return a

def savetxt(x):
    basedir='/tmp'  #'/mnt'
    idx=len(glob(basedir+'/reduce_output-*.txt'))
    outfilename=basedir+"/reduce_output-"+str(idx).zfill(2)+".txt"
    outfile=open(outfilename,'w')
    outfile.write(str(x[0])+" "+str(x[1])+" "+str(x[2])+"\n")

def savebin(x):
    basedir='/tmp'  #'/mnt'
    idx=len(glob(basedir+'/reduce_output-*.bin'))
    outfilename=basedir+"/reduce_output-"+str(idx).zfill(2)+".bin"
    outfile=open(outfilename,'w')
    outfile.write(str(x.data))

if __name__ == "__main__":

    if len(sys.argv) != 2:
        print("Usage: simple_reduce <file>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="SimpleReduce")

    lines = sc.binaryRecords(sys.argv[1],24) #three doubles per vector (record)
    A = lines.map(parseVector).cache()

    print("numPartitions(%d,%s): %d"%(A.id(),A.name(),A.getNumPartitions()))

    avg = A.reduce(avg_vec3)

    savebin(avg)
    savetxt(avg)

    sc.stop()
