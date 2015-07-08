"""
A -> B map microbenchmark.

Requires NumPy (http://www.numpy.org/).
"""

from __future__ import print_function
import sys
import numpy as np
from pyspark import SparkContext
from glob import glob

def parseVector(line):
    arr= np.fromstring(line,dtype=np.float64)
    return arr

def add_vec3(a,b):
    a[0] += b[0]
    a[1] += b[1]
    a[2] += b[2]
    return a

def construct_apply_shift(shift):
    return lambda p: add_vec3(p,shift)
        
def savebin(iterator):
    basedir='/tmp'  #'/mnt'
    idx=len(glob(basedir+'/binary_output*.bin'))
    outfilename=basedir+"/binary_output-"+str(idx).zfill(2)+".bin"
    outfile=open(outfilename,'w')
    for x in iterator:
        outfile.write(x.data)

if __name__ == "__main__":

    if len(sys.argv) != 2:
        print("Usage: simple_map <file>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="SimpleMap")

    lines = sc.binaryRecords(sys.argv[1],24) #three doubles per vector (record)
    A = lines.map(parseVector).cache()

    print("numPartitions(%d,%s): %d"%(A.id(),A.name(),A.getNumPartitions()))

    shift=np.array([25.25,-12.125,6.333],dtype=np.float64)
    B = A.map(construct_apply_shift(shift))
    print("numPartitions(%d,%s): %d"%(B.id(),B.name(),B.getNumPartitions()))

    B.foreachPartition(savebin)

    sc.stop()
