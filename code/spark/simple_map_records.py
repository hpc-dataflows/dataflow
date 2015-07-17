"""
A -> B map microbenchmark.

Reads one double vector per record. This is highly inefficient use of
space: 64mb input requires ~350mb memory to store the array of
records.

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
    A = lines.map(parseVector) # is .cache() this causing unnecessary/redundant processing when memory is overflowed? At least for this case, cache isn't needed anyway. Basically, I'm afraid that calling cache with insufficient memory performs computation to create the cache entry, which then pushes previous entries out. In the end, when the data is actually used it needs to be computed again. As my pappy always told me, "be careful with cache!"

    print("numPartitions(%d,%s): %d"%(A.id(),A.name(),A.getNumPartitions()))

    shift=np.array([25.25,-12.125,6.333],dtype=np.float64)
    B = A.map(construct_apply_shift(shift))
    print("numPartitions(%d,%s): %d"%(B.id(),B.name(),B.getNumPartitions()))

    B.foreachPartition(savebin)

    sc.stop()
