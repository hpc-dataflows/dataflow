"""
A -> B map microbenchmark.

Reads FILESIZE/24 double vectors per record. This is more memory efficient than reading one vector per record.

Requires NumPy (http://www.numpy.org/).
"""

from __future__ import print_function
import sys
import numpy as np
from pyspark import SparkContext
from glob import glob

def parseVectors(bin):
    arr= np.fromstring(bin[1],dtype=np.float64)
    arr=arr.reshape(arr.shape[0]/3,3)
    return (bin[0],arr)

def add_vec3(arr,vec):
    for i in xrange(len(arr[1])):
        arr[1][i] += vec
    return arr

def savebin(arr):
    import re
    basedir='/projects/visualization/cam/output/simple_map'
    idx=re.match(".*(.-..?)\.bin",arr[0]).group(1) # terribly specific
    outfilename=basedir+"/binary_output-"+str(idx)+".bin"
    outfile=open(outfilename,'w')
    outfile.write(arr[1].data)

if __name__ == "__main__":

    if len(sys.argv) != 2:
        print("Usage: simple_map <file>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="SimpleMap")

    rdd = sc.binaryFiles(sys.argv[1])
    A = rdd.map(parseVectors) #.cache() #(just cached to see size of one block)
    print("numPartitions(%d,%s): %d"%(A.id(),A.name(),A.getNumPartitions()))

    shift=np.array([25.25,-12.125,6.333],dtype=np.float64)
    B = A.map(lambda x: add_vec3(x,shift))
    print("numPartitions(%d,%s): %d"%(B.id(),B.name(),B.getNumPartitions()))
    #(todo, like aps_thresholder) B.foreach(lambda x: savebin(x,outdir))
    B.foreach(savebin)

    # import time
    # while True:
    #     time.sleep(5)

    sc.stop()
