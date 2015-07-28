"""
A -> B map microbenchmark.

Reads FILESIZE/24 double vectors per record. This is more memory efficient than reading one vector per record.

Requires NumPy (http://www.numpy.org/).
"""

from __future__ import print_function
import numpy as np
from pyspark import SparkContext

def parseVectors(bin):
    arr= np.fromstring(bin[1],dtype=np.float64)
    arr=arr.reshape(arr.shape[0]/3,3)
    return (bin[0],arr)

def add_vec3(arr,vec):
    for i in xrange(len(arr[1])):
        arr[1][i] += vec
    return arr

def savebin(arr,dst):
    import re
    # expects input names of the form <name>X-##?.bin (ex: imageA-3.bin, imageK-97.bin)
    idx=re.match(".*(.-..?)\.bin",arr[0]).group(1) # terribly specific
    outfilename=dst+"/output-"+str(idx)+".bin"
    outfile=open(outfilename,'w')
    outfile.write(arr[1].data)

if __name__ == "__main__":

    import argparse
    parser = argparse.ArgumentParser(description="Simple Map Microbenchmark")
    parser.add_argument("-s","--src",help="directory containing input files")
    parser.add_argument("-d","--dst",help="directory to write output files")
    args = parser.parse_args()

    sc = SparkContext(appName="SimpleMap")

    # read input files
    rdd = sc.binaryFiles(args.sourcedir)
    A = rdd.map(parseVectors) #.cache() # cached to see size of one block, not necessary for app
    print("numPartitions(%d,%s): %d"%(A.id(),A.name(),A.getNumPartitions()))

    # apply simple operation (V'=V+V0)
    shift=np.array([25.25,-12.125,6.333],dtype=np.float64)
    B = A.map(lambda x: add_vec3(x,shift))
    print("numPartitions(%d,%s): %d"%(B.id(),B.name(),B.getNumPartitions()))

    # write results
    outdir=args.dst+"/simple_map"
    print("outdir is "+outdir)
    from os import makedirs
    try:
        makedirs(outdir)
    except Exception as e:
        #print("exception: "+str(e))   #it's okay if directory already exists...
        pass
    B.foreach(lambda x: savebin(x,outdir))

    # used with .cache() above to examine RDD memory usage
    # import time
    # while True:
    #     time.sleep(5)

    sc.stop()
