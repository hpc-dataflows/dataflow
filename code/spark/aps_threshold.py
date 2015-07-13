"""
Image analysis of APS tiff image stack.

Requires http://www.numpy.org/.
Requires http://scikit-image.org/.
"""

from __future__ import print_function
from __future__ import division
import sys
import numpy as np
from skimage import data,filter
from pyspark import SparkContext
from libtiff import TIFF
import re

def readTiff(name):
    """returns a tuple of the slice index and loaded tiff"""
    print("Reading %s..."%name)
    num=re.match("image(...).tif",name)  #yeah, yeah...
    tif=TIFF.open(name)
    return (num,tif,1)

def imgavg(a,b):
    newidx=a[0] #arbitrary
    avgimg=(a[1]*a[2]+b[1]*b[2])/(a[2]+b[2]) #running average
    newden=a[2]+b[2]
    return (newidx,avgimg,newden)

def savebin(x):
    basedir='/tmp'  #'/mnt'
    idx=len(glob(basedir+'/avg_output-*.tif'))
    outfilename=basedir+"/avg_output-"+str(idx).zfill(2)+".tif"
    outfile=TIFF.open(outfilename,mode='w')
    outfile.write_image(x[1])

if __name__ == "__main__":

    if len(sys.argv) != 2:
        print("Usage: threshold <tiff_dir> <threshold_percent>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="APS_Thresholder")

    filelist=sc.textFile(sys.argv[1])
    stack=filelist.map(readTiff).cache()
    
    print("numPartitions(%d,%s): %d"%(stack.id(),stack.name(),stack.getNumPartitions()))

    avg = stack.reduce(imgavg)

    savebin(avg)

    sc.stop()
