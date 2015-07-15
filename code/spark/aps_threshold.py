"""
Image analysis of APS tiff image stack.

Requires http://www.numpy.org/.
Requires http://scikit-image.org/.
"""

from __future__ import print_function
from __future__ import division
import sys
import numpy as np
from skimage import io,data,filters
from pyspark import SparkContext
#from libtiff import TIFF
from glob import glob
import re
#from sys import stdout
from PIL import Image 

def readTiff(name):
    """returns a tuple of the slice index and loaded tiff"""
    num=int(re.match(".*image(...).tif",name).group(1))  #yeah, yeah...
    print("Reading %s, num = %d..."%(name,num))
    #tif=TIFF.open(name)     # read image info using libtiff (don't worry, this only loads the metadata, very fast)
    #print(tif.info())
    img=io.imread(name)/65535 # read image data using skimage.io (because libtiff.read_image doesn't work!)
    print("image range: "+str(img.min())+" to "+str(img.max()))
    return ([num],img,1)  # (slice, data, running_average_count)

def imgavg(a,b):
    print("reducing...")
    newidx=a[0]; newidx.extend(b[0])
    avgimg=(a[1]*a[2]+b[1]*b[2])/(a[2]+b[2]) #running average
    newden=a[2]+b[2]
    return (newidx,avgimg,newden)

def savebin(x):
    basedir='/tmp'  #'/mnt'
    idx=len(glob(basedir+'/avg_output-*.tif'))
    outfilename=basedir+"/avg_output-"+str(idx).zfill(2)+".tif"
    print("saving %s..."%outfilename)
    result=Image.fromarray(x[1])
    result.save(outfilename)

if __name__ == "__main__":

    if len(sys.argv) != 2:
        print("Usage: threshold <tiff_dir> <threshold_percent>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="APS_Thresholder")

    filelist=sc.textFile(sys.argv[1],30)
    stack=filelist.map(readTiff).cache()
    print("numPartitions(%d,%s): %d"%(stack.id(),stack.name(),stack.getNumPartitions()))
    avg = stack.reduce(imgavg)
    savebin(avg)

    sc.stop()
