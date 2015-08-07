"""
Image analysis of APS tiff image stack.

Requires http://www.numpy.org/.
Requires http://scikit-image.org/.
"""

from __future__ import print_function
from __future__ import division
import sys
import numpy as np
from skimage import io,data,filter
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


from PIL import Image 
import skimage.io
import matplotlib as mpl

tif=libtiff.TIFF.open('/Users/cameronchristensen/data/tilt_corrected_2-images/image001.tif')
tif.info()  # to get vmin and vmax, try to do this programatically, also note these are the same for every image in the stack, which is no doubt important for normalization consistency.
im=skimage.io.imread('/Users/cameronchristensen/data/tilt_corrected_2-images/image000.tif')
img_corr=im/65535  #note: im has different range than tif.read_image(). I don't know what the hell tif.read_image is!
norm=mpl.colors.Normalize(vmin=12917,vmax=20356)
#plt.imshow(im,cmap=cm.gray,norm=norm) # can use this to plot interactively
img_corr.dtype  #should now be float64
result=Image.fromarray(img_corr)
result.save('/tmp/test.tif')


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
