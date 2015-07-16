"""
Image analysis of APS tiff image stack.

Requires http://www.numpy.org/.
Requires http://scikit-image.org/.
"""

from __future__ import print_function
from __future__ import division
from pyspark import SparkContext

def readTiff(name):
    """returns a tuple of the slice index and loaded tiff"""
    import skimage.io
    from glob import glob
    import re
    num=int(re.match(".*image(...).tif",name).group(1))  #yeah, yeah...
    print("Reading %s, num = %d..."%(name,num))
    img=skimage.io.imread(name)/65535 # read image data using skimage.io (because libtiff.read_image doesn't work!)
    print("image range: "+str(img.min())+" to "+str(img.max()))
    return ([num],img,1)  # (slice, data, running_average_count)

def threshold(x,sigma):
    """crop and smooth image, then calculate multithreshold"""
    import skimage.filters,skimage.util
    cropped=skimage.util.crop(x[1],(50,100,50,100))
    img=skimage.filters.gaussian_filter(x[1],sigma)
    # since we don't have Matlab's multithresh, and furthermore since that's really just a hack...
    # 1) reduce image range to mean +- stddev
    # 2) use otsu's method to produce a single threshold
    # afterwards we'll average the thresholds
    # This is also just a hack, based on experimentation using ImageJ.
    # (It's possible we want to average all these images prior to utilizing this method.)
    sdv=smoothed.std(); mean=smoothed.mean()
    img=smoothed[smoothed<(mean+sdv)]
    img=img[img>(mean-sdv)]
    thresh=skimage.filters.threshold_otsu(img)
    return (x[0],thresh,1)

def threshold_func(sigma):
    return lambda x: crop_and_smooth(x,sigma)
        
def smooth_and_apply_threshold(x,sigma,thresh):
    """smooth and apply the given threshold"""
    import skimage.filters
    img=skimage.filters.gaussian_filter(x[1],sigma)
    img[img<thresh]=0.15
    img[img>thresh]=0.85
    return (x[0],img,1)

def threshold_func(sigma):
    return lambda x: crop_and_smooth(x,sigma)
        
def avg(a,b):
    newidx=a[0]; newidx.extend(b[0])
    avgimg=(a[1]*a[2]+b[1]*b[2])/(a[2]+b[2]) #running average
    newden=a[2]+b[2]
    return (newidx,avgimg,newden)

def savebin(x):
    from PIL import Image 
    basedir='/tmp'  #'/mnt'
    idx=len(glob(basedir+'/avg_output-*.tif'))
    outfilename=basedir+"/avg_output-"+str(idx).zfill(2)+".tif"
    print("saving %s..."%outfilename)
    result=Image.fromarray(x[1])
    result.save(outfilename)

if __name__ == "__main__":

    import sys
    if len(sys.argv) < 2:
        print("Usage: threshold <tiff_dir> <threshold_percent> <gaussian_sigma>", file=sys.stderr)
        exit(-1)

    if len(sys.argv) > 2:
        print("TODO: currently <threshold_percent> and <gaussian_sigma> are ignored.")
    threshold_percent=0.3
    gaussian_sigma=5

    sc = SparkContext(appName="APS_Thresholder")

    filelist=sc.textFile(sys.argv[1],30)
    stack=filelist.map(readTiff).cache()
    print("numPartitions(%d,%s): %d"%(stack.id(),stack.name(),stack.getNumPartitions()))
    #avg = stack.reduce(avg)
    #savebin(avg)

    # calculate threshold using partial stack
    tmin_idx=int(threshold_percent*stack.count())
    tmax_idx=stack.count()-tmin_idx
    threshold_stack=stack.filter(lambda x: x[0]>=tmin_idx and x[1]<=tmax_idx)
    thresholds=threshold_stack.map(threshold_func(gaussian_sigma))
    thresh=thresholds.reduce(avg)
    print("threshold is %f"%thresh)
    
    # apply threshold to entire stack
    stack=stack.map(apply_threshold_func(thresh))
    stack.foreach(savebin)
    
    sc.stop()
