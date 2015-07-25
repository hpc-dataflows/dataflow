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

def crop(x):
    import skimage.util
    return (x[0],x[1][50:100,50:-100],1)

def smooth(x,sigma):
    import skimage.filter
    return (x[0],skimage.filter.gaussian_filter(x[1],sigma),1)

def thresh(x):
    import skimage.filter
    img=x[1]
    sdv=img.std(); mean=img.mean()
    img=img[img<(mean+sdv)]
    img=img[img>(mean-sdv)]
    thresh=skimage.filter.threshold_otsu(img)
    return (x[0],thresh,1)

def threshold(x,sigma):
    """crop and smooth image, then calculate multithreshold"""
    import skimage.filter,skimage.util
    cropped=x[1][50:100,50:-100]
    img=skimage.filter.gaussian_filter(x[1],sigma)
    # since we don't have Matlab's multithresh, and furthermore since that's really just a hack...
    # 1) reduce image range to mean +- stddev
    # 2) use otsu's method to produce a single threshold
    # afterwards we'll average the thresholds
    # This is also just a hack, based on experimentation using ImageJ.
    # (It's possible we want to average all these images prior to utilizing this method.)
    sdv=img.std(); mean=img.mean()
    img=img[img<(mean+sdv)]
    img=img[img>(mean-sdv)]
    thresh=skimage.filter.threshold_otsu(img)
    return (x[0],thresh,1)

def smooth_and_apply_threshold(x,sigma,thresh):
    """smooth and apply the given threshold"""
    import skimage.filter
    img=skimage.filter.gaussian_filter(x[1],sigma)
    img[img<thresh]=0.15
    img[img>thresh]=0.85
    return (x[0],img,1)

def apply_threshold(x,thresh):
    """apply the given threshold"""
    img=x[1]
    img[img<thresh]=0.15
    img[img>thresh]=0.85
    return (x[0],img,1)

def avg(a,b):
    newidx=a[0]; newidx.extend(b[0])
    result=(a[1]*a[2]+b[1]*b[2])/(a[2]+b[2]) #running average
    newden=a[2]+b[2]
    return (newidx,result,newden)

def savebin(x,dst):
    from PIL import Image 
    from glob import glob
    basedir=dst
    idx=x[0][0]
    outfilename=basedir+"/img-"+str(idx).zfill(3)+".tif"
    print("saving %s..."%outfilename)
    result=Image.fromarray(x[1])
    result.save(outfilename)

def savetxt(x):
    from glob import glob
    basedir='/mnt/share/output' # '/tmp'  #'/mnt'
    #idx=len(glob(basedir+'/thresh-*.txt'))  #race conditions when using /mnt/share (nfs share), reasonable to expect for any non-serial execition
    idx=x[0][0]
    outfilename=basedir+"/thresh-"+str(idx).zfill(3)+".txt"
    print("saving %s..."%outfilename)
    outfile=open(outfilename,'w')
    outfile.write(str(x[0])+": "+str(x[1])+"\n")

def noop(x):
    print("noop")

def genfilelist(path,ext):
    import os, os.path, tempfile
    files = os.listdir(path)
    filelist =  [ os.path.join(path,f) for f in files if f.endswith(ext) ]
    outfile = tempfile.NamedTemporaryFile(delete=False,prefix="/projects/visualization/cam/scratch")
    for f in filelist:
        outfile.write(f + '\n')
    outfile.close()
    return outfile.name

if __name__ == "__main__":

    import argparse
    parser = argparse.ArgumentParser(description="APS Image Processing (for Nicola Ferier).")
    parser.add_argument("-l","--filelist",help="text file containing names of input files")
    parser.add_argument("-z","--path",help="path containing files")
    parser.add_argument("-x","--ext",default=".tif",help="file extension (usually tif)")
    parser.add_argument("-p","--percent",type=float,default=0.3,help="filelist containing names of input files")
    parser.add_argument("-s","--sigma",type=float,default=5,help="sigma to use for gaussing smoothing")
    parser.add_argument("-d","--dst",default="/tmp",help="destination path")
    parser.add_argument("-n","--num_partitions",default=16,help="number of partitions to create, each with num_files/num_partitions records")
    parser.add_argument('--nocache', dest='nocache',default=False,action='store_true',help="cache image stack before thresholding")
    parser.add_argument('--granular', dest='granular',default=False,action='store_true',help="granular image processing operations (vs grouped)")
    args = parser.parse_args()

    threshold_percent=args.percent
    gaussian_sigma=args.sigma

    sc = SparkContext(appName="APS_Thresholder")

    # import time
    # t0=time.clock()

    filelist=sc.emptyRDD()
    if args.path != None:
        filelist = genfilelist(args.path, args.ext)
        filelist = sc.textFile(filelist)
    else:
        filelist=sc.textFile(args.filelist)

    # threshold_stack.foreach(noop)  #useful to force pipeline to execute for debugging
    # tmark=time.clock()
    # print("generate and read filelist: %0.6f"%(tmark-to))
    # t0=tmark

    slice_count=filelist.count()
    filelist.repartition(args.num_partitions)  # Or maybe just slice_count, but I suspect file size plays a significant role in how many records per partition is optimal.

    stack=filelist.map(readTiff)
    if not args.nocache:
        stack.cache()

    #print("numPartitions(%d,%s): %d"%(stack.id(),stack.name(),stack.getNumPartitions()))
    #avg = stack.reduce(avg)
    #savebin(avg)

    # calculate threshold using partial stack
    num_threshold_slices=int(threshold_percent*slice_count)
    tmin_idx=(slice_count-num_threshold_slices)//2
    tmax_idx=slice_count-tmin_idx
    threshold_stack=stack.filter(lambda x: x[0][0]>=tmin_idx and x[0][0]<=tmax_idx)
    #print("partial stack size is %d"%threshold_stack.count())   #remember, .count() is expensive, so don't use it unnecessarily

    thresholds=sc.emptyRDD()
    if args.granular:
        thresholds=threshold_stack.map(crop).map(lambda x: smooth(x,gaussian_sigma)).map(thresh)
    else:
        thresholds=threshold_stack.map(lambda x: threshold(x,gaussian_sigma))

    #thresholds.foreach(savetxt)
    thresh=thresholds.reduce(avg)
    print("threshold is %f"%thresh[1])
    
    # apply threshold to entire stack
    thresholds=sc.emptyRDD()
    if args.granular:
        stack=stack.map(lambda x: smooth(x,gaussian_sigma)).map(lambda x: apply_threshold(x,thresh[1]))
    else:
        stack=stack.map(lambda x: smooth_and_apply_threshold(x,gaussian_sigma,thresh[1]))

    # save output slices
    outdir=args.dst+"/output-pct"+str(threshold_percent)+"-sigma"+str(gaussian_sigma)+"__thresh"+str(thresh[1])
    print("outdir is "+outdir)
    from os import makedirs
    try:
        makedirs(outdir)
    except Exception as e:
        print("exception: "+str(e))
        pass
    stack.foreach(lambda x: savebin(x,outdir))
    
    sc.stop()
