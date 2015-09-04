"""
A -> B map microbenchmark.

Reads FILESIZE/24 double vectors per record. This is more memory efficient than reading one vector per record.

Requires NumPy (http://www.numpy.org/).
"""

from __future__ import print_function
import numpy as np
from pyspark import SparkContext

def generate(x,block_count):
    from time import time
    seed=int(time()/(x+1))
    np.random.seed(seed)
    print("(%d) generating %d vectors..."%(x,block_count))
    a,b=-1000,1000
    arr=(b-a)*np.random.random_sample((block_count,3))+a
    return (x,arr)
    
def parseVectors(bin):
    arr= np.fromstring(bin[1],dtype=np.float64)
    arr=arr.reshape(arr.shape[0]/3,3)
    return (bin[0],arr)

def add_vec3(arr,vec):
    for i in xrange(len(arr[1])):
        arr[1][i] += vec
    return arr

def avg_vec3_arr(arr):
    avg=np.array([0.0,0.0,0.0])
    for i in xrange(len(arr[1])):
        avg += arr[1][i]
    avg/=len(arr[1])
    return (arr[0],avg)

def savebin(arr,dst):
    import re
    # expects input names of the form <name>X-##?.bin (ex: imageA-3.bin, imageK-97.bin)
    idx=re.match(".*(.-..?)\.bin",arr[0]).group(1) # terribly specific
    outfilename=dst+"/output-"+str(idx)+".bin"
    outfile=open(outfilename,'w')
    outfile.write(arr[1].data)
    #outfile.write(str(arr[1].shape)+"\n")

def saveavg(arr,dst):
    import re
    # expects input names of the form <name>X-##?.bin (ex: imageA-3.bin, imageK-97.bin)
    #idx=re.match(".*(.-..?)\.bin",arr[0]).group(1) # terribly specific
    idx=arr[0] #generated data just has the index
    outfilename=dst+"/average-"+str(idx)+".txt"
    outfile=open(outfilename,'w')
    outfile.write(str(arr[1]))

def noop(x):
    pass #print("noop")

if __name__ == "__main__":

    import argparse
    parser = argparse.ArgumentParser(description="Simple Map Microbenchmark")
    parser.add_argument("-s","--src",help="directory containing input files")
    parser.add_argument("-d","--dst",help="directory to write output files")
    parser.add_argument("-g","--generate",type=int,nargs=2,default=[0,0],help="generate <m> blocks of size <k> (in MB) data instead of loading from files")
    parser.add_argument("-n","--nodes",type=int,required=True,help="number of nodes (for reporting)")
    parser.add_argument("-p","--nparts",type=int,default=1,help="how many partitions to create per node")
    parser.add_argument("-z","--size",type=int,required=True,help="input size (in mb, for reporting)")
    args = parser.parse_args()

    sc = SparkContext(appName="SimpleMap")

    # write results
    outdir=args.dst+"/results"
    print("outdir is "+outdir)
    from os import makedirs
    try:
        makedirs(outdir)
    except Exception as e:
        #print("exception: "+str(e))   #it's okay if directory already exists...
        pass

    from glob import glob
    name=args.dst+'/simple_map-n'+str(args.nodes*12)+'-'+str(args.size)+'mb-'
    idx=len(glob(name+'*.txt'))
    outfilename=name+str(idx).zfill(2)+".txt"
    outfile=open(outfilename,'w')
    outfile.write("cores: "+str(args.nodes*12)+"\n")
    outfile.write("data size: "+str(args.size)+"\n")
    gen_num_blocks,gen_block_size=args.generate

    # read input files or generate input data
    import time
    t0=tbegin=time.time()

    if gen_num_blocks>0 and gen_block_size>0:
        rdd=sc.parallelize(range(gen_num_blocks),args.nodes*12*args.nparts)
        gen_block_count=gen_block_size*1E6/24  # 24 bytes per vector
        print("generating %d blocks of %d vectors each..."%(gen_num_blocks,gen_block_count))
        outfile.write("generating data...\n")
        outfile.write("partition_multiplier: "+str(args.nparts)+"\n")
        outfile.write("gen_num_blocks: "+str(gen_num_blocks)+"\n")
        outfile.write("gen_block_size: "+str(gen_block_size)+"\n")
        outfile.write("total_data_size: "+str(gen_num_blocks*gen_block_size)+"\n")
        A=rdd.map(lambda x:generate(x,gen_block_count))
    elif args.src:
        outfile.write("reading data...\n")
        outfile.write(args.src+"\n")
        rdd = sc.binaryFiles(args.src)
        A = rdd.map(parseVectors)
    else:
        print("either --src or --generate must be specified")
        sc.stop();
        from sys import exit
        exit(-1)

    #rdd.foreach(noop)  #useful to force pipeline to execute for debugging
    tmark=time.time()
    outfile.write("read/parse or generate partitions: %0.6f\n"%(tmark-t0))
    outfile.write("numPartitions(%d,%s): %d\n"%(A.id(),A.name(),A.getNumPartitions()))
    t0=tmark

    # apply simple operation (V'=V+V0)
    shift=np.array([25.25,-12.125,6.333],dtype=np.float64)
    B = A.map(lambda x: add_vec3(x,shift))
    #outfile.write("numPartitions(%d,%s): %d"%(B.id(),B.name(),B.getNumPartitions()))

    #B.foreach(noop)  #useful to force pipeline to execute for debugging
    tmark=time.time()
    outfile.write("simple map (V'=V0+V): %0.6f\n"%(tmark-t0))
    t0=tmark

    # write results
    #B.foreach(lambda x: savebin(x,outdir))

    B_avg=B.map(avg_vec3_arr)

    #B.foreach(noop)  #useful to force pipeline to execute for debugging
    tmark=time.time()
    outfile.write("calculate per-partition average: %0.6f\n"%(tmark-t0))
    t0=tmark

    B_avg.foreach(lambda x: saveavg(x,outdir))

    #B.foreach(noop)  #useful to force pipeline to execute for debugging
    tmark=time.time()
    outfile.write("write results: %0.6f\n"%(tmark-t0))
    t0=tmark

    # used with .cache() above to examine RDD memory usage
    # import time
    # while True:
    #     time.sleep(5)

    outfile.close()
    sc.stop()
