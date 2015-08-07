"""
[A,A,A,A,A] -> B reduce microbenchmark.

Calculates average of a set of binary vectors.

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
    return ([x],arr,1) #id,arr,nblocks
    
def avg_vec3(a,b):
    print("a: "+str(a)+" b: "+str(b))
    avg = (a[2]*a[1]+b[2]*b[1])/(a[2]+b[2])
    ids=a[0]; ids.extend(b[0])
    return (ids,avg,a[2]+b[2])

def avg_vec3_arr(arr):
    print("arr: "+str(arr))
    nelems=len(arr[1])
    avg=np.array([0.0,0.0,0.0])
    for i in xrange(nelems):
        avg += arr[1][i]
    avg/=nelems
    return (arr[0],avg,nelems)

def avg_vec3_arrs(arr0,arr1):
    print("entering avg_vec3_arrs...")
    avg0=avg_vec3_arr(arr0)
    avg1=avg_vec3_arr(arr1)
    avg=avg_vec3(avg0,avg1)
    print("avg_vec3_arrs: idx="+str(ids)+" avg="+str(avg))
    return avg

def saveavg(arr,dst):
    print("saveavg: arr="+str(arr))
    from glob import glob
    name=dst+'/simple_reduce_average-'
    idx=len(glob(name+'*.txt'))
    outfilename=name+str(idx).zfill(2)+".txt"
    outfile=open(outfilename,'w')
    outfile.write("average: "+str(arr[1])+"\n")
    outfile.write("nvecs: "+str(arr[2])+"\n")
    outfile.write("partition_ids: "+str(arr[0])+"\n")

def noop(x):
    pass #print("noop")

if __name__ == "__main__":

    import argparse
    parser = argparse.ArgumentParser(description="Simple Map Microbenchmark")
    parser.add_argument("-d","--dst",help="directory to write output files")
    parser.add_argument("-g","--generate",type=int,required=True,nargs=2,default=[0,0],help="generate <m> blocks of size <k> (in MB) data instead of loading from files")
    parser.add_argument("-n","--nodes",type=int,required=True,help="number of nodes")
    parser.add_argument("-p","--nparts",type=int,default=1,help="how many partitions to create per node")
    parser.add_argument("-f","--fast",default=False,action="store_true",help="calculate averages per-partition locally before reducing")
    args = parser.parse_args()

    sc = SparkContext(appName="SimpleReduce")

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
    gen_num_blocks,gen_block_size=args.generate
    if not gen_num_blocks>0 and gen_block_size>0:
        print("arguments to --generate must be positive")
        sc.stop();
        from sys import exit
        exit(-1)

    name=args.dst+'/simple_reduce-n'+str(args.nodes*12)+'-'+str(args.generate[0])+'-'+str(args.generate[1])+'-'
    idx=len(glob(name+'*.txt'))
    outfilename=name+str(idx).zfill(2)+".txt"
    outfile=open(outfilename,'w')
    outfile.write("cores: "+str(args.nodes*12)+"\n")
    outfile.write("data size: "+str(gen_num_blocks*gen_block_size)+"\n")

    # generate input data
    import time
    t0=tbegin=time.time()

    rdd=sc.parallelize(range(gen_num_blocks),args.nodes*12*args.nparts)
    gen_block_vec_count=gen_block_size*1E6/24  # 24 bytes per vector
    print("generating %d blocks of %d vectors each..."%(gen_num_blocks,gen_block_vec_count))
    outfile.write("generating data...\n")
    outfile.write("partition_multiplier: "+str(args.nparts)+"\n")
    outfile.write("gen_num_blocks: "+str(gen_num_blocks)+"\n")
    outfile.write("gen_block_size: "+str(gen_block_size)+"\n")
    outfile.write("total_data_size: "+str(gen_num_blocks*gen_block_size)+"\n")
    A=rdd.map(lambda x:generate(x,gen_block_vec_count))

    #rdd.foreach(noop)  #useful to force pipeline to execute for debugging
    tmark=time.time()
    outfile.write("generate partitions: %0.6f\n"%(tmark-t0))
    outfile.write("numPartitions(%d,%s): %d\n"%(A.id(),A.name(),A.getNumPartitions()))
    t0=tmark

    # calculate average
    avg=0.0
    if args.fast:
        avg=A.map(avg_vec3_arr).reduce(avg_vec3)
    else:
        avg=A.reduce(avg_vec3_arrs)

    #B.foreach(noop)  #useful to force pipeline to execute for debugging
    tmark=time.time()
    outfile.write("simple reduce (avg): %0.6f\n"%(tmark-t0))
    t0=tmark

    # write results
    saveavg(avg,outdir)
    print("average: "+str(avg[1]))
    print("nvecs: "+str(avg[2]))
    print("partition_ids: "+str(avg[0]))

    #B.foreach(noop)  #useful to force pipeline to execute for debugging
    tmark=time.time()
    outfile.write("write results: %0.6f\n"%(tmark-t0))
    t0=tmark

    outfile.close()
    sc.stop()
