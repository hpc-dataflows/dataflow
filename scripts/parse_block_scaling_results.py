

from __future__ import print_function
import numpy as np


if __name__ == "__main__":

    import argparse
    parser = argparse.ArgumentParser(description="Collect experiment results")
    parser.add_argument("-s","--src",required=True,help="directory containing input files")
    parser.add_argument("-d","--dst",required=True,help="name of output file")
    args = parser.parse_args()

    outfile=open(args.dst,'w')
    outfile.write("cores,datasize,partitions,nblocks,blocksize,time\n")

    import fnmatch,os
    files=[]
    for root, dirnames, filenames in os.walk(args.src):
        for filename in fnmatch.filter(filenames, 'simple_map*.txt'):
            files.append(os.path.join(root, filename))

    for name in files:
        f=open(name,'r')
        cores=int(f.readline().split(' ')[1]) #cores: 24
        size=int(f.readline().split(' ')[2]) #data size: 131072
        f.readline()  #generating data...
        partitions=cores*int(f.readline().split(' ')[1]) #partition_multiplier: 4
        nblocks=int(f.readline().split(' ')[1]) #gen_num_blocks: 4096
        blocksz=int(f.readline().split(' ')[1]) #gen_block_size: 32
        assert(size==int(f.readline().split(' ')[1])) #total_data_size: 131072
        t0=float(f.readline().split(' ')[4]) #read/parse or generate partitions: 0.1741
        t1=float(f.readline().split(' ')[3]) #simple map (V'=V0+V): 0.000038
        t2=float(f.readline().split(' ')[3]) #calculate per-partition average: 0.000007
        t3=float(f.readline().split(' ')[2]) #write results: 519.920516
        time=t0+t1+t2+t3
        outfile.write(str(cores)+","+str(size)+","+str(partitions)+","+str(nblocks)+","+str(blocksz)+","+str(time)+"\n")

    outfile.close()
