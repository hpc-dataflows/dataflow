from __future__ import print_function
import numpy as np


if __name__ == "__main__":

    import argparse
    parser = argparse.ArgumentParser(description="Collect experiment results")
    parser.add_argument("-s","--src",required=True,help="directory containing input files")
    parser.add_argument("-d","--dst",required=True,help="name of output file")
    args = parser.parse_args()

    outfile=open(args.dst,'w')
    outfile.write("cores,size,time\n")

    import fnmatch,os
    files=[]
    for root, dirnames, filenames in os.walk(args.src):
        for filename in fnmatch.filter(filenames, '*.txt'):
            files.append(os.path.join(root, filename))

    for name in files:
        f=open(name,'r')
        cores=int(f.readline())
        size=int(f.readline())
        f.readline()
        t0=float(f.readline().split(' ')[2]) #read files: 0.394027
        t1=float(f.readline().split(' ')[3]) #parse numpy arrays: 0.000043
        t2=float(f.readline().split(' ')[3]) #simple map (V'=V0+V): 0.000041
        t3=float(f.readline().split(' ')[3]) #calculate per-partition average: 0.000007
        t4=float(f.readline().split(' ')[2]) #write results: 59.341426
        time=t0+t1+t2+t3+t4
        outfile.write(str(cores)+","+str(size)+","+str(time)+"\n")

    outfile.close()
