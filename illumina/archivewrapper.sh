#!/bin/bash

#SBATCH -w r202u03n01 -p admintest -t 7-0 -n 1 -c 4 # 4 cpus to help with IO
#SBATCH HETJOB
#SBATCH -p ycga -t 7-0 -n 100 -c 1

python3 archive.py --maxthds=100 --maxsum=1500 -a 

# /gpfs/ycga/sequencers/illumina/sequencerC/runs/230111_A01519_0214_BHG3NWDSX5
# /gpfs/ycga/sequencers/illumina/sequencerB/runs/230501_M01156_0815_000000000-KVB6H
