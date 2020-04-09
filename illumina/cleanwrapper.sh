#!/bin/bash

#SBATCH -c 20 --mem 120g 
#SBATCH --mail-type end --mail-user robert.bjornson@yale.edu
#SBATCH -J Clean 

cd /ycga-gpfs/project/fas/lsprog/tools/ycga-utils/illumina/RUNS/CRONJOBS
sudo python /ycga-gpfs/project/fas/lsprog/tools/ycga-utils/illumina/clean.py --automatic 
sudo python /ycga-gpfs/project/fas/lsprog/tools/ycga-utils/illumina/clean.py  --cutoff=-300 --nocheckUnaligned --automatic -l DELETE_NOCHECK

