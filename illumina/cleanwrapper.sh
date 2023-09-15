#!/bin/bash

#SBATCH -c 20 
#SBATCH --mail-type end --mail-user robert.bjornson@yale.edu
#SBATCH -J Clean 

cd /ycga-gpfs/project/fas/lsprog/tools/ycga-utils/illumina/RUNS/CRONJOBS
sudo python3 /ycga-gpfs/project/fas/lsprog/tools/ycga-utils/illumina/clean.py --automatic 
sudo python3 /ycga-gpfs/project/fas/lsprog/tools/ycga-utils/illumina/clean.py  --cutoff=-300 --nocheckUnaligned --automatic -l CLEAN_NOCHECK

