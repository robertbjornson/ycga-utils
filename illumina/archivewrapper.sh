#!/bin/bash

#SBATCH -c 20 --mem 120g 
#SBATCH --mail-type all --mail-user robert.bjornson@yale.edu
#SBATCH -J Archive

cd /ycga-gpfs/project/fas/lsprog/tools/ycga-utils/illumina/RUNS/CRONJOBS
sudo python /ycga-gpfs/project/fas/lsprog/tools/ycga-utils/illumina/archive.py --automatic -l PROD

