#!/bin/bash

#SBATCH -c 1 --mem 8g 
#SBATCH --mail-type end --mail-user robert.bjornson@yale.edu
#SBATCH -J Pacbio 

cd /ycga-gpfs/project/fas/lsprog/tools/ycga-utils/pacbio/RUNS/CRONJOBS
sudo bash /ycga-gpfs/project/fas/lsprog/tools/ycga-utils/pacbio/archive.sh

