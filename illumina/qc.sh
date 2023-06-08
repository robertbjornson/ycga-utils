#!/bin/bash

#SBATCH -c 20
module purge
module load bcl2fastq2

bcl2fastq -R ../../../
--output-dir QC -p $SLURM_CPUS_PER_TASK --sample-sheet emptySS.csv \
--tiles 120[0-9] --create-fastq-for-index-reads \
> QC.log 2> QC.err 
