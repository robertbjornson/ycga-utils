#!/bin/bash

#SBATCH --mail-type end --mail-user robert.bjornson@yale.edu

cd /ycga-gpfs/project/fas/lsprog/tools/ycga-utils/pacbio/RUNS

python /ycga-gpfs/project/fas/lsprog/tools/ycga-utils/pacbio/arch_del.py -l arch_del_Single_Cell --fastq /gpfs/ycga/sequencers/pacbio/gw92/10x/Single_Cell /SAY/archive/YCGA-729009-YCGA/archive/pacbio/gw92/10x/Single_Cell
python /ycga-gpfs/project/fas/lsprog/tools/ycga-utils/pacbio/arch_del.py -l arch_del_Genome --fastq /gpfs/ycga/sequencers/pacbio/gw92/10x/Genome /SAY/archive/YCGA-729009-YCGA/archive/pacbio/gw92/10x/Genome
python /ycga-gpfs/project/fas/lsprog/tools/ycga-utils/pacbio/arch_del.py -l arch_del_Exome --fastq /gpfs/ycga/sequencers/pacbio/gw92/10x/Exome /SAY/archive/YCGA-729009-YCGA/archive/pacbio/gw92/10x/Exome
#python /ycga-gpfs/project/fas/lsprog/tools/ycga-utils/pacbio/arch_del.py -l arch_del_data /gpfs/ycga/sequencers/pacbio/data /SAY/archive/YCGA-729009-YCGA/archive/pacbio/data
