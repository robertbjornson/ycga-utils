#!/bin/bash

#SBATCH --mail-type end --mail-user robert.bjornson@yale.edu

echo "disabled deleteino"
cd /ycga-gpfs/project/fas/lsprog/tools/ycga-utils/pacbio/RUNS

python3 /ycga-gpfs/project/fas/lsprog/tools/ycga-utils/pacbio/arch_del.py --nodel --descend -l arch_del_Single_Cell --fastq /gpfs/ycga/sequencers/pacbio/gw92/10x/Single_Cell /SAY/archive/YCGA-729009-YCGA-A2/archive/pacbio/gw92/10x/Single_Cell
python3 /ycga-gpfs/project/fas/lsprog/tools/ycga-utils/pacbio/arch_del.py --nodel --descend -l arch_del_Genome --fastq /gpfs/ycga/sequencers/pacbio/gw92/10x/Genome /SAY/archive/YCGA-729009-YCGA-A2/archive/pacbio/gw92/10x/Genome
python3 /ycga-gpfs/project/fas/lsprog/tools/ycga-utils/pacbio/arch_del.py --nodel --descend -l arch_del_Exome --fastq /gpfs/ycga/sequencers/pacbio/gw92/10x/Exome /SAY/archive/YCGA-729009-YCGA-A2/archive/pacbio/gw92/10x/Exome
python3 /ycga-gpfs/project/fas/lsprog/tools/ycga-utils/pacbio/arch_del.py --nodel --descend -l arch_del_data /gpfs/ycga/sequencers/pacbio/data /SAY/archive/YCGA-729009-YCGA-A2/archive/pacbio/data
python3 /ycga-gpfs/project/fas/lsprog/tools/ycga-utils/pacbio/arch_del.py --nodel -l arch_del_merfish /gpfs/gibbs/pi/ycga/mane/merfish /SAY/archive/YCGA-729009-YCGA-A2/archive/merfish
python3 /ycga-gpfs/project/fas/lsprog/tools/ycga-utils/pacbio/arch_del.py --nodel --descend -l arch_del_Guilin_indexing_Feb2023 --fastq /gpfs/ycga/sequencers/pacbio/gw92/10x/Guilin_indexing_Feb2023 /SAY/archive/YCGA-729009-YCGA-A2/archive/pacbio/gw92/10x/Guilin_indexing_Feb2023

