#!/bin/bash

tag=`date +%Y%m%d%H%M`
ofn="master.$(hostname).${tag}.out"

{

echo "Root Master cronjob on $(hostname)"
echo "Output: ${ofn}"
echo "Start $(date)"
echo "-----------------------------"

echo

DRYRUN=
WORKERS=10

echo
echo "Doing 10x"
/home/rdb9/.conda/envs/archiver/bin/python3 /gpfs/ycga/work/lsprog/tools/ycga-utils/illumina/ArchiveDirs.py --searchdir=/ycga-gpfs/sequencers/pacbio/gw92/10x/Single_Cell --trimdirs=4 --workers $WORKERS $DRYRUN

echo
echo "Doing pacbio"
/home/rdb9/.conda/envs/archiver/bin/python3 /gpfs/ycga/work/lsprog/tools/ycga-utils/illumina/ArchiveDirs.py --searchdir=/ycga-gpfs/sequencers/pacbio/data --trimdirs=2 --workers $WORKERS $DRYRUN

echo
echo "Doing Illumna clean"
/home/rdb9/.conda/envs/archiver/bin/python3 /gpfs/ycga/work/lsprog/tools/ycga-utils/illumina/NewClean.py --automatic $DRYRUN

echo
echo "Doing Illumina Archive"
/home/rdb9/.conda/envs/archiver/bin/python3 /gpfs/ycga/work/lsprog/tools/ycga-utils/illumina/NewArchive.py --automatic $DRYRUN --workers $WORKERS

echo
echo "Doing Illumina Delete"
/home/rdb9/.conda/envs/archiver/bin/python3 /gpfs/ycga/work/lsprog/tools/ycga-utils/illumina/NewDelete.py --automatic $DRYRUN

#( echo "\ndoing externaldatawrapper"; cd /ycga-gpfs/project/fas/lsprog/tools/ycga-utils/illumina/RUNS/CRONJOBS; /ycga-gpfs/project/fas/lsprog/tools/ycga-utils/illumina/externaldatawrapper.sh > /dev/null ) || exit 1

echo
echo "Done $(date)"

} > ${ofn} 2>&1

