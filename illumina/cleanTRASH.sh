
for d in /gpfs/ycga/sequencers/panfs/TRASH/sequencers*/sequencer?/runs/[0-9][0-9]* \
         /gpfs/ycga/sequencers/illumina/TRASH/sequencer?/runs/[0-9][0-9]*
do
   echo $(date): Deleting $d >> cleanTRASH.log
done
