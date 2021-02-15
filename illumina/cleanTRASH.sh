#!/bin/bash

set -e

for d in $(ls -1d /gpfs/ycga/sequencers/illumina/TRASH/sequencer?*/runs/[12]*) 
do 
  echo "considering $d"

  arch=${d/\/TRASH/}
  #arch=${arch/\/gpfs\/ycga\//\/SAY\/archive\/YCGA-729009-YCGA\/archive\/ycga-gpfs/}
  arch=${arch/\/gpfs\/ycga\//\/SAY\/archive\/YCGA-729009-YCGA-A2\/archive\/ycga-gpfs/}

  fin="$arch/$(basename $arch)_finished.txt"
  echo $fin
  [ ! -f $fin ] && (echo "$fin does not exist!"; exit 1)

  tf="$arch/$(basename $arch)_0.tar"
  echo $tf
  #[ ! -f $tf ] && (echo "$tf does not exist!"; exit 1)

  [[ $(find $tf -type f -size +10000c 2>/dev/null) ]] ||  (echo "$tf does not exist or too small!"; exit 1)

  if [ "$1" = "--delete" ]
  then
    echo "deleting $d"
    rm -rf $d
  fi
done
