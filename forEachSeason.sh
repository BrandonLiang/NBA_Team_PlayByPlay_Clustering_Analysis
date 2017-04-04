#!/bin/bash -e

# Dataset may contain header
file=$1
head -1 $file > header.bsv

for i in `seq 4 9`; do
  cat header.bsv > NN_Season_0${i}.bsv
  sed '1d' $file | grep "^0020${i}" >> NN_Season_0${i}.bsv
  ./split NN_Season_0${i}.bsv 4 5 NN_Season_0${i}
done

for i in `seq 10 15`; do
  cat header.bsv > NN_Season_${i}.bsv
  sed '1d' $file | grep "^002${i}" >> NN_Season_${i}.bsv
  ./split NN_Season_${i}.bsv 4 5 NN_Season_${i}
done
