#!/bin/bash

data="/path/to/data"
results="/path/to/results"
conifer="/path/to/conifer"

start=$SECONDS
echo "Starting step 1"

for file in $data/*.bam; do
    echo "Processing file: $file"
    python $conifer/conifer.py rpkm --probes $data/probes.txt --input $file --output $results/RPKM/${file##*/}.rpkm.txt
done

echo "Finishing step 1"
step1=$SECONDS
echo "Starting step 2"

python $conifer/conifer.py analyze --probes $data/probes.txt --rpkm_dir $results/RPKM --output $results/analysis.hdf5 \
--svd 12 --min_rpkm 1.00

echo "Finishing step 2"
step2=$SECONDS
echo "Starting step 3"

python $conifer/conifer.py call --input $results/analysis.hdf5 --output $results/calls.txt --threshold 1.50

echo "Finishing step 3"
step3=$SECONDS

echo "Execution times:
RPKM: $(($step1 - $start)) sec
Analyze: $(($step2 - $step1)) sec
Call: $(($step3 - $step2)) sec
" > $results/times.txt
