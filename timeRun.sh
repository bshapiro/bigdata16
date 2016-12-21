#!/bin/bash

num_cpu=$1
num_node=$2
mem=$3

> runTimes.txt
for i in `seq 1 5`; do
    { time spark-submit --packages graphframes:graphframes:0.2.0-spark1.5-s_2.10 --executor-memory $mem fullSparkSubmitRun.py -f /home/Rachel/ERR188044_chrX_sorted.bam -o /output/$num_cpu'CPU_'$num_node'Node_'$i/ -k $num_cpu ; } 2>> runTimes.txt
done

echo ${grep 'real' runTimes.txt | cut -f 2 | sed -e 's/[ms]/ /g' | awk '{print $1+$2/60}'}
