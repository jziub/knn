#!/bin/bash

MAHOUT="/home/workplace/hadoop_work/mahout-distribution-0.6/bin/mahout"
INPUT=""
OUTPUT=""
CLUSTER=${OUTPUT}/""

#kmeans paramters
NUM_ITER=10
NUM_CLUSTER=10

$MAHOUT kmeans \
    -i ${INPUT} \
    -c ${CLUSTER} \
    -o ${OUTPUT} \
    -dm org.apache.mahout.common.distance.CosineDistanceMeasure \
    -x ${NUM_ITER} -k ${NUM_CLUSTER} -ow --clustering \
