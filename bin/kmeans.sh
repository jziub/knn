#!/bin/bash

MAHOUT="/root/mahout-distribution-0.6/bin/mahout"
INPUT="/user/root/lucene-vector/tf-vectors"
OUTPUT="/user/root/lucene-out"
CLUSTER="${OUTPUT}/cluster"

#kmeans paramters
NUM_ITER=50
NUM_CLUSTER=50

$MAHOUT kmeans \
    -Dmapred.child.java.opts=-Xmx2048M \
    -i ${INPUT} \
    -c ${CLUSTER} \
    -o ${OUTPUT} \
    -dm org.apache.mahout.common.distance.CosineDistanceMeasure \
    -x ${NUM_ITER} -k ${NUM_CLUSTER} -cd 0.5 -ow --clustering \
