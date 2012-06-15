#!/bin/bash

MAHOUT="/root/mahout-distribution-0.6/bin/mahout"
CLUSTERS="/user/root/lucene-out/clusters-3-final"
DICTIONARY="/user/root/dictionary-seq.txt"
POINTS="/user/root/lucene-out/clusteredPoints"

#kmeans paramters
NUM_ITER=50
NUM_CLUSTER=100

$MAHOUT clusterdump \
    -s ${CLUSTERS} \
    -d ${DICTIONARY} \
    -dt sequencefile -b 10 -n 20 --evaluate -dm org.apache.mahout.common.distance.CosineDistanceMeasure \
    -o cluster3.txt 
