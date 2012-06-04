#!/bin/bash

HADOOP_HOME="/home/workplace/hadoop_work/hadoop-1.0.1"
EXE="knn-0.1-jar-with-dependencies.jar"

#skmeans paramters
SKMEANS_CLS="edu.indiana.d2i.htrc.skmeans.StreamingKMeansDriver"
INPUT=""
OUTPUT=""
CLUSTER=${OUTPUT}/""

NUM_CLUSTER=10

${HADOOP_HOME}/bin/hadoop jar 
    ${EXE} \
    ${SKMEANS_CLS} \
    ${INPUT} \
    ${OUTPUT} \
    ${NUM_CLUSTER} \


