#!/bin/bash

# run time
TOKEN2VECTOR="edu.indiana.d2i.htrc.io.SparseVectorsFromTokenizedDoc"

EXE="knn-0.1-jar-with-dependencies.jar"
HADOOP_HOME="/home/workplace/hadoop_work/hadoop-1.0.1"

# 
INPUT=""
OUTPUT=""

${HADOOP_HOME}/bin/hadoop jar ${EXE} ${TOKEN2VECTOR} \
    -i ${INPUT} \
    -o ${OUTPUT} --maxDFPercent 85 --namedVector \
