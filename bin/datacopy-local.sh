#!/bin/bash

# run time
EXE="/home/workplace/hadoop_work/ted_knn/target/knn-0.1-jar-with-dependencies.jar"
DATA_COPY_CLS="edu.indiana.d2i.htrc.io.DataCopyTokenizerJob"
DATA_PRRPROC_CLS="edu.indiana.d2i.htrc.io.SparseVectorsFromTokenizedDoc"

HADOOP_HOME="/home/workplace/hadoop_work/hadoop-1.0.1"

# data copy parameters
ID_LIST="IUTest.txt"
TOKENS_OUTPUT="IUTest-tokens"
MAX_IDS_SPLIT="2"
DATAAPI_CONF="edu.indiana.d2i.htrc.io.DataAPIDefaultConf"
ANALYZER_NAME="edu.indiana.d2i.htrc.util.filter.HTRCFilterAnalyzer"

${HADOOP_HOME}/bin/hadoop jar ${EXE} ${DATA_COPY_CLS} ${ID_LIST} ${TOKENS_OUTPUT} ${MAX_IDS_SPLIT} ${DATAAPI_CONF} ${ANALYZER_NAME}


# vectorization parameters
OUTPUT="IUTest-3"

${HADOOP_HOME}/bin/hadoop jar ${EXE} ${DATA_PRRPROC_CLS} -i ${TOKENS_OUTPUT} -o ${OUTPUT} --maxDFPercent 85 --namedVector

