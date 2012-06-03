#!/bin/bash

MAHOUT="/home/workplace/hadoop_work/mahout-distribution-0.6/bin/mahout"
  
$MAHOUT seq2sparse \
    --analyzerName edu.indiana.d2i.htrc.util.filter.HTRCFilterAnalyzer \
    -i $1 \
    -o $2 --maxDFPercent 85 --namedVector \

