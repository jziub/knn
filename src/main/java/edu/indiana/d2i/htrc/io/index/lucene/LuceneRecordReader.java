/*
#
# Copyright 2012 The Trustees of Indiana University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# -----------------------------------------------------------------
#
# Project: knn
# File:  LuceneRecordReader.java
# Description:  
#
# -----------------------------------------------------------------
# 
*/

package edu.indiana.d2i.htrc.io.index.lucene;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import edu.indiana.d2i.htrc.io.dataapi.IDInputSplit;

public class LuceneRecordReader extends RecordReader<Text, VectorWritable> {

	private static final Log logger = LogFactory.getLog(LuceneRecordReader.class); 
	
	private IDInputSplit split = null;
	private Iterator<String> iditerator = null;
	private Configuration conf = null;
	private LuceneClient client = null;
	
	private int count = 0;
	
	private Text key = new Text();
	private VectorWritable value = new VectorWritable();
	
	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		split = (IDInputSplit) inputSplit;
		iditerator = split.getIDIterator();
		conf = context.getConfiguration();
		client = LuceneClient.createLuceneClient(conf);
	}
	
	@Override
	public void close() throws IOException {
		client.close();
		logger.info("!!!!! cpu time " + client.getCPUTime());
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public VectorWritable getCurrentValue() throws IOException,
			InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return (float) count / split.getLength();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (iditerator.hasNext()) {
			String id = iditerator.next();
			key.set(id);
			
			Vector vector = client.getTFVector(id);
			value.set(vector);
			
			count++;
			return true;
		} else {
			return false;
		}
	}
}
