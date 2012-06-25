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

package edu.indiana.d2i.htrc.io.index.solr;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import edu.indiana.d2i.htrc.io.dataapi.IDInputSplit;

public class SolrRecordReader extends RecordReader<Text, VectorWritable> {

	private static final Log logger = LogFactory.getLog(SolrRecordReader.class); 
	
	private IDInputSplit split = null;
	private Iterator<String> iditerator = null;
	private Configuration conf = null;
	private SolrClient client = null;
	
	private int count = 0;
	
	private Text key = new Text();
	private VectorWritable value = new VectorWritable();
	
	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		split = (IDInputSplit) inputSplit;
		iditerator = split.getIDIterator();
		conf = context.getConfiguration();
		client = new SolrClient(conf, true);
	}
	
	@Override
	public void close() throws IOException {
//		client.close();
//		logger.info("!!!!! cpu time " + client.getCPUTime());
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
			// normal route
//			String id = iditerator.next();
//			key.set(id);
//			
//			String[] idlst = new String[1];
//			idlst[0] = id;
//			Iterable<NamedVector> tfs = client.getTermVectors(idlst);
//			value.set(tfs.iterator().next()); // only one vector
//			
//			count++;
//			return true;
			
			// handle Solr exception
			do {
				String id = iditerator.next();
				key.set(id);
				
				String[] idlst = new String[1];
				idlst[0] = id;
				Iterable<NamedVector> tfs = client.getTermVectors(idlst);
				if (tfs != null) { // otherwise, skip this book id
					value.set(tfs.iterator().next()); // only one vector
					
//					logger.info("set value for " + ((NamedVector)value.get()).getName());
					count++;
					return true;
				}				
			} while (iditerator.hasNext());
			return false;
		} else {
			return false;
		}
	}
}
