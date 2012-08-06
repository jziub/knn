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
# File:  IDInputFormat.java
# Description:  
#
# -----------------------------------------------------------------
# 
*/

package edu.indiana.d2i.htrc.io.mem;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import net.spy.memcached.transcoders.Transcoder;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.mahout.math.VectorWritable;

import edu.indiana.d2i.htrc.HTRCConstants;
import edu.indiana.d2i.htrc.io.dataapi.IDInputSplit;
import edu.indiana.d2i.htrc.io.dataapi.IDList;

public class MemIDInputFormat extends FileInputFormat<Text, VectorWritable>  {
	private static final Log logger = LogFactory.getLog(MemIDInputFormat.class);
	
	class MemCachedRecordReader extends RecordReader<Text, VectorWritable> {

		private IDInputSplit split = null;
		private Configuration conf = null;
		
		private List<String> idList = null;
		private Iterator<String> iditerator = null;
		private int count = 0;
		
		private ThreadedMemcachedClient client = null;
		private Transcoder<VectorWritable> transcoder = null;
		
		private Text key = new Text();
		private VectorWritable val;
		
		@Override
		public void close() throws IOException {
			client.close();
		}

		@Override
		public Text getCurrentKey() throws IOException, InterruptedException {
			return key;
		}

		@Override
		public VectorWritable getCurrentValue() throws IOException, InterruptedException {
			return val;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return (float)count / idList.size();
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext taskAttemptContext)
				throws IOException, InterruptedException {
			this.split = (IDInputSplit)split;
			this.conf = taskAttemptContext.getConfiguration();
			this.idList = this.split.getIDList();
			this.iditerator = this.idList.iterator();
			
			Class<?> writableClass = VectorWritable.class;
			client = ThreadedMemcachedClient.getThreadedMemcachedClient(conf);
			transcoder = new HadoopWritableTranscoder<VectorWritable>(conf,
					writableClass);
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (!iditerator.hasNext()) return false;
			
			String id = iditerator.next();
			key.set(id);
			val = client.getCache().get(id, transcoder);
			while (val == null) {
				logger.error("ignore id " + id);
				if (iditerator.hasNext()) {
					id = iditerator.next();
					key.set(id);
					val = client.getCache().get(id, transcoder);
				} else {
					return false;
				}
			}
			
			count++;
			return true;
		}
	}
	
	@Override
	public RecordReader<Text, VectorWritable> createRecordReader(InputSplit arg0,
			TaskAttemptContext arg1) throws IOException, InterruptedException {
		return (RecordReader<Text, VectorWritable>) new MemCachedRecordReader();
	}

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		int numIdsInSplit = job.getConfiguration().getInt(HTRCConstants.MAX_IDNUM_SPLIT, 
				8000);
//		String[] hosts = job.getConfiguration().getStrings(HTRCConstants.MEMCACHED_HOSTS);
//		if (hosts == null)
//			throw new IllegalArgumentException("No host is found for memcached");
		
//		IDInputSplit split = new IDInputSplit(hosts);
		IDInputSplit split = new IDInputSplit();
		List<InputSplit> splits = new ArrayList<InputSplit>();
		Path[] dirs = getInputPaths(job);
		try {
			for (int i = 0; i < dirs.length; i++) {
				FileSystem fs = dirs[i].getFileSystem(job.getConfiguration());
				DataInputStream fsinput = new DataInputStream(fs.open(dirs[i]));
				Iterator<Text> idlist = new IDList(fsinput).iterator();
				while (idlist.hasNext()) {
					Text id = idlist.next();
					split.addID(id.toString());
					if (split.getLength() >= numIdsInSplit) {
						splits.add(split);
//						split = new IDInputSplit(hosts);
						split = new IDInputSplit();
					}
				}
			}
			if (split != null && split.getLength() != 0) splits.add(split);
		} catch (InterruptedException e) {
			logger.error(e);
		}
		
		logger.info("#Splits " + splits.size());
		return splits;
	}
}
