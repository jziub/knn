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
# File:  MemCachedRecordWriter.java
# Description:  
#
# -----------------------------------------------------------------
# 
*/

package edu.indiana.d2i.htrc.io.mem;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import net.spy.memcached.transcoders.Transcoder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.mahout.math.VectorWritable;

import edu.indiana.d2i.htrc.HTRCConstants;

// should change hard code class
public class MemCachedRecordWriter extends RecordWriter<Text, VectorWritable> {

	private ThreadedMemcachedClient client = null;
	private Transcoder<VectorWritable> transcoder = null;
	private final int MAX_EXPIRE;
	
	public MemCachedRecordWriter(Configuration conf) {
		MAX_EXPIRE = conf.getInt(HTRCConstants.MEMCACHED_MAX_EXPIRE, 60);
		int numClients = conf.getInt(HTRCConstants.MEMCACHED_CLIENT_NUM, 1);
		String[] hostArray = conf.getStrings(HTRCConstants.MEMCACHED_HOSTS);
		List<String> hosts = Arrays.asList(hostArray);
		
		client = ThreadedMemcachedClient.getThreadedMemcachedClient(numClients, hosts);
		transcoder = new HadoopWritableTranscoder<VectorWritable>(
				conf, VectorWritable.class);
	}
	
	@Override
	public void close(TaskAttemptContext arg0) throws IOException,
			InterruptedException {
		// nothing
	}

	@Override
	public void write(Text key, VectorWritable val) throws IOException, InterruptedException {
		client.getCache().add(key.toString(), MAX_EXPIRE, val, transcoder);
	}

}
