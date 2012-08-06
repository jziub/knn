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
# File:  VectorInspection.java
# Description:  
#
# -----------------------------------------------------------------
# 
*/

package edu.indiana.d2i.htrc.util;

import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import net.spy.memcached.MemcachedClient;
import net.spy.memcached.transcoders.Transcoder;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.StringTuple;
import org.apache.mahout.math.VectorWritable;

import edu.indiana.d2i.htrc.io.dataapi.IDList;
import edu.indiana.d2i.htrc.io.mem.HadoopWritableTranscoder;
import edu.indiana.d2i.htrc.io.mem.MemCachedUtil;
import edu.indiana.d2i.htrc.io.mem.ThreadedMemcachedClient;

/**
 * It is used to inspect the vectors after transformation of text or cluster result 
 */
public class MemcachedValidation extends Configured implements Tool {
	private static final Log logger = LogFactory.getLog(MemcachedValidation.class);
	
	@Override
	public int run(String[] args) throws Exception {
		String idDir = args[0];
		String memhostsPath = args[1];
		
		Configuration conf = getConf();
		MemCachedUtil.configHelper(conf, memhostsPath);
		ThreadedMemcachedClient client = ThreadedMemcachedClient
				.getThreadedMemcachedClient(conf);
		MemcachedClient cache = client.getCache();
		Transcoder<VectorWritable> vectorTranscoder = new HadoopWritableTranscoder<VectorWritable>(
				conf, VectorWritable.class);
		
		// id list
		FileSystem fs = FileSystem.get(conf);
		DataInputStream fsinput = new DataInputStream(fs.open(new Path(idDir)));
		Iterator<Text> idIterator = new IDList(fsinput).iterator();
		List<String> idlist = new ArrayList<String>();
		while (idIterator.hasNext()) {
			Text id = idIterator.next();
			idlist.add(id.toString());
		}
		
		BufferedWriter writer = new BufferedWriter(new FileWriter("memdebug.txt"));
		String namespace = "";
		for (String id : idlist) {
			VectorWritable vec = cache.get(namespace + id, vectorTranscoder);
			if (vec == null) {
				System.out.println(id);
				writer.write(id + "\n");
			}
		}
		writer.close();		
		
		return 0;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new MemcachedValidation(), args);
		System.exit(0);
	}
}
