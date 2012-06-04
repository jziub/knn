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
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

/**
 * It is used to inspect the vectors after transformation of text or cluster result 
 */
public class ClusterInspection extends Configured implements Tool {
	private static final Log logger = LogFactory.getLog(ClusterInspection.class);
	
	@Override
	public int run(String[] args) throws Exception {
		String input = args[0]; // cluster path
		String output = args[1];
		
		int numVector = 0;
		
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] status = fs.listStatus(new Path(input),
				Utilities.HIDDEN_FILE_FILTER);
		Text key = new Text();
		LongWritable value = new LongWritable();
		BufferedWriter writer = new BufferedWriter(new FileWriter(output));
		for (int i = 0; i < status.length; i++) {
			SequenceFile.Reader seqReader = new SequenceFile.Reader(fs,
					status[i].getPath(), conf);
			while (seqReader.next(key, value)) {
				numVector++;
				writer.write(key.toString() + "\n");
			}
		}
		writer.close();
		
		logger.info("#vector: " + numVector);
		
		return 0;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new ClusterInspection(), args);
		System.exit(0);
	}
}
