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
# File:  DataCopyJob.java
# Description:  
#
# -----------------------------------------------------------------
# 
*/

package edu.indiana.d2i.htrc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.indiana.d2i.htrc.io.IDInputFormat;

public class ParallelDataCopyJob extends Configured implements Tool {
	private static final Log logger = LogFactory.getLog(ParallelDataCopyJob.class);
	
	private void printUsage() {
		
	}
	
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			printUsage();
		}
		
		String inputPath = args[0];
		String outputPath = args[1];
		
		logger.info("ParallelDataCopyJob ");
		logger.info(" - input: " + inputPath);
		logger.info(" - output: " + outputPath);
		
		Job job = new Job(getConf(), "Copy data from HTRC data storage parallely.");
		job.setJarByClass(ParallelDataCopyJob.class);
		
		job.setInputFormatClass(IDInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(org.apache.hadoop.mapreduce.Mapper.class);
		job.setNumReduceTasks(0);
		
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		job.waitForCompletion(true);
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new ParallelDataCopyJob(), args);
		System.exit(res);
	}
}
