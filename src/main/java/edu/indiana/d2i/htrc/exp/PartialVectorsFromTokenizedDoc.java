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

package edu.indiana.d2i.htrc.exp;

import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.StringTuple;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.vectorizer.common.PartialVectorMerger;
import org.apache.mahout.vectorizer.term.TFPartialVectorReducer;

public class PartialVectorsFromTokenizedDoc extends Configured implements Tool {

	private static final Log logger = LogFactory
			.getLog(PartialVectorsFromTokenizedDoc.class);

	private String tokenizedDocDir;
	private String dictDir;
	private String outputDir;
	private int numReducers;

	private void printUsage() {
		System.out.println("Bad input arguments!");
		System.exit(1);
	}

	// add filter !!??
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 4) {
			printUsage();
		}

		// all directories are in HDFS
		tokenizedDocDir = args[0];
		dictDir = args[1];
		outputDir = args[2];
		numReducers = Integer.valueOf(args[3]);

		logger.info("PartialVectorsFromTokenizedDoc ");
		logger.info(" - tokenizedDocDir: " + tokenizedDocDir);
		logger.info(" - dictDir: " + dictDir);
		logger.info(" - outputDir: " + outputDir);
		logger.info(" - numReducers: " + numReducers);
		
		Path tokenizedDocPath = new Path(tokenizedDocDir);
		Path dictPath = new Path(dictDir);
		Path outputPath = new Path(outputDir);

		// get dimension
		Configuration conf = getConf();
		
		int dimension = 0;
		for (Pair<Writable, IntWritable> record : new SequenceFileIterable<Writable, IntWritable>(
				dictPath, true, conf)) {
			dimension++;
		}
		logger.info("dimension of a vector: " + dimension);

		// submit job
		long t0 = System.currentTimeMillis();
		conf.set(
				"io.serializations",
				"org.apache.hadoop.io.serializer.JavaSerialization,"
						+ "org.apache.hadoop.io.serializer.WritableSerialization");
		conf.setInt(PartialVectorMerger.DIMENSION, dimension);
		DistributedCache.setCacheFiles(new URI[] { dictPath.toUri() }, conf);

		Job job = new Job(conf);
		job.setJobName("PartialVectorsFromTokenizedDoc::MakePartialVectors: input-folder: "
				+ tokenizedDocDir + ", dictionary-file: " + dictDir);
		job.setJarByClass(PartialVectorsFromTokenizedDoc.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(StringTuple.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(VectorWritable.class);
		
		FileInputFormat.setInputPaths(job, tokenizedDocPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		HadoopUtil.delete(conf, outputPath);
		
		job.setMapperClass(Mapper.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setReducerClass(TFPartialVectorReducer.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setNumReduceTasks(numReducers);

		job.waitForCompletion(true);

		long t1 = System.currentTimeMillis();
		logger.info("PartialVectorsFromTokenizedDoc takes " + (double) (t1 - t0) / 1000
				+ " seconds.");

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new PartialVectorsFromTokenizedDoc(), args);
		System.exit(res);
	}
}
