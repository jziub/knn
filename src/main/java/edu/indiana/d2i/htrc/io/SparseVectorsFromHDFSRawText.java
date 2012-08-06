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

package edu.indiana.d2i.htrc.io;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.math.VectorWritable;

import edu.indiana.d2i.htrc.HTRCConstants;

public class SparseVectorsFromHDFSRawText extends Configured implements Tool {
	
	private static final Log logger = LogFactory.getLog(SparseVectorsFromHDFSRawText.class); 
	
	private final String VECTOR_DIR = "tf-vectors";
	
	private int maxIdsPerSplit;
	private String textInputDir;
	private String outputDir;
	private String dictDir;
	
	private void printUsage() {
		System.out.println("Bad input arguments!");
		System.exit(1);
	}
	
	private void createSparseVector(Path inputPath, Path outputPath) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = new Job(getConf(),
				"Create sparse vector from plain text in HDFS.");
		job.setJarByClass(SparseVectorsFromHDFSRawText.class);

		job.getConfiguration().setInt(HTRCConstants.MAX_IDNUM_SPLIT,
				maxIdsPerSplit);

		// no speculation
	    job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", false);
	    job.getConfiguration().setBoolean("mapred.reduce.tasks.speculative.execution", false);
		
	    // maximum #id per split
	    job.getConfiguration().setInt(HTRCConstants.MAX_IDNUM_SPLIT, maxIdsPerSplit);
	    
	    // dictionary and lucene
	    job.getConfiguration().set(HTRCConstants.DICTIONARY_PATH, dictDir);
	    
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(VectorWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(VectorWritable.class);

		job.setMapperClass(SparseVectorUtil.Text2VectorMapper.class);
		job.setNumReduceTasks(0);

		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		

		job.waitForCompletion(true);
	}
	
	
	// add filter !!??
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 4) {
			printUsage();
		}

		// all directories are in HDFS
		textInputDir = args[0];
		outputDir = args[1];
		dictDir = args[2];
		maxIdsPerSplit = Integer.valueOf(args[3]);

		logger.info("SparseVectorsFromHDFSRawText ");
		logger.info(" - textInputDir: " + textInputDir);
		logger.info(" - output: " + outputDir);
		logger.info(" - dictDir: " + dictDir);
		logger.info(" - maxIdsPerSplit: " + maxIdsPerSplit);

		long t0 = System.currentTimeMillis();	
		Path textpath = new Path(textInputDir);
		Path vecpath = new Path(outputDir, VECTOR_DIR);
		createSparseVector(textpath, vecpath);
		long t1 = System.currentTimeMillis();
		
		logger.info("SparseVectorsFromHDFSRawText takes " + (double)(t1 - t0)/1000 + " seconds.");
		
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new SparseVectorsFromHDFSRawText(), args);
		System.exit(res);
	}
}
