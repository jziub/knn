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

import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.StringTuple;
import org.apache.mahout.vectorizer.DocumentProcessor;

import edu.indiana.d2i.htrc.HTRCConstants;
import edu.indiana.d2i.htrc.io.lib.IDInputFormat;
import edu.indiana.d2i.htrc.util.Utilities;

public class DataCopyTokenizerJob extends Configured implements Tool {
	private static final Log logger = LogFactory
			.getLog(DataCopyTokenizerJob.class);

	private void printUsage() {
		System.out.println("Bad input arguments!");
		System.exit(1);
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 6) {
			printUsage();
		}

		String inputPath = args[0];
		String outputPath = args[1];
		int maxIdsPerSplit = Integer.valueOf(args[2]);
		String dataAPIConfClassName = args[3];
		String analyzerClassName = args[4];
		int maxIdsPerReq = Integer.valueOf(args[5]);

		logger.info("DataCopyTokenizerJob ");
		logger.info(" - input: " + inputPath);
		logger.info(" - output: " + outputPath);
		logger.info(" - maxIdsPerSplit: " + maxIdsPerSplit);
		logger.info(" - dataAPIConfClassName: " + dataAPIConfClassName);
		logger.info(" - analyzerName: " + analyzerClassName);
		logger.info(" - maxIdsPerReq: " + maxIdsPerReq);
		
		// upload dictionary file to HDFS
//		FileSystem fs = FileSystem.get(getConf());
//		Path dictionaryPath = new Path(outputPath, Utilities.path2FileName(dictionaryFile));
//		BufferedWriter writer = new BufferedWriter(
//				new OutputStreamWriter(fs.create(dictionaryPath, true)));
//		BufferedReader reader = new BufferedReader(new FileReader(dictionaryFile));
//		String line = null;
//		while ((line = reader.readLine()) != null) {
//			writer.write(line + "\n");
//		}
//		writer.close();
		
		// 
		Job job = new Job(getConf(),
				"Copy and tokenize data from HTRC data storage parallely.");
		job.setJarByClass(DataCopyTokenizerJob.class);

		// set analyzer
//		Class<? extends Analyzer> analyzerClass = Class.forName(analyzerClassName).
//				asSubclass(Analyzer.class);
		job.getConfiguration().set(DocumentProcessor.ANALYZER_CLASS, analyzerClassName);
		
		// set distributed cache
//		Path dictionaryPath = new Path(dictionaryFile);
//		DistributedCache.setCacheFiles(new URI[] {dictionaryPath.toUri()}, job.getConfiguration());
		
		// set data api conf
		job.getConfiguration().setInt(HTRCConstants.MAX_IDNUM_SPLIT,
				maxIdsPerSplit);
		Utilities.setDataAPIConf(job.getConfiguration(), dataAPIConfClassName, maxIdsPerReq);	

		// no speculation
	    job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", false);
	    job.getConfiguration().setBoolean("mapred.reduce.tasks.speculative.execution", false);
		
		job.setInputFormatClass(IDInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(StringTuple.class);

		job.setMapperClass(DataCopyTokenizerMapper.class);
		job.setNumReduceTasks(0);

		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		long start = System.nanoTime();
		job.waitForCompletion(true);
		logger.info("DataCopyTokenizerJob took " + (System.nanoTime()-start)/1e9 + " seconds.");
		
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new DataCopyTokenizerJob(), args);
		System.exit(res);
	}
}
