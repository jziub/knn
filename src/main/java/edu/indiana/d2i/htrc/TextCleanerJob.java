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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.indiana.d2i.htrc.util.clean.LowerCaseCleaner;
import edu.indiana.d2i.htrc.util.clean.TextCleaner;

public class TextCleanerJob extends Configured implements Tool {
	static class TextCleanerMapper extends Mapper<Text, Text, Text, Text> {
		private TextCleaner cleaner = null;
		private Text cleanedText = new Text();
		
		@Override
		public void map(Text key, Text value, Context context) 
				throws IOException, InterruptedException {
			String text = cleaner.handleText(value.toString());
			cleanedText.set(text);
			context.write(key, cleanedText);
		}
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			cleaner = new LowerCaseCleaner();
		}
	}
	
	private static final Log logger = LogFactory
			.getLog(TextCleanerJob.class);

	private void printUsage() {
		System.out.println("Bad input arguments!");
		System.exit(1);
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			printUsage();
		}

		String inputPath = args[0];
		String outputPath = args[1];

		logger.info("TextCleanerJob ");
		logger.info(" - input: " + inputPath);
		logger.info(" - output: " + outputPath);

		Job job = new Job(getConf(),
				"Clean HTRC text.");
		job.setJarByClass(TextCleanerJob.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(TextCleanerMapper.class);
		job.setNumReduceTasks(0);

		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		long start = System.nanoTime();
		job.waitForCompletion(true);
		logger.info("ParallelDataCopyJob took " + (System.nanoTime()-start)/1e9 + " seconds.");
		
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new TextCleanerJob(), args);
		System.exit(res);
	}
}
