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
import java.io.StringReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
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
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.mahout.common.ClassUtils;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.vectorizer.DefaultAnalyzer;
import org.apache.mahout.vectorizer.DocumentProcessor;

import edu.indiana.d2i.htrc.HTRCConstants;
import edu.indiana.d2i.htrc.io.dataapi.IDInputFormat;
import edu.indiana.d2i.htrc.io.index.Dictionary;
import edu.indiana.d2i.htrc.io.index.filter.DictionaryFilter;
import edu.indiana.d2i.htrc.io.index.filter.FrequencyFilter;
import edu.indiana.d2i.htrc.io.index.filter.HTRCFilter;
import edu.indiana.d2i.htrc.io.index.filter.StopWordFilter;
import edu.indiana.d2i.htrc.io.index.filter.WordLengthFilter;
import edu.indiana.d2i.htrc.util.Utilities;

/**
 * no frequence filter can be applied in this case
 */
public class SVFromHDFS2HDFS extends Configured implements Tool {
	private static final Log logger = LogFactory
			.getLog(SVFromHDFS2HDFS.class);

	static class MapperClass extends Mapper<Text, Text, Text, VectorWritable> {
		private Analyzer analyzer;
		private VectorWritable vectorWritable = new VectorWritable();

		private static enum CounterType {
			TERMSNUM, CPUTIME
		}

		private long elapsedTime = 0;
		private long numTerms = 0;

		private Dictionary dictionary = null;
		private HTRCFilter filter = null;

		@Override
		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			Vector result = new RandomAccessSparseVector(dictionary.size());

			long initCPU = System.nanoTime();
			TokenStream stream = analyzer.reusableTokenStream(key.toString(),
					new StringReader(value.toString()));
			CharTermAttribute termAtt = stream
					.addAttribute(CharTermAttribute.class);
			stream.reset();
			while (stream.incrementToken()) {
				String term = new String(termAtt.buffer(), 0, termAtt.length());
				if (filter.accept(term, 0)) {
					int index = dictionary.get(term);
					result.setQuick(index, result.get(index)+1);
				}
			}
			elapsedTime += System.nanoTime() - initCPU;

			vectorWritable.set(result);
			context.write(key, vectorWritable);
		}

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);

			analyzer = ClassUtils.instantiateAs(
					context.getConfiguration().get(
							DocumentProcessor.ANALYZER_CLASS,
							DefaultAnalyzer.class.getName()), Analyzer.class);

			filter = new StopWordFilter("stopwords.txt"); // found in the classpath
			filter.addNextFilter(new DictionaryFilter(dictionary));
			filter.addNextFilter(new WordLengthFilter(context
					.getConfiguration().getInt(
							HTRCConstants.FILTER_WORD_MIN_LENGTH, 2)));
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			context.getCounter(CounterType.CPUTIME).increment(elapsedTime);
			context.getCounter(CounterType.TERMSNUM).increment(numTerms);
		}
	}

	private void printUsage() {
		System.out.println("Bad input arguments!");
		System.exit(1);
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 4) {
			printUsage();
		}

		String inputPath = args[0];
		String outputPath = args[1];
		String dictPath = args[2];
		String analyzerClassName = args[3];

		logger.info("DataCopyTokenizerJob ");
		logger.info(" - input: " + inputPath);
		logger.info(" - output: " + outputPath);
		logger.info(" - dictPath: " + dictPath);
		logger.info(" - analyzerName: " + analyzerClassName);

		//
		Job job = new Job(getConf(),
				"Create sparse vector from HTRC data storage.");
		job.setJarByClass(SVFromHDFS2HDFS.class);

		// set dictionary
		job.getConfiguration().set(HTRCConstants.DICTIONARY_PATH, dictPath);
		
		// set analyzer
		job.getConfiguration().set(DocumentProcessor.ANALYZER_CLASS,
				analyzerClassName);

		// no speculation
		job.getConfiguration().setBoolean(
				"mapred.map.tasks.speculative.execution", false);
		job.getConfiguration().setBoolean(
				"mapred.reduce.tasks.speculative.execution", false);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(VectorWritable.class);

		job.setMapperClass(SparseVectorUtil.Text2VectorMapper.class);
		job.setNumReduceTasks(0);

		Path output = new Path(outputPath);
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, output);
		FileSystem.get(job.getConfiguration()).delete(output, true);

		long start = System.nanoTime();
		job.waitForCompletion(true);
		logger.info("SparseVectorsFromRawText took " + (System.nanoTime() - start)
				/ 1e9 + " seconds.");

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new SVFromHDFS2HDFS(), args);
		System.exit(res);
	}
}
