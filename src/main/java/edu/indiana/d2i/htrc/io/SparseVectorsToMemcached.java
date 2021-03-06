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

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.Map.Entry;

import net.spy.memcached.MemcachedClient;
import net.spy.memcached.transcoders.Transcoder;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
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
import edu.indiana.d2i.htrc.io.dataapi.HTRCDataAPIClient;
import edu.indiana.d2i.htrc.io.dataapi.IDInputFormat;
import edu.indiana.d2i.htrc.io.index.Dictionary;
import edu.indiana.d2i.htrc.io.index.filter.DictionaryFilter;
import edu.indiana.d2i.htrc.io.index.filter.HTRCFilter;
import edu.indiana.d2i.htrc.io.index.filter.StopWordFilter;
import edu.indiana.d2i.htrc.io.index.filter.WordLengthFilter;
import edu.indiana.d2i.htrc.io.mem.HadoopWritableTranscoder;
import edu.indiana.d2i.htrc.io.mem.MemCachedUtil;
import edu.indiana.d2i.htrc.io.mem.MemCachedOutputFormat;
import edu.indiana.d2i.htrc.io.mem.ThreadedMemcachedClient;
import edu.indiana.d2i.htrc.util.Utilities;

/**
 * no frequence filter can be applied in this case
 */
public class SparseVectorsToMemcached extends Configured implements Tool {
	private static final Log logger = LogFactory
			.getLog(SparseVectorsToMemcached.class);

	private void printUsage() {
		System.out.println("Bad input arguments!");
		System.exit(1);
	}

	private static Vector transform2Vector(String text, String field,
			Analyzer analyzer, HTRCFilter filter, Dictionary dictionary)
			throws IOException {
		Vector result = new RandomAccessSparseVector(dictionary.size());

		TokenStream stream = analyzer.reusableTokenStream(field,
				new StringReader(text.toString()));
		CharTermAttribute termAtt = stream
				.addAttribute(CharTermAttribute.class);
		stream.reset();
		while (stream.incrementToken()) {
			// String term = new String(termAtt.buffer(), 0,
			// termAtt.length());
			String term = new String(termAtt.buffer(), 0, termAtt.length())
					.toLowerCase();
			if (filter.accept(term, 0)) {
				int index = dictionary.get(term);
				result.setQuick(index, result.get(index) + 1);
			}
		}

		return result;
	}

	private String idListDir;
	private String dictDir;
	private int maxIdsPerSplit;
	private String dataAPIConfClassName;
	private String analyzerClassName;
	private int maxIdsPerReq;
	private String memHostsPath;

	private void setupConfiguration(Configuration conf)
			throws ClassNotFoundException, IOException {
		// set dictionary
		conf.set(HTRCConstants.DICTIONARY_PATH, dictDir);

		// set analyzer
		conf.set(DocumentProcessor.ANALYZER_CLASS, analyzerClassName);

		// set data api conf
		conf.setInt(HTRCConstants.MAX_IDNUM_SPLIT, maxIdsPerSplit);
		Utilities.setDataAPIConf(conf, dataAPIConfClassName, maxIdsPerReq);

		// set memcached conf
		MemCachedUtil.configHelper(conf, memHostsPath);
	}

	private void sequentialTransform() throws Exception {
		Configuration conf = getConf();
		setupConfiguration(conf);

		HTRCDataAPIClient client = Utilities.creatDataAPIClient(conf);

		// set up analyzer, filter
		Analyzer analyzer = ClassUtils.instantiateAs(conf.get(
				DocumentProcessor.ANALYZER_CLASS,
				DefaultAnalyzer.class.getName()), Analyzer.class);
		HTRCFilter filter = new StopWordFilter("stopwords.txt"); // found in the
																	// classpath
		Dictionary dictionary = new Dictionary(conf);
		filter.addNextFilter(new DictionaryFilter(dictionary));
		filter.addNextFilter(new WordLengthFilter(conf.getInt(
				HTRCConstants.FILTER_WORD_MIN_LENGTH, 2)));

		// memcached client
		ThreadedMemcachedClient memcachedClient = ThreadedMemcachedClient
				.getThreadedMemcachedClient(conf);
		MemcachedClient cache = memcachedClient.getCache();
		int maxExpir = conf.getInt(HTRCConstants.MEMCACHED_MAX_EXPIRE, -1);
		Transcoder<VectorWritable> transcoder = new HadoopWritableTranscoder<VectorWritable>(
				conf, VectorWritable.class);

		//
		Path input = new Path(idListDir);
		FileSystem fs = input.getFileSystem(conf);
		DataInputStream fsinput = new DataInputStream(fs.open(input));
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				fsinput));
		String line = null;
		int idNumThreshold = maxIdsPerReq;
		int idNum = 0;
		StringBuilder idList = new StringBuilder();
		VectorWritable vectorWritable = new VectorWritable();
		while ((line = reader.readLine()) != null) {
			idList.append(line + "|");
			if ((++idNum) >= idNumThreshold) {
				// <id, content>
				Iterable<Entry<String, String>> content = client
						.getID2Content(idList.toString());
				for (Entry<String, String> entry : content) {
					Vector result = transform2Vector(entry.getValue(),
							entry.getKey(), analyzer, filter, dictionary);
					vectorWritable.set(result);
					cache.set(entry.getKey(), maxExpir, vectorWritable,
							transcoder);

					// validate
					VectorWritable vecWritable = cache.get(entry.getKey(),
							transcoder);
					if (vecWritable == null) {
						throw new RuntimeException(entry.getKey()
								+ " is not written to Memcached.");
					} else {
						System.out.println(entry.getKey());
					}
				}

				idList = new StringBuilder();
				idNum = 0;
			}
		}
		if (idList.length() > 0) {
			Iterable<Entry<String, String>> content = client
					.getID2Content(idList.toString());
			for (Entry<String, String> entry : content) {
				Vector result = transform2Vector(entry.getValue(),
						entry.getKey(), analyzer, filter, dictionary);
				vectorWritable.set(result);
				cache.set(entry.getKey(), maxExpir, vectorWritable, transcoder);
				
				// validate
				VectorWritable vecWritable = cache.get(entry.getKey(),
						transcoder);
				if (vecWritable == null) {
					throw new RuntimeException(entry.getKey()
							+ " is not written to Memcached.");
				} else {
					System.out.println(entry.getKey());
				}
			}
		}
	}

	private void parallelTransform() throws IOException,
			ClassNotFoundException, InterruptedException {
		//
		Job job = new Job(getConf(),
				"Create sparse vectors from HTRC data storage, store them in MemCached");
		job.setJarByClass(SparseVectorsToMemcached.class);

		Configuration conf = job.getConfiguration();
		setupConfiguration(conf);

		// no speculation
		conf.setBoolean("mapred.map.tasks.speculative.execution", false);
		conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

		job.setInputFormatClass(IDInputFormat.class);
		job.setOutputFormatClass(MemCachedOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(VectorWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(VectorWritable.class);

		job.setMapperClass(SparseVectorUtil.Text2VectorMapper.class);
		job.setNumReduceTasks(0);

		FileInputFormat.setInputPaths(job, new Path(idListDir));

		job.waitForCompletion(true);
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 8) {
			printUsage();
		}

		idListDir = args[0];
		dictDir = args[1];
		maxIdsPerSplit = Integer.valueOf(args[2]);
		dataAPIConfClassName = args[3];
		analyzerClassName = args[4];
		maxIdsPerReq = Integer.valueOf(args[5]);
		memHostsPath = args[6];
		boolean seq = Boolean.valueOf(args[7]);

		logger.info("SparseVectorsToMemcached ");
		logger.info(" - idListDir: " + idListDir); // id list
		logger.info(" - dictPath: " + dictDir); //
		logger.info(" - maxIdsPerSplit: " + maxIdsPerSplit);
		logger.info(" - dataAPIConfClassName: " + dataAPIConfClassName);
		logger.info(" - analyzerName: " + analyzerClassName);
		logger.info(" - maxIdsPerReq: " + maxIdsPerReq);
		logger.info(" - memHostsPath: " + memHostsPath); // memcached hosts list
		logger.info(" - sequential: " + seq); // memcached hosts list

		long start = System.nanoTime();
		if (seq)
			sequentialTransform();
		else
			parallelTransform();
		logger.info("SparseVectorsToMemcached took "
				+ (System.nanoTime() - start) / 1e9 + " seconds.");

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new SparseVectorsToMemcached(), args);
		System.exit(res);
	}
}
