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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;
import org.apache.mahout.math.VectorWritable;

import edu.indiana.d2i.htrc.HTRCConstants;
import edu.indiana.d2i.htrc.io.index.lucene.FileSystemDirectory;
import edu.indiana.d2i.htrc.io.index.lucene.LuceneIDFormat;
import edu.indiana.d2i.htrc.io.index.solr.SolrClient;
import edu.indiana.d2i.htrc.io.index.solr.SolrIDFormat;

public class SparseVectorFromSolr extends Configured implements Tool {
	
	private static final Log logger = LogFactory.getLog(SparseVectorFromSolr.class); 
	
	private final String ID_LIST_NAME = "idlist";
	private final String VECTOR_DIR = "tf-vectors";
	
	private int maxIdsPerSplit;
	
	private String solrEPR;
	private String outputDir;
	private String dictDir;
	private String query;
	
	private void printUsage() {
		System.out.println("Bad input arguments!");
		System.exit(1);
	}

	private void generateIDList(String solrEPR, Path idfile, String queryStr) throws IOException {
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
		
//		conf.set(HTRCConstants.SOLR_MAIN_URL, solrEPR);
		conf.set("htrc.solr.url", solrEPR);
		
		System.out.println(conf.get("htrc.solr.url"));
		
		SolrClient client = new SolrClient(conf, false);
		List<String> idList = client.getIDList(queryStr);
		
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
				fs.create(idfile, true)));
		for (String id : idList) {
			writer.write(id + "\n");
		}
		writer.close();
	}
	
	private void createSparseVector(Path inputPath, Path outputPath) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = new Job(getConf(),
				"Create sparse vector from Solr.");
		job.setJarByClass(SparseVectorFromSolr.class);

		job.getConfiguration().setInt(HTRCConstants.MAX_IDNUM_SPLIT,
				maxIdsPerSplit);

		// no speculation
	    job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", false);
	    job.getConfiguration().setBoolean("mapred.reduce.tasks.speculative.execution", false);
		
	    // maximum #id per split
	    job.getConfiguration().setInt(HTRCConstants.MAX_IDNUM_SPLIT, maxIdsPerSplit);
	    
	    // dictionary and solr
//	    job.getConfiguration().set(HTRCConstants.DICTIONARY_PATH, dictDir);
	    job.getConfiguration().set("htrc.solr.dictionary", dictDir);
	    job.getConfiguration().set(HTRCConstants.SOLR_MAIN_URL, solrEPR);
	    
		job.setInputFormatClass(SolrIDFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(VectorWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(VectorWritable.class);

		job.setMapperClass(org.apache.hadoop.mapreduce.Mapper.class);
		job.setNumReduceTasks(0);

		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		job.waitForCompletion(true);
	}
	
	
	// add filter !!??
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 5) {
			printUsage();
		}

		// all directories are in HDFS
		solrEPR = args[0];
		outputDir = args[1];
		dictDir = args[2];
		query = args[3];
		maxIdsPerSplit = Integer.valueOf(args[4]);

		logger.info("SparseVectorFromSolr ");
		logger.info(" - solrEPR: " + solrEPR);
		logger.info(" - output: " + outputDir);
		logger.info(" - dictDir: " + dictDir);
		logger.info(" - query: " + query);
		logger.info(" - maxIdsPerSplit: " + maxIdsPerSplit);

		long t0 = System.currentTimeMillis();
		
		Path idpath = new Path(outputDir, ID_LIST_NAME);
		generateIDList(solrEPR, idpath, query);
	
		Path vecpath = new Path(outputDir, VECTOR_DIR);
		createSparseVector(idpath, vecpath);
		
		long t1 = System.currentTimeMillis();
		
		logger.info("SparseVectorFromSolr takes " + (double)(t1 - t0)/1000 + " seconds.");
		
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new SparseVectorFromSolr(), args);
		System.exit(res);
	}
}
