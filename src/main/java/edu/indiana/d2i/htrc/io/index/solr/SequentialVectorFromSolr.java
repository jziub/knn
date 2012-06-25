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
# File:  SequentialVectorFromSolr.java
# Description:  
#
# -----------------------------------------------------------------
# 
*/

package edu.indiana.d2i.htrc.io.index.solr;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.InputStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.VectorWritable;

import edu.indiana.d2i.htrc.HTRCConstants;

public class SequentialVectorFromSolr extends Configured implements Tool {
	
	private static final Log logger = LogFactory.getLog(SequentialVectorFromSolr.class);
	
	private void printUsage() {
		System.out.println("Bad input arguments!");
		System.exit(1);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 4) {
			printUsage();
		}
		
		String solrURL = args[0];
		String dictionaryFile = args[1];
		String idsFile = args[2];
		String outputFile = args[3];
		
		logger.info("SequentialVectorFromSolr ");
		logger.info(" - solrURL: " + solrURL);
		logger.info(" - dictionaryFile: " + dictionaryFile);
		logger.info(" - idsFile: " + idsFile); // on HDFS
		logger.info(" - outputFile: " + outputFile); // on HDFS
		
		Configuration conf = getConf();
//		conf.set(HTRCConstants.SOLR_MAIN_URL, solrURL);
		conf.set("htrc.solr.url", solrURL);
		conf.set(HTRCConstants.DICTIONARY_PATH, dictionaryFile);
		
		SolrClient client = new SolrClient(conf, true);
		FileSystem fs = FileSystem.get(conf);
		
		SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf,
				new Path(outputFile), Text.class, VectorWritable.class);
		
		long t0 = System.nanoTime();
		DataInputStream fsinput = new DataInputStream(fs.open(new Path(idsFile)));
		BufferedReader reader = new BufferedReader(new InputStreamReader(fsinput));
		String line = null;
		String[] ids = new String[1];
		VectorWritable value = new VectorWritable();
		Text key = new Text();
		int count = 0;
		while ((line = reader.readLine()) != null) {
			ids[0] = line;
			Iterable<NamedVector> termVectors = client.getTermVectors(ids);
			for (NamedVector namedVector : termVectors) {
				value.set(namedVector);
				key.set(namedVector.getName());
				writer.append(key, value);
				count++;
			}
			if (count % 1000 == 0) System.out.println("Finish " + count + " volumes.");
		}
		long t1 = System.nanoTime();
		System.out.println("Takes " + (t1 - t0) / 1e9 + " seconds");
		
		writer.close();
		reader.close();		
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new SequentialVectorFromSolr(), args);
		System.exit(res);
	}
}
