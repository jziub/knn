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
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.utils.io.ChunkedWriter;

import edu.indiana.d2i.htrc.io.dataapi.HTRCDataAPIClient;
import edu.indiana.d2i.htrc.util.Utilities;

public class SequentialDataCopyJob extends Configured implements Tool {
	private static final Log logger = LogFactory.getLog(SequentialDataCopyJob.class);
	
	private void printUsage() {
		System.out.println("Bad input arguments!");
		System.exit(1);
	}
	
	private void text2Seq(Iterable<Entry<String, String>> content, ChunkedWriter chunkWriter) 
			throws IOException {
		Iterator<Entry<String, String>> iterator = content.iterator();
		while (iterator.hasNext()) {
			Entry<String, String> entry = iterator.next();
			chunkWriter.write(entry.getKey(), entry.getValue());
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 5) {
			printUsage();
		}
		
		String inputPath = args[0];
		String outputPath = args[1];
		int chunkSizeInMB = Integer.valueOf(args[2]);
		String dataAPIConfClassName = args[3];
		int maxIdsPerReq = Integer.valueOf(args[4]);
		
		logger.info("SequentialDataCopyJob ");
		logger.info(" - input: " + inputPath);
		logger.info(" - output: " + outputPath);
		logger.info(" - chunkSizeInMB: " + chunkSizeInMB);
		logger.info(" - dataAPIConfClassName: " + dataAPIConfClassName);
		logger.info(" - maxIdsPerReq: " + maxIdsPerReq);
		
		Configuration conf = getConf();
		Utilities.setDataAPIConf(conf, dataAPIConfClassName, maxIdsPerReq);
		
		HTRCDataAPIClient client = Utilities.creatDataAPIClient(conf);
		
		ChunkedWriter chunkWriter = new ChunkedWriter(getConf(), 
				chunkSizeInMB, new Path(outputPath));
		
		Path input = new Path(inputPath);
		FileSystem fs = input.getFileSystem(conf);
		DataInputStream fsinput = new DataInputStream(fs.open(input));
		BufferedReader reader = new BufferedReader(new InputStreamReader(fsinput));
		String line = null;
		int idNumThreshold = 100;
		int idNum = 0;
		StringBuilder idList = new StringBuilder();
		while ((line = reader.readLine()) != null) {
			idList.append(line + "|");
			if ((++idNum) >= idNumThreshold) {
				text2Seq(client.getID2Content(idList.toString()), chunkWriter);
				idList = new StringBuilder();
				idNum = 0;
			}
		}
		if (idList.length() > 0)
			text2Seq(client.getID2Content(idList.toString()), chunkWriter);
		
		chunkWriter.close();
		reader.close();
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new SequentialDataCopyJob(), args);
		System.exit(res);
	}
}
