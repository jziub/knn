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
# File:  DataValidation.java
# Description:  
#
# -----------------------------------------------------------------
# 
*/

package edu.indiana.d2i.htrc.util;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.indiana.d2i.htrc.HTRCConstants;
import edu.indiana.d2i.htrc.io.dataapi.HTRCDataAPIClient;

public class DataCopyValidation extends Configured implements Tool {
private static final Log logger = LogFactory.getLog(DataCopyValidation.class);
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		
		String outputPath = args[0]; // result
		String dataAPIConfClassName = args[1];
		int maxIdsPerReq = Integer.valueOf(args[2]);
		
		logger.info("DataValidation ");
		logger.info(" - output: " + outputPath);
		logger.info(" - dataAPIConfClassName: " + dataAPIConfClassName);
		logger.info(" - maxIdsPerReq: " + maxIdsPerReq);
		
		Utilities.setDataAPIConf(conf, dataAPIConfClassName, maxIdsPerReq);

//		HTRCDataAPIClient client = Utilities.creatDataAPIClient(conf);
		String dataEPR = conf.get(HTRCConstants.HOSTS_SEPARATEDBY_COMMA, 
				"https://129-79-49-119.dhcp-bl.indiana.edu:25443/data-api");
		String delimitor = conf.get(HTRCConstants.DATA_API_URL_DELIMITOR, "|");
		String clientID = conf.get(HTRCConstants.DATA_API_CLIENTID, "yim");
		String clientSecrete = conf.get(HTRCConstants.DATA_API_CLIENTSECRETE, "yim");
		String tokenLoc = conf.get(HTRCConstants.DATA_API_TOKENLOC, "https://129-79-49-119.dhcp-bl.indiana.edu:25443/oauth2/token?grant_type=client_credentials");
		boolean selfsigned = conf.getBoolean(HTRCConstants.DATA_API_SELFSIGNED, true);
		HTRCDataAPIClient client = new HTRCDataAPIClient.Builder(dataEPR, delimitor)
		.authentication(true).selfsigned(selfsigned).clientID(clientID)
		.clientSecrete(clientSecrete).tokenLocation(tokenLoc).build();

		FileSystem fs = FileSystem.get(conf);
		FileStatus[] status = fs.listStatus(new Path(outputPath), Utilities.HIDDEN_FILE_FILTER);
		Text key = new Text();
		Text value = new Text();
		for (int i = 0; i < status.length; i++) {
			SequenceFile.Reader seqReader = new SequenceFile.Reader(
					fs, status[i].getPath(), conf);
			while (seqReader.next(key, value)) {
//				logger.info(key.toString());
				Iterable<Entry<String, String>> content = 
						client.getID2Content(key.toString());
				Iterator<Entry<String, String>> iterator = content.iterator();
				Entry<String, String> entry = iterator.next();
				if (!entry.getValue().equals(value.toString())) {
					logger.error("Book : " + key.toString() + " corrupts!");
				}
			}
		}
		
		logger.info("Finish validation.");

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new DataCopyValidation(), args);
		System.exit(res);
	}
}
