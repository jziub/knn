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
# File:  VectorInspection.java
# Description:  
#
# -----------------------------------------------------------------
# 
*/

package edu.indiana.d2i.htrc.util;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

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
import org.apache.mahout.math.VectorWritable;

import edu.indiana.d2i.htrc.HTRCConstants;
import edu.indiana.d2i.htrc.io.dataapi.HTRCDataAPIClient;

/**
 * It is used to inspect the vectors after transformation of text or cluster result 
 */
public class DataAPITestDriver extends Configured implements Tool {
	private static final Log logger = LogFactory.getLog(DataAPITestDriver.class);
	
	@Override
	public int run(String[] args) throws Exception {
		String dataAPIConfClassName = args[0];
		int maxIdsPerReq = Integer.valueOf(args[1]);
		String queryStr = args[2];
		
		Configuration conf  = getConf();
		Utilities.setDataAPIConf(conf, dataAPIConfClassName, maxIdsPerReq);
		
		int maxIdRetrieved = conf.getInt(HTRCConstants.MAX_ID_RETRIEVED, 100);
		String dataEPR = conf.get(HTRCConstants.HOSTS_SEPARATEDBY_COMMA).split(",")[0];
		String delimitor = conf.get(HTRCConstants.DATA_API_URL_DELIMITOR, "|");
		String clientID = conf.get(HTRCConstants.DATA_API_CLIENTID, "yim");
		String clientSecrete = conf.get(HTRCConstants.DATA_API_CLIENTSECRETE, "yim");
		String tokenLoc = conf.get(HTRCConstants.DATA_API_TOKENLOC, "https://129-79-49-119.dhcp-bl.indiana.edu:25443/oauth2/token?grant_type=client_credentials");
		boolean selfsigned = conf.getBoolean(HTRCConstants.DATA_API_SELFSIGNED, true);
		
		if (dataEPR.equals(HTRCConstants.DATA_API_DEFAULT_URL)) {
			dataEPR = HTRCConstants.DATA_API_DEFAULT_URL_PREFIX + dataEPR;
		}
		
		HTRCDataAPIClient dataClient = new HTRCDataAPIClient.Builder(dataEPR, delimitor)
		.authentication(true).selfsigned(selfsigned).clientID(clientID)
		.clientSecrete(clientSecrete).tokenLocation(tokenLoc).build();
		
//		String queryStr = "yale.39002052249902|uc2.ark:/13960/t88g8h13f|uc2.ark:/13960/t6sx67388|uc2.ark:/13960/t5j96547r|uc2.ark:/13960/t6ww79z3v|yale.39002085406669|miua.4918260.0305.001|uc2.ark:/13960/t3416xb23|uc2.ark:/13960/t86h4mv25|loc.ark:/13960/t2k64mv58|";
		Iterable<Entry<String, String>> entries = dataClient.getID2Content(queryStr);
		for (Entry<String, String> entry : entries) {
			System.out.println(entry.getKey());
		}
		
		return 0;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new DataAPITestDriver(), args);
		System.exit(0);
	}
}
