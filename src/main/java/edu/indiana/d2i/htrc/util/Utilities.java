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
# File:  Utilities.java
# Description:  
#
# -----------------------------------------------------------------
# 
 */

package edu.indiana.d2i.htrc.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.mahout.utils.io.ChunkedWriter;

import edu.indiana.d2i.htrc.HTRCConstants;
import edu.indiana.d2i.htrc.io.DataAPIDefaultConf;
import edu.indiana.d2i.htrc.io.dataapi.HTRCDataAPIClient;
import gov.loc.repository.pairtree.Pairtree;

public class Utilities {
	private static final Log logger = LogFactory.getLog(Utilities.class);
	
	public static String path2FileName(String path) {
		String des = path;
		int index = des.lastIndexOf("/");
		if (index == des.length() - 1) {
			des = des.substring(0, index);
			index = des.lastIndexOf("/");
		}
		
		return des.substring(index+1);
	}
	
	public static HTRCDataAPIClient creatDataAPIClient(Configuration conf) {
		String dataEPR = conf.get(HTRCConstants.DATA_API_EPR,
				"129-79-49-119.dhcp-bl.indiana.edu:25443");
		String delimitor = conf.get(HTRCConstants.DATA_API_URL_DELIMITOR, "|");
		String clientID = conf.get(HTRCConstants.DATA_API_CLIENTID, "yim");
		String clientSecrete = conf.get(HTRCConstants.DATA_API_CLIENTSECRETE, "yim");
		String tokenLoc = conf.get(HTRCConstants.DATA_API_TOKENLOC, "https://129-79-49-119.dhcp-bl.indiana.edu:25443/oauth2/token?grant_type=client_credentials");
		boolean selfsigned = conf.getBoolean(HTRCConstants.DATA_API_SELFSIGNED, true);

		return new HTRCDataAPIClient.Builder(dataEPR,
				delimitor).authentication(true).selfsigned(selfsigned)
				.clientID(clientID).clientSecrete(clientSecrete)
				.tokenLocation(tokenLoc).build();
	}

	public static final PathFilter HIDDEN_FILE_FILTER = new PathFilter() {
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_") && !name.startsWith(".");
		}
	};
	
	public static void setDataAPIConf(Configuration conf, String dataAPIConfClassName, int maxIdsPerReq) throws ClassNotFoundException {
		Class<?> dataAPIConfClass = Class.forName(dataAPIConfClassName);
		DataAPIDefaultConf confInstance = (DataAPIDefaultConf) ReflectionUtils
				.newInstance(dataAPIConfClass, conf);
		confInstance.configurate(conf, maxIdsPerReq);
		
		logger.info("Data API configuration");
		logger.info(" - host: " + conf.get(HTRCConstants.HOSTS_SEPARATEDBY_COMMA, 
				"129-79-49-119.dhcp-bl.indiana.edu:25443/data-api"));
		logger.info(" - delimitor: " + conf.get(HTRCConstants.DATA_API_URL_DELIMITOR, "|"));
		logger.info(" - clientID: " + conf.get(HTRCConstants.DATA_API_CLIENTID, "yim"));
		logger.info(" - clientSecret: " + conf.get(HTRCConstants.DATA_API_CLIENTSECRETE, "yim"));
		logger.info(" - tokenLoc: " + conf.get(HTRCConstants.DATA_API_TOKENLOC, "https://129-79-49-119.dhcp-bl.indiana.edu:25443/oauth2/token?grant_type=client_credentials"));
		logger.info(" - selfsigned: " + conf.getBoolean(HTRCConstants.DATA_API_SELFSIGNED, true));
		logger.info(" - maxIDRetrieved: " + conf.getInt(HTRCConstants.MAX_ID_RETRIEVED, 100));
	}
	
	public static void Dictionary2SeqFile(String input, String output) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(input));
		
		Configuration conf = new Configuration();
		SequenceFile.Writer writer = new SequenceFile.Writer(FileSystem.get(conf), conf,
				new Path(output), Text.class, IntWritable.class);
		
		String line = null;
		Text key = new Text();
		IntWritable value = new IntWritable();
		int count = 0;
		while ((line = reader.readLine()) != null) {
			key.set(line);
			value.set(count++);
			writer.append(key, value);
		}
		
		writer.close();
		reader.close();
	}
	
	public static void Clean2Unclean(String input, String output) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(input));
		BufferedWriter writer = new BufferedWriter(new FileWriter(output));
		
		Pairtree pairtree = new Pairtree();
		
		String line = null;
		while ((line = reader.readLine()) != null) {
			writer.write(pairtree.uncleanId(line) + "\n");
		}
		
		writer.close();
		reader.close();
	}
	
	public static void filterUnexistID(String input, String output) throws Exception {
		BufferedReader reader = new BufferedReader(new FileReader(input));
		BufferedWriter writer = new BufferedWriter(new FileWriter(output));
		
		Configuration conf = new Configuration();
		Utilities.setDataAPIConf(conf, "edu.indiana.d2i.htrc.io.DataAPISilvermapleConf", 1);	
		
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
		
		
		String line = null;
		while ((line = reader.readLine()) != null) {
			Iterable<Entry<String, String>> content = dataClient.getID2Content(line);
			if (content != null) writer.write(line + "\n"); 
		}
		
		writer.close();
		reader.close();
	}
}
