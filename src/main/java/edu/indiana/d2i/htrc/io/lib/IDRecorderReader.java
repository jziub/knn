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
# File:  IDRecorderReader.java
# Description:  
#
# -----------------------------------------------------------------
# 
*/

package edu.indiana.d2i.htrc.io.lib;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import edu.indiana.d2i.htrc.HTRCConstants;
import edu.indiana.d2i.htrc.io.lib.HTRCDataAPIClient.Builder;
import edu.indiana.d2i.htrc.util.Utilities;

public class IDRecorderReader extends RecordReader<Text, Text> {
	private static final Log logger = LogFactory.getLog(IDRecorderReader.class);
	
	private Configuration conf = null;
	private int maxIdRetrieved = 0;
	private String delimitor = "";
	private String dataEPR = "";
	private String clientID = "";
	private String clientSecrete = "";
	private String tokenLoc = "";
	private boolean selfsigned;
	
	private HTRCDataAPIClient dataClient = null;
	private IDInputSplit split = null;
	private Text key, value;
	
	private Iterator<String> iditerator = null;
	private Iterator<Entry<String, String>> entryIterator = null;
	
	private int numIdProcessed = 0;
	
	private Iterator<Entry<String, String>> generateID2ContentIterator() throws Exception {
		StringBuilder strBuilder = new StringBuilder();
		int count = 0;
		while (iditerator.hasNext() && count < maxIdRetrieved) {
			strBuilder.append(iditerator.next() + delimitor);
			count++;
		}
		
		if (strBuilder.length() > 0) {
			Iterable<Entry<String, String>> content = 
					dataClient.getID2Content(strBuilder.toString());
//			return (content == null) ? null: content.iterator();
			if (content != null) {
				return content.iterator();
			} else {
				logger.info("content is null!!! " + strBuilder.toString());
				logger.info("numIdProcessed: " + numIdProcessed + ", remained: " + (split.getLength()-numIdProcessed));
				return null;
			}
				
		} else {
			logger.info("strBuilder.length() " + strBuilder.length());
			logger.info("numIdProcessed: " + numIdProcessed + ", remained: " + (split.getLength()-numIdProcessed));
			return null;
		}
	}

	
	@Override
	public void close() throws IOException {
		dataClient.close();
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return (float) numIdProcessed / split.getLength();
	}

	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
			throws IOException, InterruptedException {
		split = (IDInputSplit) inputSplit;
		iditerator = split.getIDIterator();
		
		logger.info("split has " + split.getLength() + " books");
		
		conf = taskAttemptContext.getConfiguration();
		maxIdRetrieved = conf.getInt(HTRCConstants.MAX_ID_RETRIEVED, 100);
		dataEPR = split.getLocations()[0];
		delimitor = conf.get(HTRCConstants.DATA_API_URL_DELIMITOR, "|");
		clientID = conf.get(HTRCConstants.DATA_API_CLIENTID, "yim");
		clientSecrete = conf.get(HTRCConstants.DATA_API_CLIENTSECRETE, "yim");
		tokenLoc = conf.get(HTRCConstants.DATA_API_TOKENLOC, "https://129-79-49-119.dhcp-bl.indiana.edu:25443/oauth2/token?grant_type=client_credentials");
		selfsigned = conf.getBoolean(HTRCConstants.DATA_API_SELFSIGNED, true);
			
		if (dataEPR.equals(HTRCConstants.DATA_API_DEFAULT_URL)) {
			dataEPR = HTRCConstants.DATA_API_DEFAULT_URL_PREFIX + dataEPR;
		}
		
		dataClient = new HTRCDataAPIClient.Builder(dataEPR, delimitor)
			.authentication(true).selfsigned(selfsigned).clientID(clientID)
			.clientSecrete(clientSecrete).tokenLocation(tokenLoc).build();
		
//		dataClient = Utilities.creatDataAPIClient(conf);
		
		key = new Text();
		value = new Text();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		try {
			if (entryIterator == null) {
				entryIterator = generateID2ContentIterator();
				if (entryIterator == null) return false;
			} 
			
			if (!entryIterator.hasNext()) {
				entryIterator = generateID2ContentIterator();
				if (entryIterator == null) return false;
			}
			
			Entry<String, String> entry = entryIterator.next();
			key.set(entry.getKey());
			value.set(entry.getValue());
			numIdProcessed++;
			return true;
		} catch (Exception e) {
			logger.error(e);
			throw new RuntimeException(e);
		}		
	}
}
