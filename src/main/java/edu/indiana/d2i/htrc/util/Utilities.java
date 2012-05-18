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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.util.ReflectionUtils;

import edu.indiana.d2i.htrc.HTRCConstants;
import edu.indiana.d2i.htrc.io.DataAPIDefaultConf;
import edu.indiana.d2i.htrc.io.HTRCDataAPIClient;

public class Utilities {
	private static final Log logger = LogFactory.getLog(Utilities.class);
	
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
	
	public static void setDataAPIConf(Configuration conf, String dataAPIConfClassName) throws ClassNotFoundException {
		Class<?> dataAPIConfClass = Class.forName(dataAPIConfClassName);
		DataAPIDefaultConf confInstance = (DataAPIDefaultConf) ReflectionUtils
				.newInstance(dataAPIConfClass, conf);
		confInstance.configurate(conf);
		
		logger.info("Data API configuration");
		logger.info(" - host: " + conf.get(HTRCConstants.HOSTS_SEPARATEDBY_COMMA, 
				"https://129-79-49-119.dhcp-bl.indiana.edu:25443/data-api"));
		logger.info(" - delimitor: " + conf.get(HTRCConstants.DATA_API_URL_DELIMITOR, "|"));
		logger.info(" - clientID: " + conf.get(HTRCConstants.DATA_API_CLIENTID, "yim"));
		logger.info(" - clientSecret: " + conf.get(HTRCConstants.DATA_API_CLIENTSECRETE, "yim"));
		logger.info(" - tokenLoc: " + conf.get(HTRCConstants.DATA_API_TOKENLOC, "https://129-79-49-119.dhcp-bl.indiana.edu:25443/oauth2/token?grant_type=client_credentials"));
		logger.info(" - selfsigned: " + conf.getBoolean(HTRCConstants.DATA_API_SELFSIGNED, true));
		logger.info(" - maxIDRetrieved: " + conf.getInt(HTRCConstants.MAX_ID_RETRIEVED, 100));
	}
}
