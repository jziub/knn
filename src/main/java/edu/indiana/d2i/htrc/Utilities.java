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

package edu.indiana.d2i.htrc;

import org.apache.hadoop.conf.Configuration;

public class Utilities {
	public static HTRCDataAPIClient creatDataAPIClient(Configuration conf) {
		String dataEPR = conf.get(HTRCConstaints.DATA_API_EPR, "https://129-79-49-119.dhcp-bl.indiana.edu:25443/data-api");
		String delimitor = conf.get(HTRCConstaints.DATA_API_URL_DELIMITOR, "|");
		String clientID = conf.get(HTRCConstaints.DATA_API_CLIENTID, "yim");
		String clientSecrete = conf.get(HTRCConstaints.DATA_API_CLIENTSECRETE, "yim");
		String tokenLoc = conf.get(HTRCConstaints.DATA_API_TOKENLOC, "https://129-79-49-119.dhcp-bl.indiana.edu:25443/oauth2/token?grant_type=client_credentials");
		boolean selfsigned = conf.getBoolean(HTRCConstaints.DATA_API_SELFSIGNED, true);
			
		HTRCDataAPIClient dataClient = new HTRCDataAPIClient.Builder(dataEPR, delimitor)
			.authentication(true).selfsigned(selfsigned).clientID(clientID)
			.clientSecrete(clientSecrete).tokenLocation(tokenLoc).build();
		return dataClient;
	}
}
