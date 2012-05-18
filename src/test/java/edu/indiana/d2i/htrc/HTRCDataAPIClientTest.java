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
# File:  HTRCDataAPIClientTest.java
# Description:  
#
# -----------------------------------------------------------------
# 
 */

package edu.indiana.d2i.htrc;

import java.util.Iterator;
import java.util.Map.Entry;

import edu.indiana.d2i.htrc.io.HTRCDataAPIClient;

public class HTRCDataAPIClientTest {
	public static void main(String[] args) throws Exception {
		String url = "129-79-49-119.dhcp-bl.indiana.edu:25443";
		String delimitor = "|";
		String clientID = "yim";
		String clientSecrete = "yim";
		String tokenLoc = "https://129-79-49-119.dhcp-bl.indiana.edu:25443/oauth2/token?grant_type=client_credentials";
		boolean selfsigned = true;

		HTRCDataAPIClient client = new HTRCDataAPIClient.Builder(url, delimitor)
				.authentication(selfsigned).selfsigned(selfsigned).clientID(clientID)
				.clientSecrete(clientSecrete).tokenLocation(tokenLoc).build();
		
		String queryStr = "inu.30000088849835|inu.30000108255831";
		Iterable<Entry<String, String>> content = client.getID2Content(queryStr);
		Iterator<Entry<String, String>> iterator = content.iterator();
		while (iterator.hasNext()) {
			Entry<String, String> entry = iterator.next();
			System.out.println(entry.getKey());
//			System.out.println(entry.getValue());
		}
	}
}
