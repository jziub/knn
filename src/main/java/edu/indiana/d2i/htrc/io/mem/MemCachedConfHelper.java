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
# File:  MemCachedConfHelper.java
# Description:  
#
# -----------------------------------------------------------------
# 
*/

package edu.indiana.d2i.htrc.io.mem;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.indiana.d2i.htrc.HTRCConstants;

public class MemCachedConfHelper {
	public static void hadoopConfHelper(Configuration conf, String memhostsPath) throws IOException {
		List<String> hosts = new ArrayList<String>();
		FileSystem fs = FileSystem.get(conf);
		DataInputStream fsinput = new DataInputStream(fs.open(new Path(memhostsPath)));
		BufferedReader reader = new BufferedReader(new InputStreamReader(fsinput));
		String line = null;
		while ((line = reader.readLine()) != null) {
			hosts.add(line);
		}
		reader.close();
		String[] hostsArray = hosts.toArray(new String[hosts.size()]);
		
		conf.setInt(HTRCConstants.MEMCACHED_CLIENT_NUM, 1);
//		conf.setInt(HTRCConstants.MEMCACHED_MAX_EXPIRE, Integer.MAX_VALUE);
		conf.setInt(HTRCConstants.MEMCACHED_MAX_EXPIRE, 60*5); // seconds
		conf.setStrings(HTRCConstants.MEMCACHED_HOSTS, hostsArray);
	}
}
