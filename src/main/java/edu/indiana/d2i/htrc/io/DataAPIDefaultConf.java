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
# File:  DataAPIDefaultConf.java
# Description:  
#
# -----------------------------------------------------------------
# 
*/

package edu.indiana.d2i.htrc.io;

import org.apache.hadoop.conf.Configuration;

import edu.indiana.d2i.htrc.HTRCConstants;

public class DataAPIDefaultConf {
	public void configurate(Configuration conf) {
		conf.setInt(HTRCConstants.MAX_ID_RETRIEVED, 50);
	}
}
