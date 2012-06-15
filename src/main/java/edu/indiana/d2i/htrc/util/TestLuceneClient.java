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
# File:  TestLuceneClient.java
# Description:  
#
# -----------------------------------------------------------------
# 
*/

package edu.indiana.d2i.htrc.util;

import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;

import edu.indiana.d2i.htrc.io.index.lucene.LuceneClient;

public class TestLuceneClient extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		String indexDir = args[0];
		String dictDir = args[1];
		String outputDir = args[2];
		String volumeId = args[3];
		
		Configuration conf = getConf();
		conf.set("htrc.lucene.index.path", indexDir);
		conf.set("htrc.solr.dictionary", dictDir);
		
		LuceneClient client = LuceneClient.createLuceneClient(conf);
		Vector tfVector = client.getTFVector(volumeId);
		
		for (Element element : tfVector) {
			System.out.println(element.get());
		}

		return 0;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new TestLuceneClient(), args);
		System.exit(0);
	}
}
