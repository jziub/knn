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
# File:  SolrClientTest.java
# Description:  
#
# -----------------------------------------------------------------
# 
*/

package edu.indiana.d2i.htrc.io.solr;

import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.indiana.d2i.htrc.HTRCConstants;

public class SolrClientTest {

	private SolrClient client = null;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		Configuration conf = new Configuration();
		conf.set(HTRCConstants.SOLR_TV_URL, "http://chinkapin.pti.indiana.edu:9994/solr/");
		conf.set(HTRCConstants.DICTIONARY_PATH, "./src/main/resources/dictionary-seq.txt");
		client = new SolrClient(conf);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testGetTermVectors() {
		String[] ids = new String[]{"nnc2.ark+=13960=t0ns1hd2j"};
		client.getTermVectors(ids);
	}

}
