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
# File:  HTRCConstaints.java
# Description:  
#
# -----------------------------------------------------------------
# 
 */

package edu.indiana.d2i.htrc;

public class HTRCConstants {
	public static String MAX_IDNUM_SPLIT = "htrc.split.max.idnum";
	public static String MAX_ID_RETRIEVED = "htrc.reader.max.idnum";

	// data api
	public static String HOSTS_SEPARATEDBY_COMMA = "htrc.api.hosts";
	public static String DATA_API_EPR = "htrc.api.epr";
	public static String DATA_API_URL_DELIMITOR = "htrc.api.delimitor";
	public static String DATA_API_CLIENTID = "htrc.api.clientid";
	public static String DATA_API_CLIENTSECRETE = "htrc.api.clientsecrete";
	public static String DATA_API_TOKENLOC = "htrc.api.tokenloc";
	public static String DATA_API_SELFSIGNED = "htrc.api.selfsigned";

	// handle URI syntax
	public static String DATA_API_DEFAULT_URL = "dhcp-bl.indiana.edu:25443";
	public static String DATA_API_DEFAULT_URL_PREFIX = "129-79-49-119.";

	// analyzer
	// public static String ANALYZER_CLASS = "htrc.analyzer.class";

	//
	public static String SOLR_DELIMITOR = "|";
	public static String SOLR_MAIN_URL = "htrc.solr.url";
	public static String DICTIONARY_PATH = "htrc.solr.dictionary";

	// lucene
	public static String LUCENE_INDEX_PATH = "htrc.lucene.index.path";

	// filter
	public static String FILTER_WORD_MIN_FREQUENCE = "htrc.lucene.filter.min.word.freq";
	public static String FILTER_WORD_MIN_LENGTH = "htrc.lucene.filter.min.word.length";
	
	// memcache
	public static String MEMCACHED_MAX_EXPIRE = "htrc.memcached.expire.max";
	public static String MEMCACHED_CLIENT_NUM = "htrc.memcached.client.num";
	public static String MEMCACHED_HOSTS = "htrc.memcached.hosts";
}
