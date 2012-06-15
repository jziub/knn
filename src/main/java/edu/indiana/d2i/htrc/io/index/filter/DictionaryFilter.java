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
# File:  DictionaryFilter.java
# Description:  
#
# -----------------------------------------------------------------
# 
*/

package edu.indiana.d2i.htrc.io.index.filter;

import edu.indiana.d2i.htrc.io.index.solr.Dictionary;

public class DictionaryFilter extends HTRCFilter {

	private Dictionary dictionary = null;
	
	public DictionaryFilter(Dictionary dictionary) {
		this.dictionary = dictionary;
	}
	
	@Override
	public boolean accept(String term, int freq) {
		if (!dictionary.containsKey(term)) {
			return false;
		} else {
			return (nextFilter != null) ? nextFilter.accept(term, freq): true; 
		}
	}
}
