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
# File:  StopWordFilter.java
# Description:  
#
# -----------------------------------------------------------------
# 
 */

package edu.indiana.d2i.htrc.io.index.filter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class StopWordFilter extends HTRCFilter {

	public static final String[] ENGLISH_STOP_WORDS = { "a", "an", "and",
			"are", "as", "at", "be", "but", "by", "for", "if", "in", "into",
			"is", "it", "no", "not", "of", "on", "or", "such", "that", "the",
			"their", "then", "there", "these", "they", "this", "to", "was",
			"will", "with", "which" };

	public static Set<String> STOP_WORDS_SET;

	public StopWordFilter() {
		STOP_WORDS_SET = new HashSet<String>(Arrays.asList(ENGLISH_STOP_WORDS));
	}

	public StopWordFilter(String localfile) throws IOException {
		STOP_WORDS_SET = new HashSet<String>();
		
		InputStream stopwordIn = getClass().getClassLoader()
				.getResourceAsStream(localfile);
		BufferedReader reader = new BufferedReader(new InputStreamReader(stopwordIn));
		String line = null;
		while ((line = reader.readLine()) != null) {
			STOP_WORDS_SET.add(line);
		}
		reader.close();
	}

	@Override
	public boolean accept(String term, int freq) {
		if (STOP_WORDS_SET.contains(term)) {
			return false;
		} else {
			return (nextFilter != null) ? nextFilter.accept(term, freq) : true;
		}
	}

}
