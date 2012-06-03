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
# File:  RegexpFilter.java
# Description:  
#
# -----------------------------------------------------------------
# 
*/

package edu.indiana.d2i.htrc.util.filter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

/**
 * keep the term according to the filter
 */
public class DictionaryFilter extends TokenFilter {

	private CharTermAttribute termAtt = null;
	private Set<String> dictionary = new HashSet<String>(); 
	
	protected DictionaryFilter(TokenStream input) {
		super(input);
		termAtt = input.addAttribute(CharTermAttribute.class);
		
//		InputStream dictIn = getClass().getClassLoader().getResourceAsStream(
//				"dict-ubuntu.txt"); 
		InputStream dictIn = getClass().getClassLoader().getResourceAsStream(
				"dictionary.txt");
		BufferedReader reader = new BufferedReader(new InputStreamReader(dictIn));
		try {
			String term = null;
			while ((term = reader.readLine()) != null) {
				dictionary.add(term);
			}
			reader.close();
		} catch (IOException e) {
			
		} finally {
			
		}
		
	}

	private final boolean match(String token) {
		return dictionary.contains(token) ? true: false ;
	}
	
	@Override
	public boolean incrementToken() throws IOException {
		while (input.incrementToken()) {
			String token = new String(termAtt.buffer(), 0, termAtt.length());
			
			if (match(token)) {
				return true;
			}
		}
		return false;
	}

}
