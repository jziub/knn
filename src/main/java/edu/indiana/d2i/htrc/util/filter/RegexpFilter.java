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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

/**
 * keep the term according to the filter
 */
public class RegexpFilter extends TokenFilter {

	private CharTermAttribute termAtt = null;
	private List<Pattern> patterns = new ArrayList<Pattern>();
	
	protected RegexpFilter(TokenStream input, String[] regex) {
		super(input);
		termAtt = input.addAttribute(CharTermAttribute.class);
		for (int i = 0; i < regex.length; i++)
			if (regex[i].length() > 0)
				patterns.add(Pattern.compile(regex[i]));
	}

	private final boolean match(String token) {
		for (Pattern pattern : patterns) {
			if (pattern.matcher(token).matches())
				return true;
		}
		return false;
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
