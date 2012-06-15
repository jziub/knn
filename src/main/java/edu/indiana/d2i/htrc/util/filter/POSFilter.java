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
# File:  POSFilter.java
# Description:  
#
# -----------------------------------------------------------------
# 
 */

package edu.indiana.d2i.htrc.util.filter;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTaggerME;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

/**
 * Only works for English corpus
 */
public class POSFilter extends TokenFilter {

	private CharTermAttribute termAtt = null;
	private List<Pattern> patterns = new ArrayList<Pattern>();
	
	private POSTaggerME tagger = null;
	private String[] sentence = new String[1];

	protected POSFilter(TokenStream input, String[] regex) {
		super(input);
		termAtt = input.addAttribute(CharTermAttribute.class);
		for (int i = 0; i < regex.length; i++)
			if (regex[i].length() > 0)
				patterns.add(Pattern.compile(regex[i]));
		initOpenNlp();
	}

	private void initOpenNlp() {
		InputStream modelIn = null;
		try {
			modelIn = getClass().getClassLoader().getResourceAsStream(
					"en-pos-maxent.bin"); 
			POSModel model = new POSModel(modelIn);
			tagger = new POSTaggerME(model);
		} catch (IOException e) {
			// Model loading failed, handle the error
			e.printStackTrace();
		} finally {
			if (modelIn != null) {
				try {
					modelIn.close();
				} catch (IOException e) {
				}
			}
		}
	}

	private final boolean match(String token) {
		// change to pos
		sentence[0] = token;
		String[] pos = tagger.tag(sentence);

		for (Pattern pattern : patterns) {
			if (!pattern.matcher(pos[0]).matches())
				return false;
		}
		return true;
	}

	@Override
	public boolean incrementToken() throws IOException {		
		StringBuilder string = new StringBuilder();
		while (input.incrementToken()) {
			string.delete(0, string.length());
			string.append(termAtt.buffer());			
//			String token = new String(termAtt.buffer(), 0, termAtt.length());
//			if (match(token)) {
//				return true;
//			}
			
			if (match(string.toString())) {
				return true;
			}
		}

		return false;
	}

}
