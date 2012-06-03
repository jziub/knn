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
# File:  HTRCFilterAnalyzerTest.java
# Description:  
#
# -----------------------------------------------------------------
# 
 */

package edu.indiana.d2i.htrc.util.filter;

import java.io.IOException;
import java.io.StringReader;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

public class HTRCFilterAnalyzerTest {
	public static void main(String[] args) throws IOException {
		HTRCFilterAnalyzer analyzer = new HTRCFilterAnalyzer();

		TokenStream stream = analyzer
				.reusableTokenStream(
						"field",
						new StringReader(
								"a iss Pierre 1 Vinken , 61 years old , "
										+ "will join the board as joins a nonexecutive joining director Nov. "
										+ "29 .Mr. car Vinken is cars chairman of Elsevier N.V. , the Dutch "
										+ "publishing group ."));
		CharTermAttribute termAtt = stream
				.addAttribute(CharTermAttribute.class);
		stream.reset();
		while (stream.incrementToken()) {
			if (termAtt.length() > 0) {
				System.out.println(new String(termAtt.buffer(), 0, termAtt.length()));
			}
		}
		
		System.out.println("Done???");
	}
}
