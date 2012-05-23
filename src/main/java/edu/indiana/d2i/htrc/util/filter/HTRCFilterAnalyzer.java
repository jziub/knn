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
# File:  POSAnalyzer.java
# Description:  
#
# -----------------------------------------------------------------
# 
 */

package edu.indiana.d2i.htrc.util.filter;

import java.io.IOException;
import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.Version;

public class HTRCFilterAnalyzer extends Analyzer {
	private final StandardAnalyzer stdAnalyzer = new StandardAnalyzer(Version.LUCENE_31);
	
	public HTRCFilterAnalyzer() {
		
	}
	
	@Override
	public TokenStream tokenStream(String fieldName, Reader reader) {
		TokenStream stream = stdAnalyzer.tokenStream(fieldName, reader);
		stream = new LowerCaseTransformer(stream);
		stream = new POSFilter(stream, new String[]{"NN.*"});
		return stream;
	}

	@Override
	public TokenStream reusableTokenStream(String fieldName, Reader reader)
			throws IOException {
		TokenStream stream = stdAnalyzer.reusableTokenStream(fieldName, reader);
		stream = new LowerCaseTransformer(stream);
		stream = new POSFilter(stream, new String[]{"NN.*"});		
		return stream;
	}
}
