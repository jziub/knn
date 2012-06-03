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
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.PorterStemFilter;
import org.apache.lucene.analysis.StopAnalyzer;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.en.EnglishPossessiveFilter;
import org.apache.lucene.analysis.icu.ICUFoldingFilter;
import org.apache.lucene.analysis.icu.segmentation.ICUTokenizer;
import org.apache.lucene.util.Version;

public class HTRCFilterAnalyzer extends Analyzer {
	 private final EnglishAnalyzer engAnalyzer = new EnglishAnalyzer(Version.LUCENE_31);

	public HTRCFilterAnalyzer() {

	}

	@Override
	public TokenStream tokenStream(String fieldName, Reader reader) {
		// TokenStream stream = stdAnalyzer.tokenStream(fieldName, reader);
		// stream = new LowerCaseTokenizer(Version.LUCENE_31, reader);
		// stream = new PorterStemFilter(stream);
		// stream = new POSFilter(stream, new String[]{"NN.*"});
		// return stream;

//		ICUTokenizer icut = new ICUTokenizer(reader);
//		TokenStream stream = new LowerCaseFilter(Version.LUCENE_31, icut);
//		stream = new RegexpFilter(stream, new String[]{"^[a-z]+$"});
//		stream = new StopFilter(Version.LUCENE_31, stream,
//				StopAnalyzer.ENGLISH_STOP_WORDS_SET, false);
//		stream = new PorterStemFilter(stream);
//		return new ICUFoldingFilter(stream);
		
		TokenStream stream = new ICUTokenizer(reader);		
		stream = new DictionaryFilter(stream);
		stream = new StopFilter(Version.LUCENE_31, stream,
				StopAnalyzer.ENGLISH_STOP_WORDS_SET, true);
		stream = new PorterStemFilter(stream);
		stream = new LowerCaseFilter(Version.LUCENE_31, stream);
		return new ICUFoldingFilter(stream);
		
//		TokenStream stream = engAnalyzer.tokenStream(fieldName, reader);
//		stream = new LowerCaseFilter(Version.LUCENE_31, stream);
//		stream = new RegexpFilter(stream, new String[]{"^[a-z]+$"});
////		stream = new StopFilter(Version.LUCENE_31, stream,
////				StopAnalyzer.ENGLISH_STOP_WORDS_SET, false);
//		stream = new EnglishPossessiveFilter(stream);
//		stream = new PorterStemFilter(stream);
//		return stream;
	}

	@Override
	public TokenStream reusableTokenStream(String fieldName, Reader reader)
			throws IOException {
		// TokenStream stream = stdAnalyzer.reusableTokenStream(fieldName,
		// reader);
		// stream = new LowerCaseTokenizer(Version.LUCENE_31, reader);
		// stream = new PorterStemFilter(stream);
		// stream = new POSFilter(stream, new String[]{"NN.*"});
		// return stream;

//		ICUTokenizer icut = new ICUTokenizer(reader);
//		TokenStream stream = new LowerCaseFilter(Version.LUCENE_31, icut);
//		stream = new RegexpFilter(stream, new String[]{"^[a-z]+$"});
//		stream = new StopFilter(Version.LUCENE_31, stream,
//				StopAnalyzer.ENGLISH_STOP_WORDS_SET, false);
//		stream = new PorterStemFilter(stream);
//		return new ICUFoldingFilter(stream);
		
		TokenStream stream = new ICUTokenizer(reader);
		stream = new DictionaryFilter(stream);
		stream = new StopFilter(Version.LUCENE_31, stream,
				StopAnalyzer.ENGLISH_STOP_WORDS_SET, true);
		stream = new PorterStemFilter(stream);
		stream = new LowerCaseFilter(Version.LUCENE_31, stream);
		return new ICUFoldingFilter(stream);
		
//		TokenStream stream = engAnalyzer.tokenStream(fieldName, reader);
//		stream = new LowerCaseFilter(Version.LUCENE_31, stream);
//		stream = new RegexpFilter(stream, new String[]{"^[a-z]+$"});
////		stream = new StopFilter(Version.LUCENE_31, stream,
////				StopAnalyzer.ENGLISH_STOP_WORDS_SET, false);
//		stream = new EnglishPossessiveFilter(stream);
//		stream = new PorterStemFilter(stream);
//		return stream;
	}
}
