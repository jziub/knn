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
# File:  SparseVectorUtil.java
# Description:  
#
# -----------------------------------------------------------------
# 
*/

package edu.indiana.d2i.htrc.io;

import java.io.IOException;
import java.io.StringReader;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.mahout.common.ClassUtils;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.vectorizer.DefaultAnalyzer;
import org.apache.mahout.vectorizer.DocumentProcessor;

import edu.indiana.d2i.htrc.HTRCConstants;
import edu.indiana.d2i.htrc.io.index.Dictionary;
import edu.indiana.d2i.htrc.io.index.filter.DictionaryFilter;
import edu.indiana.d2i.htrc.io.index.filter.HTRCFilter;
import edu.indiana.d2i.htrc.io.index.filter.StopWordFilter;
import edu.indiana.d2i.htrc.io.index.filter.WordLengthFilter;

public class SparseVectorUtil {
	public static Vector transform2Vector(String text, String field, 
			Analyzer analyzer, HTRCFilter filter, Dictionary dictionary) throws IOException {
		Vector result = new RandomAccessSparseVector(dictionary.size());

		TokenStream stream = analyzer.reusableTokenStream(field,
				new StringReader(text.toString()));
		CharTermAttribute termAtt = stream
				.addAttribute(CharTermAttribute.class);
		stream.reset();
		while (stream.incrementToken()) {
			// String term = new String(termAtt.buffer(), 0,
			// termAtt.length());
			String term = new String(termAtt.buffer(), 0, termAtt.length())
					.toLowerCase();
			if (filter.accept(term, 0)) {
				int index = dictionary.get(term);
				result.setQuick(index, result.get(index) + 1);
			}
		}
		
		return result;
	}
	
	public static class Text2VectorMapper extends Mapper<Text, Text, Text, VectorWritable> {
		private Analyzer analyzer;
		private VectorWritable vectorWritable = new VectorWritable();

		private static enum CounterType {
			TERMSNUM, CPUTIME
		}

		private long elapsedTime = 0;
		private long numTerms = 0;

		private Dictionary dictionary = null;
		private HTRCFilter filter = null;

		@Override
		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {			
			Vector result = transform2Vector(value.toString(), key.toString(), 
					analyzer, filter, dictionary);

			// VectorWritable vectorWritable = new VectorWritable();
			vectorWritable.set(result);
			context.write(key, vectorWritable);
		}

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);

			dictionary = new Dictionary(context.getConfiguration());

			analyzer = ClassUtils.instantiateAs(
					context.getConfiguration().get(
							DocumentProcessor.ANALYZER_CLASS,
							DefaultAnalyzer.class.getName()), Analyzer.class);

			filter = new StopWordFilter("stopwords.txt"); // found in the
															// classpath
			filter.addNextFilter(new DictionaryFilter(dictionary));
			filter.addNextFilter(new WordLengthFilter(context
					.getConfiguration().getInt(
							HTRCConstants.FILTER_WORD_MIN_LENGTH, 2)));
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			context.getCounter(CounterType.CPUTIME).increment(elapsedTime);
			context.getCounter(CounterType.TERMSNUM).increment(numTerms);
		}
	}
}
