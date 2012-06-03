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
# File:  ParallelDataCopyMapper.java
# Description:  
#
# -----------------------------------------------------------------
# 
 */

package edu.indiana.d2i.htrc.io;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.mahout.common.ClassUtils;
import org.apache.mahout.common.StringTuple;
import org.apache.mahout.vectorizer.DefaultAnalyzer;
import org.apache.mahout.vectorizer.DocumentProcessor;

public class DataCopyTokenizerMapper extends
		Mapper<Text, Text, Text, StringTuple> {

//	private Set<String> dictionary = new HashSet<String>();
	private Analyzer analyzer;

	@Override
	public void map(Text key, Text value, Context context) throws IOException,
			InterruptedException {
		TokenStream stream = analyzer.reusableTokenStream(key.toString(),
				new StringReader(value.toString()));
		CharTermAttribute termAtt = stream
				.addAttribute(CharTermAttribute.class);
		StringTuple document = new StringTuple();
		stream.reset();
		while (stream.incrementToken()) {
			if (termAtt.length() > 0) {
				String term = new String(termAtt.buffer(), 0, termAtt.length());
				document.add(term);
//				if (dictionary.contains(term))
//					document.add(term);
			}
		}
		context.write(key, document);
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);

		analyzer = ClassUtils.instantiateAs(
				context.getConfiguration().get(
						DocumentProcessor.ANALYZER_CLASS,
						DefaultAnalyzer.class.getName()), Analyzer.class);

//		Configuration conf = context.getConfiguration();
//		URI[] localFiles = DistributedCache.getCacheFiles(conf);
//		if (localFiles == null || localFiles.length == 0)
//			throw new RuntimeException(
//					"Cannot find paths from distribute cache.");
//
//		Path dictionaryFile = new Path(localFiles[0].getPath());
//		FileSystem fs = FileSystem.get(conf);
//		BufferedReader reader = new BufferedReader(new InputStreamReader(
//				fs.open(dictionaryFile)));
//		String term = null;
//		while ((term = reader.readLine()) != null) {
//			dictionary.add(term.toLowerCase());
//		}
//		reader.close();
	}
}
