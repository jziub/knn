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
# File:  LuceneUtil.java
# Description:  
#
# -----------------------------------------------------------------
# 
 */

package edu.indiana.d2i.htrc.io.index.lucene;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermPositionVector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

import edu.indiana.d2i.htrc.HTRCConstants;
import edu.indiana.d2i.htrc.io.index.filter.DictionaryFilter;
import edu.indiana.d2i.htrc.io.index.filter.FrequencyFilter;
import edu.indiana.d2i.htrc.io.index.filter.HTRCFilter;
import edu.indiana.d2i.htrc.io.index.filter.StopWordFilter;
import edu.indiana.d2i.htrc.io.index.filter.WordLengthFilter;
import edu.indiana.d2i.htrc.io.index.solr.Dictionary;

public class LuceneClient {

	private static final Log logger = LogFactory.getLog(LuceneClient.class);

	private static LuceneClient client = null;

	private IndexSearcher indexSearcher = null;
	private IndexReader indexReader = null;
	private Dictionary dictionary = null;

	private HTRCFilter filter = null;
	
	private long elapsedTime = 0;

	private LuceneClient(Configuration conf) throws IOException {
		String directory = conf.get(HTRCConstants.LUCENE_INDEX_PATH);
		// String directory = conf.get("htrc.lucene.index.path");
		FileSystem fs = FileSystem.get(conf);
		Path indexPath = new Path(directory);
		Directory dir = new FileSystemDirectory(fs, indexPath, false, conf);
		indexSearcher = new IndexSearcher(dir);
		indexReader = IndexReader.open(dir);

		dictionary = new Dictionary(conf);

		// dynamic load the filter ??
//		filter = new StopWordFilter();
		filter = new StopWordFilter("stopwords.txt"); // found in the classpath
		filter.addNextFilter(new DictionaryFilter(dictionary));
		filter.addNextFilter(new FrequencyFilter(conf.getInt(
				HTRCConstants.FILTER_WORD_MIN_FREQUENCE, 2)));
		filter.addNextFilter(new WordLengthFilter(conf.getInt(
				HTRCConstants.FILTER_WORD_MIN_LENGTH, 2)));
	}

	public void close() throws IOException {
		indexReader.close();
		indexSearcher.close();
	}

	public Vector getTFVector(String volumeId) throws IOException {
		Vector result = new RandomAccessSparseVector(dictionary.size());

		logger.info("Get TF vector for " + volumeId);

		TermQuery termquery = new TermQuery(new Term("id", volumeId));
		TopDocs hits = indexSearcher.search(termquery, indexSearcher.maxDoc());
		ScoreDoc[] docs = hits.scoreDocs;
		int docId = docs[0].doc; // only one hit!!!
		TermPositionVector vector = (TermPositionVector) indexReader
				.getTermFreqVector(docId, "ocr");

		long t0 = System.nanoTime();
		String[] terms = vector.getTerms();
		int[] freq = vector.getTermFrequencies();
		for (int j = 0; j < terms.length; j++) {
			// if (dictionary.containsKey(terms[j])) {
			// result.setQuick(dictionary.get(terms[j]), freq[j]);
			// }

			if (filter.accept(terms[j], freq[j])) {
				result.setQuick(dictionary.get(terms[j]), freq[j]);
			}
		}		
		long t1 = System.nanoTime();
		elapsedTime += t1 - t0;
		
		return result;
	}

	public long getCPUTime() {
		return elapsedTime;
	}
	
	public static LuceneClient createLuceneClient(Configuration conf)
			throws IOException {
		if (client == null)
			client = new LuceneClient(conf);
		return client;
	}
}
