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
# File:  Dictionary.java
# Description:  
#
# -----------------------------------------------------------------
# 
 */

package edu.indiana.d2i.htrc.io.index;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
import org.apache.mahout.math.map.OpenObjectIntHashMap;

import edu.indiana.d2i.htrc.HTRCConstants;
import edu.indiana.d2i.htrc.io.dataapi.IDInputSplit;

public class Dictionary {

	private OpenObjectIntHashMap<String> dictionary = new OpenObjectIntHashMap<String>();

	public static void Dictionary2SeqFile(Configuration conf, String input, String output) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		DataInputStream fsinput = new DataInputStream(fs.open(new Path(input)));
		BufferedReader reader = new BufferedReader(new InputStreamReader(fsinput));
		
		SequenceFile.Writer writer = new SequenceFile.Writer(FileSystem.get(conf), conf,
				new Path(output), Text.class, IntWritable.class);
		
		String line = null;
		Text key = new Text();
		IntWritable value = new IntWritable();
		int count = 0;
		while ((line = reader.readLine()) != null) {
			key.set(line);
			value.set(count++);
			writer.append(key, value);
		}
		
		writer.close();
		reader.close();
	} 	
	
	public Dictionary(Configuration conf) throws IOException {
//		String dicionaryPath = conf.get(HTRCConstants.DICTIONARY_PATH);
		String dicionaryPath = conf.get("htrc.solr.dictionary");
		
		System.out.println("!!!dicionaryPath " + dicionaryPath);
		
		SequenceFile.Reader reader = new SequenceFile.Reader(
				FileSystem.get(conf), new Path(dicionaryPath), conf);
		Text key = new Text();
		IntWritable value = new IntWritable();
		while (reader.next(key, value)) {
			dictionary.put(key.toString(), value.get());
		}
		reader.close();
	}

	public boolean containsKey(String term) {
		return dictionary.containsKey(term);
	}

	public int get(String term) {
		return dictionary.get(term);
	}

	public int size() {
		return dictionary.size();
	}
}
