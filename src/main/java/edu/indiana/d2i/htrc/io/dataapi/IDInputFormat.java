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
# File:  IDInputFormat.java
# Description:  
#
# -----------------------------------------------------------------
# 
*/

package edu.indiana.d2i.htrc.io.dataapi;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.LineReader;

import edu.indiana.d2i.htrc.HTRCConstants;

public class IDInputFormat<K extends Writable, V extends Writable> extends FileInputFormat<K, V>  {
	private static final Log logger = LogFactory.getLog(IDInputFormat.class);
	
	@SuppressWarnings("unchecked")
	@Override
	public RecordReader<K, V> createRecordReader(InputSplit arg0,
			TaskAttemptContext arg1) throws IOException, InterruptedException {
		return (RecordReader<K, V>) new IDRecorderReader();
	}

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		int numIdsInSplit = job.getConfiguration().getInt(HTRCConstants.MAX_IDNUM_SPLIT, 
				(int)1e6);
		String hostStr = job.getConfiguration().get(HTRCConstants.HOSTS_SEPARATEDBY_COMMA, 
				HTRCConstants.DATA_API_DEFAULT_URL);
		if (hostStr == null) 
			throw new RuntimeException("Cannot find hosts of HTRC Data Storage.");
		String[] hosts = hostStr.split(",");
		
		IDInputSplit split = new IDInputSplit(hosts);
		List<InputSplit> splits = new ArrayList<InputSplit>();
		Path[] dirs = getInputPaths(job);
		try {
			for (int i = 0; i < dirs.length; i++) {
				FileSystem fs = dirs[i].getFileSystem(job.getConfiguration());
				DataInputStream fsinput = new DataInputStream(fs.open(dirs[i]));
				Iterator<Text> idlist = new IDList(fsinput).iterator();
				while (idlist.hasNext()) {
					Text id = idlist.next();
					split.addID(id.toString());
					if (split.getLength() >= numIdsInSplit) {
						splits.add(split);
						split = new IDInputSplit(hosts);
					}
				}
				
//				LineReader reader = new LineReader(fsinput);
//				Text line = new Text();
//				while (reader.readLine(line) > 0) {
//					split.addID(line.toString());
//					if (split.getLength() >= numIdsInSplit) {
//						splits.add(split);
//						split = new IDInputSplit(hosts);
//					}
//				}
//				reader.close();
			}
			if (split != null && split.getLength() != 0) splits.add(split);
		} catch (InterruptedException e) {
			logger.error(e);
		}
		
		logger.info("#Splits " + splits.size());
		return splits;
	}
}
