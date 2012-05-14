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

package edu.indiana.d2i.htrc.io;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import edu.indiana.d2i.htrc.HTRCConstaints;

public class IDInputFormat<K, V> extends FileInputFormat<K, V>  {
	private static final Log logger = LogFactory.getLog(IDInputFormat.class);
	
	@Override
	public RecordReader<K, V> createRecordReader(InputSplit arg0,
			TaskAttemptContext arg1) throws IOException, InterruptedException {
		return null;
	}

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		int numIdsInSplit = job.getConfiguration().getInt(HTRCConstaints.MAX_IDNUM_SPLIT, 
				(int)1e6);
		String hostStr = job.getConfiguration().get(HTRCConstaints.HOSTS_SEPARATEDBY_COMMA);
		if (hostStr == null) 
			throw new RuntimeException("Cannot find hosts of HTRC Data Storage.");
		String[] hosts = hostStr.split(",");
		
		String line = null;
		IDInputSplit split = new IDInputSplit(hosts);
		List<InputSplit> splits = new ArrayList<InputSplit>();
		Path[] dirs = getInputPaths(job);
		for (int i = 0; i < dirs.length; i++) {
			FileSystem fs = dirs[i].getFileSystem(job.getConfiguration());
			DataInputStream fsinput = new DataInputStream(fs.open(dirs[i]));
			BufferedReader reader = new BufferedReader(new InputStreamReader(fsinput));
			while ((line = reader.readLine()) != null) {
				split.addID(line);
				try {
					if (split.getLength() >= numIdsInSplit) {
						splits.add(split);
						split = new IDInputSplit(hosts);
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			reader.close();
		}
		
		logger.info("#Splits " + splits.size());
		return splits;
	}
}
