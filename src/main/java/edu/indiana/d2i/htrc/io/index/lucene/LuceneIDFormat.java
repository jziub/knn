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
# File:  LuceneIDFormat.java
# Description:  
#
# -----------------------------------------------------------------
# 
*/

package edu.indiana.d2i.htrc.io.index.lucene;

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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.mahout.math.VectorWritable;

import edu.indiana.d2i.htrc.HTRCConstants;
import edu.indiana.d2i.htrc.io.dataapi.IDInputFormat;
import edu.indiana.d2i.htrc.io.dataapi.IDInputSplit;

public class LuceneIDFormat extends FileInputFormat<Text, VectorWritable> {
	
	private static final Log logger = LogFactory.getLog(LuceneIDFormat.class);
	
	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		int numIdsInSplit = job.getConfiguration().getInt(HTRCConstants.MAX_IDNUM_SPLIT, 
				(int)1e6);
		
		String line = null;
		IDInputSplit split = new IDInputSplit();
		List<InputSplit> splits = new ArrayList<InputSplit>();
		Path[] dirs = getInputPaths(job);
		
		try {
			for (int i = 0; i < dirs.length; i++) {
				FileSystem fs = dirs[i].getFileSystem(job.getConfiguration());
				DataInputStream fsinput = new DataInputStream(fs.open(dirs[i]));
				BufferedReader reader = new BufferedReader(new InputStreamReader(fsinput));
				while ((line = reader.readLine()) != null) {
					split.addID(line);
					if (split.getLength() >= numIdsInSplit) {
						splits.add(split);
						split = new IDInputSplit();
					}
				}
				reader.close();
			}
			if (split != null && split.getLength() != 0) splits.add(split);
		} catch (InterruptedException e) {
			logger.error(e);
		}
		
		logger.info("#Splits " + splits.size());
		return splits;
	}
	
	@Override
	public RecordReader<Text, VectorWritable> createRecordReader(InputSplit arg0,
			TaskAttemptContext arg1) throws IOException, InterruptedException {
		return new LuceneRecordReader();
	}
}
