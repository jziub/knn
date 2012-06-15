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
# File:  SequentialDataCopyJobTest.java
# Description:  
#
# -----------------------------------------------------------------
# 
 */

package edu.indiana.d2i.htrc;

import java.util.Iterator;
import java.util.Map.Entry;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.indiana.d2i.htrc.io.dataapi.HTRCDataAPIClient;
import edu.indiana.d2i.htrc.util.Utilities;

public class DataCopyJobTest extends Configured implements Tool {
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		String outputPath = args[0]; // result

		HTRCDataAPIClient client = Utilities.creatDataAPIClient(conf);

		FileSystem fs = FileSystem.get(conf);
		
		FileStatus[] status = fs.listStatus(new Path(outputPath), Utilities.HIDDEN_FILE_FILTER);
		Text key = new Text();
		Text value = new Text();
		for (int i = 0; i < status.length; i++) {
			SequenceFile.Reader seqReader = new SequenceFile.Reader(
					fs, status[i].getPath(), conf);
			while (seqReader.next(key, value)) {
				Iterable<Entry<String, String>> content = 
						client.getID2Content(key.toString());
				Iterator<Entry<String, String>> iterator = content.iterator();
				Entry<String, String> entry = iterator.next();
				Assert.assertEquals(entry.getValue(), value.toString());
			}
		}
		
		System.out.println("Finish validation.");
		
//		FileStatus[] status = fs.listStatus(new Path(outputPath), Utilities.HIDDEN_FILE_FILTER);
//		for (int i = 0; i < status.length; i++) {
//			System.out.println(status[i].getPath().getName());
//		}
//		System.out.println("==========================================");
//		FileStatus[] globStatus = fs.globStatus(new Path(outputPath));
//		for (int i = 0; i < globStatus.length; i++) {
//			System.out.println(globStatus[i].getPath().getName());
//		}
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new DataCopyJobTest(), args);
		System.exit(res);
	}
}
