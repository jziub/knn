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
# File:  MemCachedOutputFormat.java
# Description:  
#
# -----------------------------------------------------------------
# 
*/

package edu.indiana.d2i.htrc.io.mem;

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class MemCachedOutputFormat<K extends Writable, V extends Writable> extends OutputFormat<K, V> {

	// committer starts
	public static class MemCachedOutputCommitter extends OutputCommitter {

		@Override
		public void abortTask(TaskAttemptContext arg0) throws IOException {
			
		}

		@Override
		public void commitTask(TaskAttemptContext arg0) throws IOException {
			//
		}

		@Override
		public boolean needsTaskCommit(TaskAttemptContext arg0)
				throws IOException {
			return true;
		}

		@Override
		public void setupJob(JobContext arg0) throws IOException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void setupTask(TaskAttemptContext arg0) throws IOException {
			// nothing
		}
	}
	// committer end 
	
	@Override
	public void checkOutputSpecs(JobContext arg0) throws IOException,
			InterruptedException {
		// nothing
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new MemCachedOutputCommitter();
	}

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new MemCachedRecordWriter<K, V>(context.getConfiguration());
	}
	
}
