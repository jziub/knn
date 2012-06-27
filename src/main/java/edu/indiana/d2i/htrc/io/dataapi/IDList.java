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
# File:  IDList.java
# Description:  
#
# -----------------------------------------------------------------
# 
*/

package edu.indiana.d2i.htrc.io.dataapi;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

public class IDList implements Iterable<Text> {
	
	private static final Log logger = LogFactory.getLog(IDList.class);
	
	private LineReader reader = null;
	
	private class IDListIterator implements Iterator<Text> {
		private Text id = new Text(); 
		
		@Override
		public boolean hasNext() {
			try {
				return (reader.readLine(id) > 0) ? true: false;
			} catch (IOException e) {
				logger.error(e);
				return false;
			}
		}

		@Override
		public Text next() {
			return id;
		}

		@Override
		public void remove() {
			
		}
	}
	
	@Override
	public Iterator<Text> iterator() {
		return new IDListIterator();
	}

	public IDList(DataInputStream fsinput) {
		this.reader = new LineReader(fsinput);
	}
}
