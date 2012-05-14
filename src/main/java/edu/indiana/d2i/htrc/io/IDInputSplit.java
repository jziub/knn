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
# File:  IDInputSplit.java
# Description:  
#
# -----------------------------------------------------------------
# 
*/

package edu.indiana.d2i.htrc.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class IDInputSplit extends InputSplit implements Writable {
	private List<String> idList = new ArrayList<String>();
	private String[] hosts = null;

	@Override
	public void readFields(DataInput input) throws IOException {
		int idListSize = input.readInt();
		this.idList = new ArrayList<String>();
		for (int i = 0; i < idListSize; i++) {
			idList.add(input.readUTF());
		}
		int hostsSize = input.readInt();
		for (int i = 0; i < hostsSize; i++) {
			hosts[i] = input.readUTF();
		}
	}

	@Override
	public void write(DataOutput output) throws IOException {
		output.write(idList.size());
		for (String id : idList) {
			output.writeUTF(id);
		}
		output.write(hosts.length);
		for (int i = 0; i < hosts.length; i++) {
			output.writeUTF(hosts[i]);
		}
	}

	@Override
	public long getLength() throws IOException, InterruptedException {
		return idList.size();
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		return hosts;
	}
	
	public void addID(String id) {
		idList.add(id);
	}
	
	public IDInputSplit(String[] hosts) {
		this.hosts = hosts;
	}
}
