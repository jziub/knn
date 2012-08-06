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
# File:  TestMemClient.java
# Description:  
#
# -----------------------------------------------------------------
# 
 */

package edu.indiana.d2i.htrc.io.mem;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.kmeans.Cluster;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import net.spy.memcached.MemcachedClient;
import net.spy.memcached.transcoders.Transcoder;

public class TestMemClient {
	public static void main(String[] args) throws IOException {
//		ThreadedMemcachedClient client = ThreadedMemcachedClient
//				.getThreadedMemcachedClient();
		
		List<String> hosts = new ArrayList<String>();
		hosts.add("d2i-openstack-00.cs.indiana.edu:11211");
		hosts.add("d2i-openstack-01.cs.indiana.edu:11211");
		ThreadedMemcachedClient client = ThreadedMemcachedClient
				.getThreadedMemcachedClient(1, hosts);
		MemcachedClient cache = client.getCache();
		
		Configuration conf = new Configuration();
		Transcoder<VectorWritable> transcoder = 
				new HadoopWritableTranscoder<VectorWritable>(conf, VectorWritable.class);
		
//		String[] keys = {"inu.30000125311054", "inu.30000125550933", "inu.30000108255526"};
		String[] keys = {"inu.30000125550933"};
//		Text val = new Text("my world");
//		cache.set(key, 60, val, transcoder);
		
//		for (int i = 0; i < keys.length; i++) {
//			String key = keys[i];
//			VectorWritable obj = cache.get(key, transcoder);
////			String obj = (String)cache.get(key);
//			if (obj != null) {
//				System.out.println(key);
////				System.out.println(obj.get().size());
//				System.out.println(obj);
//			}
//		}

	
		BufferedWriter writer = new BufferedWriter(new FileWriter("out.txt"));
		Transcoder<Cluster> clusterTranscoder = new HadoopWritableTranscoder<Cluster>(
				conf, Cluster.class);
		for (int i = 0; i < 30; i++) {
			String key = "cl:" + i;
			Cluster obj = cache.get(key, clusterTranscoder);
//			System.out.println(obj.asFormatString());
			writer.write(obj.asFormatString() + "\n");
		}
		writer.close();
		
		System.exit(0);
	}
}
