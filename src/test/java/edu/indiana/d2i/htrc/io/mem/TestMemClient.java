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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import net.spy.memcached.MemcachedClient;
import net.spy.memcached.transcoders.Transcoder;

public class TestMemClient {
	public static void main(String[] args) {
		ThreadedMemcachedClient client = ThreadedMemcachedClient
				.getThreadedMemcachedClient();
		MemcachedClient cache = client.getCache();
		
		Transcoder<VectorWritable> transcoder = 
				new HadoopWritableTranscoder<VectorWritable>(new Configuration(), VectorWritable.class);
		
		String[] keys = {"inu.30000125311054", "inu.30000125550933", "inu.30000108255526"};
//		Text val = new Text("my world");
//		cache.set(key, 60, val, transcoder);
		
		for (int i = 0; i < keys.length; i++) {
			String key = keys[i];
			VectorWritable obj = cache.get(key, transcoder);
//			String obj = (String)cache.get(key);
			if (obj != null) {
//				System.out.println(key);
//				System.out.println(obj.get().size());
				System.out.println(obj);
			}
		}
		
		
//		Transcoder<Text> transcoder = 
//				new HadoopWritableTranscoder<Text>(new Configuration(), Text.class);
		
//		for (int i = 0; i < 3; i++) {
//			String key = "hi" + i;
//			Vector val = new RandomAccessSparseVector(10);
//			val.setQuick(i+1, i+1);
//			VectorWritable vecWritable = new VectorWritable(val);
//			cache.add(key, 60, vecWritable, transcoder);
////			cache.add(key, 60, new Text(String.valueOf(i+1)), transcoder);
//		}
//		
//		for (int i = 0; i < 3; i++) {
//			String key = "hi" + i;
//			VectorWritable vecWritable = cache.get(key, transcoder);
//			System.out.println(vecWritable);
//			
////			Text text = cache.get(key, transcoder);
////			System.out.println(text);
//		}
		
//		System.exit(0);
	}
}
