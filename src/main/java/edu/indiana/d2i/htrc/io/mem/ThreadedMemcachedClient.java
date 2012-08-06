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
# File:  ThreadedMemcachedClient.java
# Description:  
#
# -----------------------------------------------------------------
# 
*/

package edu.indiana.d2i.htrc.io.mem;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.mahout.math.VectorWritable;

import edu.indiana.d2i.htrc.HTRCConstants;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.BinaryConnectionFactory;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.transcoders.Transcoder;

public class ThreadedMemcachedClient {
//	private static ThreadedMemcachedClient threadedClient = null;
	private MemcachedClient[] clients = null;
	
	private ThreadedMemcachedClient(int numClients, List<String> hosts) {
		try {
			clients = new MemcachedClient[numClients];
			for (int i = 0; i < numClients; i++) {
//				MemcachedClient client = new MemcachedClient(
//						new BinaryConnectionFactory(),
//						AddrUtil.getAddresses(hosts));
				MemcachedClient client = new MemcachedClient(
						AddrUtil.getAddresses(hosts));
				clients[i] = client;
			}
		} catch (Exception e) {
			
		}
	}
	
	public static ThreadedMemcachedClient getThreadedMemcachedClient() {
		List<String> hosts = new ArrayList<String>();
		hosts.add("127.0.0.1:11211");
		return getThreadedMemcachedClient(1, hosts);
	}
	
	public static ThreadedMemcachedClient getThreadedMemcachedClient(Configuration conf) {
		int numClients = conf.getInt(HTRCConstants.MEMCACHED_CLIENT_NUM, 1);
		String[] hostArray = conf.getStrings(HTRCConstants.MEMCACHED_HOSTS);
		List<String> hosts = Arrays.asList(hostArray);
		return getThreadedMemcachedClient(numClients, hosts);
	}
	
	public static ThreadedMemcachedClient getThreadedMemcachedClient(int numClients, List<String> hosts) {
//		if (threadedClient == null) {
//			threadedClient = new ThreadedMemcachedClient(numClients, hosts);
//		}
//		return threadedClient;
		
		return new ThreadedMemcachedClient(numClients, hosts);
	}
	
	public MemcachedClient getCache() {
		synchronized (clients) {
			int length = clients.length;
			int index = (int)(Math.random() * length);
			return clients[index];
		}
	}
	
	public void close() {
		for (int i = 0; i < clients.length; i++) {
			clients[i].shutdown();
			clients[i] = null;
		}
//		threadedClient = null;
	}
}
