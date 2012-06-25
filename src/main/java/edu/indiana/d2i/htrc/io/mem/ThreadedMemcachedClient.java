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
import java.util.List;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.BinaryConnectionFactory;
import net.spy.memcached.MemcachedClient;

public class ThreadedMemcachedClient {
	private static ThreadedMemcachedClient threadedClient = null;
	private static MemcachedClient[] clients = null;
	
	private ThreadedMemcachedClient(int numClients, List<String> hosts) {
		try {
			clients = new MemcachedClient[numClients];
			for (int i = 0; i < numClients; i++) {
				MemcachedClient client = new MemcachedClient(
						new BinaryConnectionFactory(),
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
	
	public static ThreadedMemcachedClient getThreadedMemcachedClient(int numClients, List<String> hosts) {
		if (threadedClient == null) {
			threadedClient = new ThreadedMemcachedClient(numClients, hosts);
		}
		return threadedClient;
	}
	
	public MemcachedClient getCache() {
		synchronized (clients) {
			int length = clients.length;
			int index = (int)(Math.random() * length);
			return clients[index];
		}
	}
}
