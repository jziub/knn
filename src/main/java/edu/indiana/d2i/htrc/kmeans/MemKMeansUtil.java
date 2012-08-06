/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.indiana.d2i.htrc.kmeans;

import java.io.IOException;
import java.util.Collection;

import net.spy.memcached.MemcachedClient;
import net.spy.memcached.transcoders.Transcoder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.clustering.canopy.Canopy;
import org.apache.mahout.clustering.kmeans.Cluster;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirValueIterable;

import edu.indiana.d2i.htrc.HTRCConstants;
import edu.indiana.d2i.htrc.io.mem.HadoopWritableTranscoder;
import edu.indiana.d2i.htrc.io.mem.ThreadedMemcachedClient;

final class MemKMeansUtil {

	private MemKMeansUtil() {
	}

	public static final String CLUSTER_NAMESPACE = "cl:";
	
	/** Configure the mapper with the cluster info */
	public static void configureWithClusterInfo(Configuration conf,
			Path clusterPath, Collection<Cluster> clusters) {
		for (Writable value : new SequenceFileDirValueIterable<Writable>(
				clusterPath, PathType.LIST, PathFilters.partFilter(), conf)) {
			Class<? extends Writable> valueClass = value.getClass();
			if (valueClass.equals(Cluster.class)) {
				// get the cluster info
				clusters.add((Cluster) value);
			} else if (valueClass.equals(Canopy.class)) {
				// get the cluster info
				Canopy canopy = (Canopy) value;
				clusters.add(new Cluster(canopy.getCenter(), canopy.getId(),
						canopy.getMeasure()));
			} else {
				throw new IllegalStateException("Bad value class: "
						+ valueClass);
			}
		}
	}
	
	public static void loadClusterInfo(Configuration conf, Collection<Cluster> clusters) {
		int k = conf.getInt(MemKMeansConfig.CLUSTER_NUM, -1);
		if (k == -1)
			throw new IllegalArgumentException("Number of cluster is -1!");
		
		ThreadedMemcachedClient client = ThreadedMemcachedClient
				.getThreadedMemcachedClient(conf);
		MemcachedClient cache = client.getCache();
		Transcoder<Cluster> clusterTranscoder = new HadoopWritableTranscoder<Cluster>(
				conf, Cluster.class);
		
		for (int i = 0; i < k; i++) {
			Cluster cluster = cache.get(toClusterName(i), clusterTranscoder);
			if (cluster != null) {
				clusters.add(cluster);
			} else {
//				logger.error("cannot find VectorWritable for " + id);
				client.close();
				throw new RuntimeException("can't find cluster " + toClusterName(i));
			}			
		}
		client.close();
	}
	
	public static boolean isConverged(Configuration conf) {
		int k = conf.getInt(MemKMeansConfig.CLUSTER_NUM, -1);
		if (k == -1)
			throw new IllegalArgumentException("Number of cluster is -1!");
		
		ThreadedMemcachedClient client = ThreadedMemcachedClient
				.getThreadedMemcachedClient(conf);
		MemcachedClient cache = client.getCache();
		Transcoder<Cluster> clusterTranscoder = new HadoopWritableTranscoder<Cluster>(
				conf, Cluster.class);
		
		for (int i = 0; i < k; i++) {
			Cluster cluster = cache.get(toClusterName(i), clusterTranscoder);
			if (cluster != null) {
				if (!cluster.isConverged())
					return false;
			} else {
				throw new RuntimeException("can't find cluster " + toClusterName(i));
			}			
		}
		client.close();
		
		return true;
	}
	
	public static String toClusterName(int id) {
		return CLUSTER_NAMESPACE + id;
	}
	
	public static void kmeansConfigHelper(Configuration conf, int k) {
		conf.setInt(MemKMeansConfig.CLUSTER_NUM, k);
		conf.set(MemKMeansConfig.KEY_NS, CLUSTER_NAMESPACE);
	}
	
	public static void writeClusters2HDFS(Configuration conf, Path des) throws IOException {
		int k = conf.getInt(MemKMeansConfig.CLUSTER_NUM, -1);
		if (k == -1)
			throw new IllegalArgumentException("Number of cluster is -1!");
		
		ThreadedMemcachedClient client = ThreadedMemcachedClient
				.getThreadedMemcachedClient(conf);
		MemcachedClient cache = client.getCache();
		Transcoder<Cluster> clusterTranscoder = new HadoopWritableTranscoder<Cluster>(
				conf, Cluster.class);
		
		SequenceFile.Writer writer = new SequenceFile.Writer(
				FileSystem.get(conf), conf, des, Text.class, Cluster.class);
		Text key = new Text();
		for (int i = 0; i < k; i++) {
			Cluster cluster = cache.get(toClusterName(i), clusterTranscoder);
			key.set(cluster.getIdentifier());
			writer.append(key, cluster);		
		}
		writer.close();
		client.close();
	}

}
