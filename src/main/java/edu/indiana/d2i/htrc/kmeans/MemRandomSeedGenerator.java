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

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.io.Closeables;

import edu.indiana.d2i.htrc.HTRCConstants;
import edu.indiana.d2i.htrc.io.dataapi.IDInputSplit;
import edu.indiana.d2i.htrc.io.dataapi.IDList;
import edu.indiana.d2i.htrc.io.mem.HadoopWritableTranscoder;
import edu.indiana.d2i.htrc.io.mem.ThreadedMemcachedClient;

import net.spy.memcached.MemcachedClient;
import net.spy.memcached.transcoders.Transcoder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.clustering.kmeans.Cluster;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Given an Input Path containing a {@link org.apache.hadoop.io.SequenceFile},
 * randomly select k vectors and write them to the output file as a
 * {@link org.apache.mahout.clustering.kmeans.Cluster} representing the initial
 * centroid to use.
 */
public final class MemRandomSeedGenerator {

	private static final Logger logger = LoggerFactory
			.getLogger(MemRandomSeedGenerator.class);

	private MemRandomSeedGenerator() {
	}

	public static void buildRandom(Configuration conf, Path input,
			int k, DistanceMeasure measure) throws IOException {
		// build id list
		FileSystem fs = FileSystem.get(conf);
		DataInputStream fsinput = new DataInputStream(fs.open(input));
		Iterator<Text> idIterator = new IDList(fsinput).iterator();
		List<String> idlist = new ArrayList<String>();
		while (idIterator.hasNext()) {
			Text id = idIterator.next();
			idlist.add(id.toString());
		}

		ThreadedMemcachedClient client = ThreadedMemcachedClient
				.getThreadedMemcachedClient(conf);
		MemcachedClient cache = client.getCache();
		Transcoder<VectorWritable> vectorTranscoder = new HadoopWritableTranscoder<VectorWritable>(
				conf, VectorWritable.class);

		// pick k random id
		List<Text> chosenTexts = Lists.newArrayListWithCapacity(k);
		List<Cluster> chosenClusters = Lists.newArrayListWithCapacity(k);
		int nextClusterId = 0;
//		Random random = RandomUtils.getRandom();
//		for (String id : idlist) {
//			VectorWritable vectorWritable = cache.get(id, vectorTranscoder);
//			if (vectorWritable != null) {
//				Cluster newCluster = new Cluster(vectorWritable.get(),
//						nextClusterId++, measure);
//				newCluster.observe(vectorWritable.get(), 1);
//				Text newText = new Text(id);
//				int currentSize = chosenTexts.size();
////				if (currentSize < k) {
////					chosenTexts.add(newText);
////					chosenClusters.add(newCluster);
////				} else if (random.nextInt(currentSize + 1) != 0) {
////					int indexToRemove = random.nextInt(currentSize);
////					chosenTexts.remove(indexToRemove);
////					chosenClusters.remove(indexToRemove);
////					chosenTexts.add(newText);
////					chosenClusters.add(newCluster);
////				}
//			} else {
//				logger.error("cannot find VectorWritable for " + id);
////				throw new RuntimeException("cannot find VectorWritable for " + id);
//			}
//		}
		
		for (int i = 0; i < k; i++) {
			String id = idlist.get(i);
			VectorWritable vectorWritable = cache.get(id, vectorTranscoder);
			
			System.out.println("pick " + id);
			
			if (vectorWritable != null) {
				Cluster newCluster = new Cluster(vectorWritable.get(),
						nextClusterId++, measure);
				chosenClusters.add(newCluster);				
			} else {
				logger.error("cannot find VectorWritable for " + id);
				throw new RuntimeException("cannot find VectorWritable for " + id);
			}
		}

		// write out the seeds to Memcached
		int maxExpir = conf.getInt(HTRCConstants.MEMCACHED_MAX_EXPIRE, -1);
		Transcoder<Cluster> clusterTranscoder = new HadoopWritableTranscoder<Cluster>(
				conf, Cluster.class);
		for (int i = 0; i < chosenClusters.size(); i++) {
			System.out.println("set cluster " + MemKMeansUtil.toClusterName(i));
			
			cache.set(MemKMeansUtil.toClusterName(i), maxExpir,
					chosenClusters.get(i), clusterTranscoder);
			
			Cluster cluster = cache.get(MemKMeansUtil.toClusterName(i), clusterTranscoder);
			if (cluster != null) {
				DataOutputBuffer buf = new DataOutputBuffer();
				cluster.write(buf);
				System.out.println("read from memcached " + cluster.getIdentifier() + " size " + buf.size());
			}
		}
		client.close();
	}
}
