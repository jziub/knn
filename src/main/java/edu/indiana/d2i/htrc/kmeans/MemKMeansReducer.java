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
# File:  MemKMeansReducer.java
# Description:  
#
# -----------------------------------------------------------------
# 
 */

package edu.indiana.d2i.htrc.kmeans;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.clustering.ClusterObservations;
import org.apache.mahout.clustering.kmeans.Cluster;
import org.apache.mahout.clustering.kmeans.KMeansClusterer;
import org.apache.mahout.clustering.kmeans.KMeansConfigKeys;
import org.apache.mahout.common.ClassUtils;
import org.apache.mahout.common.distance.DistanceMeasure;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class MemKMeansReducer extends Reducer<Text, ClusterObservations, Text, Cluster> {
	private Map<String, Cluster> clusterMap;
	private double convergenceDelta;
	private KMeansClusterer clusterer;
	
	private Text identifier = new Text();
	private static int count = 0;
	
	@Override
	protected void reduce(Text key, Iterable<ClusterObservations> values,
			Context context) throws IOException, InterruptedException {
		Cluster cluster = clusterMap.get(key.toString());
		for (ClusterObservations delta : values) {
			cluster.observe(delta);
		}
		// force convergence calculation
		boolean converged = clusterer.computeConvergence(cluster,
				convergenceDelta);
		if (converged) {
			context.getCounter("Clustering", "Converged Clusters").increment(1);
		}
		cluster.computeParameters();
//		context.write(new Text(cluster.getIdentifier()), cluster);
		
		identifier.set(MemKMeansUtil.toClusterName(count++));
		context.write(identifier, cluster);
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		Configuration conf = context.getConfiguration();
		DistanceMeasure measure = ClassUtils.instantiateAs(
				conf.get(KMeansConfigKeys.DISTANCE_MEASURE_KEY),
				DistanceMeasure.class);
		measure.configure(conf);

		this.convergenceDelta = Double.parseDouble(conf
				.get(KMeansConfigKeys.CLUSTER_CONVERGENCE_KEY));
		this.clusterer = new KMeansClusterer(measure);
		this.clusterMap = Maps.newHashMap();

		// load clusters from memcache
		Collection<Cluster> clusters = Lists.newArrayList();
		MemKMeansUtil.loadClusterInfo(conf, clusters);
		setClusterMap(clusters);
	}

	private void setClusterMap(Collection<Cluster> clusters) {
		clusterMap = Maps.newHashMap();
		for (Cluster cluster : clusters) {
			clusterMap.put(cluster.getIdentifier(), cluster);
		}
		clusters.clear();
	}

//	public void setup(Collection<Cluster> clusters, DistanceMeasure measure) {
//		setClusterMap(clusters);
//		this.clusterer = new KMeansClusterer(measure);
//	}
}
