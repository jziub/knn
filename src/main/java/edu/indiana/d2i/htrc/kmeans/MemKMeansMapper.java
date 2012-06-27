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
# File:  MemKMeansMapper.java
# Description:  
#
# -----------------------------------------------------------------
# 
 */

package edu.indiana.d2i.htrc.kmeans;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.clustering.ClusterObservations;
import org.apache.mahout.clustering.kmeans.Cluster;
import org.apache.mahout.clustering.kmeans.KMeansConfigKeys;
import org.apache.mahout.common.ClassUtils;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.math.VectorWritable;

import com.google.common.collect.Lists;

public class MemKMeansMapper extends Mapper<WritableComparable<?>, VectorWritable, Text, ClusterObservations> {

	private KMeansClusterer clusterer;

	private final Collection<Cluster> clusters = Lists.newArrayList();

	@Override
	protected void map(WritableComparable<?> key, VectorWritable point,
			Context context) throws IOException, InterruptedException {
		this.clusterer.emitPointToNearestCluster(point.get(), this.clusters,
				context);
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

		this.clusterer = new KMeansClusterer(measure);

		// load clusters from memcache
		MemKMeansUtil.loadClusterInfo(conf, clusters);
	}
}
