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
# File:  StreamingKMeansReducer.java
# Description:  
#
# -----------------------------------------------------------------
# 
 */

package edu.indiana.d2i.htrc.skmeans;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.common.ClassUtils;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.knn.means.StreamingKmeans;
import org.apache.mahout.knn.search.ProjectionSearch;
import org.apache.mahout.knn.search.Searcher;
import org.apache.mahout.knn.search.UpdatableSearcher;
import org.apache.mahout.math.MatrixSlice;
import org.apache.mahout.math.VectorWritable;

import edu.indiana.d2i.htrc.HTRCConstants;

class StreamingKMeansReducer extends
		Reducer<IntWritable, VectorWritable, Text, StreamingKMeansCluster> {
	private StreamingKMeansAdapter skmeans = null;
	private DistanceMeasure distance = null;
	
	@Override
	public void reduce(IntWritable key, Iterable<VectorWritable> values,
			Context context) throws IOException, InterruptedException {
		for (VectorWritable vectorWritable : values) {
			skmeans.cluster(vectorWritable.get());
		}

		Text identifier = new Text();
		StreamingKMeansCluster cluster = null;
		Searcher centroids = skmeans.getCentroids();
		for (MatrixSlice matrixSlice : centroids) {
			cluster = StreamingKMeansCluster.getStreamingKMeansCluster(
					matrixSlice.vector(), distance);
			identifier.set(cluster.getIdentifier());
			context.write(identifier, cluster);
		}
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// ??????
		distance =
		        ClassUtils.instantiateAs(context.getConfiguration().get(StreamingKMeansConfigKeys.DIST_MEASUREMENT), DistanceMeasure.class);
		skmeans = new StreamingKMeansAdapter(context.getConfiguration());
	}
}
