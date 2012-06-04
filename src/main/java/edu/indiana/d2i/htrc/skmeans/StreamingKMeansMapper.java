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
# File:  StreamingKMeansMapper.java
# Description:  
#
# -----------------------------------------------------------------
# 
 */

package edu.indiana.d2i.htrc.skmeans;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.clustering.kmeans.KMeansConfigKeys;
import org.apache.mahout.common.ClassUtils;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.knn.means.StreamingKmeans;
import org.apache.mahout.knn.search.ProjectionSearch;
import org.apache.mahout.knn.search.UpdatableSearcher;
import org.apache.mahout.math.MatrixSlice;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import edu.indiana.d2i.htrc.HTRCConstants;
import edu.indiana.d2i.htrc.vecproj.VectorProjectionIF;

class StreamingKMeansMapper
		extends
		Mapper<WritableComparable<?>, VectorWritable, IntWritable, VectorWritable> {
	private StreamingKMeansAdapter skmeans = null;
	private VectorProjectionIF projector = null;

	@Override
	public void map(WritableComparable<?> key, VectorWritable value,
			Context context) throws IOException, InterruptedException {
//		skmeans.cluster(value.get());
		
		Vector vector = projector.project(value.get());
		skmeans.cluster(vector);
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		skmeans = new StreamingKMeansAdapter(context.getConfiguration());
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		// flush the centroids
		IntWritable identifier = new IntWritable(1); 
		VectorWritable centroid = new VectorWritable();
		for (MatrixSlice slice : skmeans.getCentroids()) {
			centroid.set(slice.vector());
			context.write(identifier, centroid);
		}
	}
}
