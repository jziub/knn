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
# File:  StreamingKMeansAdapterTest.java
# Description:  
#
# -----------------------------------------------------------------
# 
 */

package edu.indiana.d2i.htrc.skmeans;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.mahout.common.ClassUtils;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.knn.WeightedVector;
import org.apache.mahout.knn.generate.MultiNormal;
import org.apache.mahout.knn.means.StreamingKmeans;
import org.apache.mahout.knn.means.StreamingKmeans.CentroidFactory;
import org.apache.mahout.knn.search.ProjectionSearch;
import org.apache.mahout.knn.search.Searcher;
import org.apache.mahout.knn.search.UpdatableSearcher;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.MatrixSlice;
import org.junit.Test;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public class StreamingKMeansAdapterTest {

	private static double totalWeight(Iterable<MatrixSlice> data) {
		double sum = 0;
		for (MatrixSlice row : data) {
			if (row.vector() instanceof WeightedVector) {
				sum += ((WeightedVector) row.vector()).getWeight();
			} else {
				sum++;
			}
		}
		return sum;
	}

	@Test
	public static void testCluster() {
		int dimension = 500;
		
		// construct data samplers centered on the corners of a unit cube
		Matrix mean = new DenseMatrix(8, dimension);
		List<MultiNormal> rowSamplers = Lists.newArrayList();
		for (int i = 0; i < 8; i++) {
//			mean.viewRow(i).assign(
//					new double[] { 0.25 * (i & 4), 0.5 * (i & 2), i & 1 });
			
			double[] random = new double[dimension];
			for (int j = 0; j < random.length; j++) {
				random[j] = Math.random();
			}
			mean.viewRow(i).assign(random);
			rowSamplers.add(new MultiNormal(0.01, mean.viewRow(i)));
		}

		// sample a bunch of data points
		Matrix data = new DenseMatrix(10000, dimension);
		for (MatrixSlice row : data) {
			row.vector().assign(rowSamplers.get(row.index() % 8).sample());
		}

		// cluster the data
		long t0 = System.currentTimeMillis();

		double cutoff = StreamingKMeansAdapter.estimateCutoff(data, 100);
		Configuration conf = new Configuration();
		conf.setInt(StreamingKMeansConfigKeys.MAXCLUSTER, 1000);
		conf.setFloat(StreamingKMeansConfigKeys.CUTOFF, (float) cutoff);
		conf.setClass(StreamingKMeansConfigKeys.DIST_MEASUREMENT,
				EuclideanDistanceMeasure.class, DistanceMeasure.class);
		conf.setInt(StreamingKMeansConfigKeys.VECTOR_DIMENSION, dimension);
		StreamingKMeansAdapter skmeans = new StreamingKMeansAdapter(conf);
		// for (MatrixSlice row : Iterables.skip(data, 1)) {
		// skmeans.cluster(row.vector());
		// }
		for (MatrixSlice row : data) {
			skmeans.cluster(row.vector());
		}

		// validate
		Searcher r = skmeans.getCentroids();

		// StreamingKMeansAdapter skmeans = new StreamingKMeansAdapter();
		// Searcher r = skmeans.cluster(data, 1000, centroidFactory);

		long t1 = System.currentTimeMillis();

		assertEquals("Total weight not preserved", totalWeight(data),
				totalWeight(r), 1e-9);

		// and verify that each corner of the cube has a centroid very nearby
		for (MatrixSlice row : mean) {
			WeightedVector v = r.search(row.vector(), 1).get(0);
			assertTrue(v.getWeight() < 0.05);
		}
		System.out.printf("%.2f for clustering\n%.1f us per row\n",
				(t1 - t0) / 1000.0, (t1 - t0) / 1000.0 / data.rowSize() * 1e6);
		
		System.out.println("Done??");
	}

	public static void main(String[] args) {
		testCluster();
	}
}
