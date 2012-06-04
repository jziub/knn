/*
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

package org.apache.mahout.knn.means;

import com.google.common.collect.Lists;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.knn.WeightedVector;
import org.apache.mahout.knn.generate.MultiNormal;
import org.apache.mahout.knn.search.Brute;
import org.apache.mahout.knn.search.ProjectionSearch;
import org.apache.mahout.knn.search.Searcher;
import org.apache.mahout.knn.search.UpdatableSearcher;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.MatrixSlice;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class PressureSKTest {
    @Test
    public static void testEstimateBeta() {
        Matrix m = new DenseMatrix(8, 3);
        for (int i = 0; i < 8; i++) {
            m.viewRow(i).assign(new double[]{0.125 * (i & 4), i & 2, i & 1});
        }
        Assert.assertEquals(0.5, StreamingKmeans.estimateCutoff(m, 100), 1e-9);
    }

    @Test
    public static void testClustering1(final int dim) {
        // construct data samplers centered on the corners of a unit cube
    	long t = System.currentTimeMillis();
        Matrix mean = new DenseMatrix(8, dim);
        List<MultiNormal> rowSamplers = Lists.newArrayList();
        for (int i = 0; i < 8; i++) {
//            mean.viewRow(i).assign(new double[]{0.25 * (i & 4), 0.5 * (i & 2), i & 1});
            
        	double[] random = new double[dim];
			for (int j = 0; j < random.length; j++) {
				random[j] = Math.random();
			}
        	rowSamplers.add(new MultiNormal(0.01, mean.viewRow(i)));
        }

        // sample a bunch of data points
        Matrix data = new DenseMatrix(10000, dim);
        for (MatrixSlice row : data) {
            row.vector().assign(rowSamplers.get(row.index() % 8).sample());
        }
        long t0 = System.currentTimeMillis();
        
        System.out.println("data generator takes " + (t0 - t)/1000.0 + " seconds");

        // cluster the data
        final EuclideanDistanceMeasure distance = new EuclideanDistanceMeasure();
        Searcher r = new StreamingKmeans().cluster(data, 1000, new StreamingKmeans.CentroidFactory() {
            @Override
            public UpdatableSearcher create() {
//                return new ProjectionSearch(dim, distance, 8, 20);
            	return new Brute(distance);
            }
        });
        long t1 = System.currentTimeMillis();

        assertEquals("Total weight not preserved", totalWeight(data), totalWeight(r), 1e-9);

        // and verify that each corner of the cube has a centroid very nearby
        for (MatrixSlice row : mean) {
            WeightedVector v = r.search(row.vector(), 1).get(0);
//            assertTrue(v.getWeight() < 0.05);
        }
        System.out.printf("%.2f seconds for clustering\n%.1f us per row\n",
                (t1 - t0) / 1000.0, (t1 - t0) / 1000.0 / data.rowSize() * 1e6);
    }

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
    
    public static void main(String[] args) {
		int dim = Integer.valueOf(args[0]);
    	testClustering1(dim);
	}
}
