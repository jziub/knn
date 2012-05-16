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
# File:  StreamingKMeansAdapter.java
# Description:  
#
# -----------------------------------------------------------------
# 
*/

package edu.indiana.d2i.htrc.skmeans;

import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.knn.Centroid;
import org.apache.mahout.knn.WeightedVector;
import org.apache.mahout.knn.means.StreamingKmeans;
import org.apache.mahout.knn.search.Searcher;
import org.apache.mahout.knn.search.UpdatableSearcher;
import org.apache.mahout.math.MatrixSlice;
import org.apache.mahout.math.VectorWritable;

import com.google.common.collect.Lists;

import edu.indiana.d2i.htrc.HTRCConstants;

/**
 * Slightly modify the original one to let it work in MapReduce context.
 */
public class StreamingKMeansAdapter extends StreamingKmeans {
	private CentroidFactory centroidFactory = null;
	private UpdatableSearcher centroids = null;
	private int maxClusters;
	private int numCluster = 0;
	
	public StreamingKMeansAdapter(Configuration conf, CentroidFactory centroidFactory) {
		float cutoff = conf.getFloat(
				HTRCConstants.STREAMING_KMEANS_CUTOFF, 0);
		int maxClusters = conf.getInt(
				HTRCConstants.STREAMING_KMEANS_MAXCLUSTER, 0);
		if (cutoff == 0 || maxClusters == 0)
			throw new RuntimeException(
					"Illegal parameters for streaming kmeans, cutoff: "
							+ cutoff + ", maxClusters: " + maxClusters);
		
		this.maxClusters = maxClusters;
		this.distanceCutoff = cutoff;
		this.centroidFactory = centroidFactory;
		this.centroids = centroidFactory.create();
	}
	
	public Searcher getCentroids() {
		return centroids;
	}
	
	public void cluster(VectorWritable vector) {
		if (centroids.getSearchSize() == 0) {
			centroids.add(Centroid.create(0, vector.get()), 0);
		}
		else {
			Random rand = RandomUtils.getRandom();
			
			// estimate distance d to closest centroid
            WeightedVector closest = centroids.search(vector.get(), 1).get(0);
            
            if (rand.nextDouble() < closest.getWeight() / distanceCutoff) {
                // add new centroid, note that the vector is copied because we may mutate it later
                centroids.add(Centroid.create(centroids.size(), vector.get()), centroids.size());
            } else {
                // merge against existing
                Centroid c = (Centroid) closest.getVector();
                centroids.remove(c);
                c.update(vector.get());
                centroids.add(c, c.getIndex());
            }
		}
		
		if (centroids.size() > maxClusters) {
            maxClusters = (int) Math.max(maxClusters, 10 * Math.log(numCluster));
            // TODO does shuffling help?
            List<MatrixSlice> shuffled = Lists.newArrayList(centroids);
            Collections.shuffle(shuffled);
            centroids = clusterInternal(shuffled, maxClusters, 1, centroidFactory);

            // in the original algorithm, with distributions with sharp scale effects, the
            // distanceCutoff can grow to excessive size leading sub-clustering to collapse
            // the centroids set too much. This test prevents increase in distanceCutoff
            // the current value is doing fine at collapsing the clusters.
            if (centroids.size() > 0.2 * maxClusters) {
                distanceCutoff *= BETA;
            }
        }
		
		numCluster++;
	}
}
