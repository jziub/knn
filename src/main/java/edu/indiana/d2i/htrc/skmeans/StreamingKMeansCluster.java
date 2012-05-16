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
# File:  StreamingKMeansCluster.java
# Description:  
#
# -----------------------------------------------------------------
# 
 */

package edu.indiana.d2i.htrc.skmeans;

import org.apache.mahout.clustering.DistanceMeasureCluster;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.math.Vector;

public class StreamingKMeansCluster extends DistanceMeasureCluster {
	private static StreamingKMeansCluster cluster = null;
	private static int id = 1;

	public StreamingKMeansCluster(Vector point, int id, DistanceMeasure measure) {
		super(point, id, measure);
	}

	@Override
	public String getIdentifier() {
		return "SK:" + getId();
	}

	public static StreamingKMeansCluster getStreamingKMeansCluster(Vector point,
			DistanceMeasure measure) {
		if (cluster == null) {
			cluster = new StreamingKMeansCluster(point, id++, measure);
		}
		else {
			cluster.setCenter(point);
			cluster.setId(id++);
		}
			
		return cluster;
	}
}
