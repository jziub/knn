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
# File:  RandomProjection.java
# Description:  
#
# -----------------------------------------------------------------
# 
 */

package edu.indiana.d2i.htrc.vecproj;

import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.function.DoubleFunction;
import org.apache.mahout.math.function.Functions;

public class RandomProjection implements VectorProjectionIF {

	private int originalDim, reducedDim;
	private Matrix basis = null;
	private final DoubleFunction random; // (0.0, 1.0)

	class DoubleFunctionWrapper implements DoubleFunction {
		@Override
		public double apply(double arg1) {
			return -1 + random.apply(arg1) * 2; // (-1.0, 1.0)
		}
	}

	class RandomProjectionMeasurement extends EuclideanDistanceMeasure {
		private double scale;
		
		public RandomProjectionMeasurement(double scale) {
			this.scale = scale;
		}
		
		@Override
		public double distance(Vector v1, Vector v2) {
			return Math.sqrt(super.distance(v1, v2)) * scale;
		}

		@Override
		public double distance(double centroidLengthSquare, Vector centroid,
				Vector v) {
			return Math.sqrt(super.distance(centroidLengthSquare, centroid, v)) * scale;
		}
	}

	public RandomProjection(int originalDim /* d */, int reducedDim /* k */) {
		this.originalDim = originalDim;
		this.reducedDim = reducedDim;

		// generate random matrix, use a O(kd) algorithm [Achlioptas, 2000]
		random = Functions.random();
		DoubleFunctionWrapper randomWrapper = new DoubleFunctionWrapper();
		basis = new DenseMatrix(reducedDim, originalDim);
		for (int i = 0; i < reducedDim; i++) {
			DenseVector projection = new DenseVector(originalDim);
			projection.assign(randomWrapper);
			basis.assignRow(i, projection);
		}
	}

	@Override
	public DistanceMeasure getDistanceMeasurement() {
		return new RandomProjectionMeasurement(Math.pow((double)originalDim/reducedDim, 0.5));
	}

	@Override
	public Vector project(Vector v) {
		return basis.times(v);
	}
}
