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
# File:  TestRandomProjection.java
# Description:  
#
# -----------------------------------------------------------------
# 
*/

package edu.indiana.d2i.htrc.vecproj;

import static org.junit.Assert.*;
import junit.framework.Assert;

import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.function.DoubleFunction;
import org.apache.mahout.math.function.Functions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestRandomProjection {

	private int originalDim = 10000, reducedDim = 500;
	private VectorProjectionIF projector = null;
	
	private DoubleFunction random;
	
	@Before
	public void setUp() throws Exception {
		projector = new RandomProjection(originalDim, reducedDim);
		
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testGetDistanceMeasurement() {
//		fail("Not yet implemented");
	}

	@Test
	public void testProject() {
		random = Functions.random();
		for (int i = 0; i < 100; i++) { // test 100 times
			DenseVector vector = new DenseVector(originalDim);
			vector.assign(random);
			Vector projected = projector.project(vector);
			Assert.assertEquals(reducedDim, projected.size());
		}
	}
}
