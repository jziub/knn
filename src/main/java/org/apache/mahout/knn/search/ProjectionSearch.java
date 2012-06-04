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

package org.apache.mahout.knn.search;

import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.knn.WeightedVector;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.MatrixSlice;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.function.DoubleFunction;
import org.apache.mahout.math.function.Functions;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

/**
 * Does approximate nearest neighbor dudes search by projecting the data.
 */
public class ProjectionSearch extends UpdatableSearcher implements Iterable<MatrixSlice> {
    private final List<TreeSet<WeightedVector>> vectors;

    private DistanceMeasure distance;
    private List<Vector> basis;
    private int searchSize;

    public ProjectionSearch(int d, DistanceMeasure distance, int projections, int searchSize) {
        this.searchSize = searchSize;
        Preconditions.checkArgument(projections > 0 && projections < 100, "Unreasonable value for number of projections");

        final DoubleFunction random = Functions.random();

        this.distance = distance;
        vectors = Lists.newArrayList();
        basis = Lists.newArrayList();

        // we want to create several projections.  Each is alike except for the
        // direction of the projection
        for (int i = 0; i < projections; i++) {
            // create a random vector to use for the basis of the projection
            final DenseVector projection = new DenseVector(d);
            projection.assign(random);
            projection.normalize();

            basis.add(projection);

            // the projection is implemented by a tree set where the ordering of vectors
            // is based on the dot product of the vector with the projection vector
            vectors.add(Sets.<WeightedVector>newTreeSet());
        }
    }

    /**
     * Adds a vector into the set of projections for later searching.
     * @param v  The vector to add.
     * @param index   An integer for tracking which vector is which
     */
    public void add(Vector v, int index) {
        // add to each projection separately
        Iterator<Vector> projections = basis.iterator();
        for (TreeSet<WeightedVector> s : vectors) {
            s.add(WeightedVector.project(v, projections.next(), index));
        }
    }

    /**
     * Returns the number of vectors that we can search
     * @return  The number of vectors added to the search so far.
     */
    public int size() {
        return vectors.get(0).size();
    }

    public List<WeightedVector> search(final Vector query, int n) {
        Multiset<WeightedVector> candidates = HashMultiset.create();
        Iterator<Vector> projections = basis.iterator();
        for (TreeSet<WeightedVector> v : vectors) {
            WeightedVector projectedQuery = WeightedVector.project(query, projections.next());
            for (WeightedVector candidate : Iterables.limit(v.tailSet(projectedQuery, true), searchSize)) {
                candidates.add(new WeightedVector(candidate.getVector(), 0, candidate.getIndex()));
            }
            for (WeightedVector candidate : Iterables.limit(v.headSet(projectedQuery, false).descendingSet(), searchSize)) {
                candidates.add(new WeightedVector(candidate.getVector(), 0, candidate.getIndex()));
            }
        }

        // if searchSize * vectors.size() is small enough not to cause much memory pressure, this is probably
        // just as fast as a priority queue here.
        List<WeightedVector> top = Lists.newArrayList();
        for (WeightedVector candidate : candidates.elementSet()) {
            candidate.setWeight(distance.distance(query, candidate));
            top.add(candidate);
        }
        Collections.sort(top);
        return top.subList(0, n);
    }

    public Collection<WeightedVector> getVectors() {
        return vectors.get(0);
    }

    @Override
    public int getSearchSize() {
        return searchSize;
    }

    @Override
    public void setSearchSize(int size) {
        searchSize = size;
    }

    @Override
    public Iterator<MatrixSlice> iterator() {
        return new AbstractIterator<MatrixSlice>() {
            int index = 0;
            Iterator<WeightedVector> data = vectors.get(0).iterator();

            @Override
            protected MatrixSlice computeNext() {
                if (!data.hasNext()) {
                    return endOfData();
                } else {
                    return new MatrixSlice(data.next().getVector(), index++);
                }
            }
        };
    }

    public boolean remove(Vector vector) {
        List<WeightedVector> x = search(vector, 1);
        if (x.get(0).getWeight() < 1e-7) {
            Iterator<Vector> basisVectors = basis.iterator();
            for (TreeSet<WeightedVector> projection : vectors) {
                WeightedVector v = new WeightedVector(vector, basisVectors.next(), -1);
                boolean r = projection.remove(v);
                if (!r) {
                    throw new RuntimeException("Internal inconsistency in ProjectionSearch");
                }
            }
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void clear() {
        for (TreeSet<WeightedVector> set : vectors) {
            set.clear();
        }
    }
}
