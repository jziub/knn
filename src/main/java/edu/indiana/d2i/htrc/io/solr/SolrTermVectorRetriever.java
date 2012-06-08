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
# File:  SolrTermVectorRetriever.java
# Description:  
#
# -----------------------------------------------------------------
# 
 */

package edu.indiana.d2i.htrc.io.solr;

import java.util.Iterator;

import edu.indiana.d2i.htrc.HTRCConstants;

public class SolrTermVectorRetriever {
	class SolrSolrTermVectorIterator implements Iterator<SolrTermVector> {

		private SolrClient client;
		private Iterator<SolrTermVector> internalIterator;
		private int length = 0;
		private int end = 0;
		private final int gap = 10;
		
		private String[] getPartialIds() { 
			if (end >= length) return null;
			
			String[] idlist = ((end+gap) > length) ? 
					new String[length-end] : new String[gap];
			System.arraycopy(ids, end, idlist, 0, idlist.length);
			end += idlist.length + 1;
			return idlist;
		}
		
		public SolrSolrTermVectorIterator() {
			client = new SolrClient("http://chinkapin.pti.indiana.edu:9994/solr/");
			length = ids.length;
			end = gap;
		}
		
		@Override
		public boolean hasNext() {
			if (!internalIterator.hasNext()) return false;
			
			String[] idlist = getPartialIds();
			internalIterator = client.getTermVectors(idlist).iterator();
			
			if (internalIterator == null) return false;
			else if (!internalIterator.hasNext()) return false;
			else return true;
		}

		@Override
		public SolrTermVector next() {
			return internalIterator.next();
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("UnsupportedOperationException");
		}
	}

	class SolrSolrTermVectorIterable implements Iterable<SolrTermVector> {
		@Override
		public Iterator<SolrTermVector> iterator() {
			return new SolrSolrTermVectorIterator();
		}
	}

	private String[] ids;
	
	public Iterable<SolrTermVector> getTermVectors(String idStr) {
		this.ids = idStr.split(HTRCConstants.SOLR_DELIMITOR);
		return new SolrSolrTermVectorIterable();
	}

}
