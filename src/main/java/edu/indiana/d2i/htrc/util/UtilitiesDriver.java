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
# File:  UtilitiesDriver.java
# Description:  
#
# -----------------------------------------------------------------
# 
*/

package edu.indiana.d2i.htrc.util;

import org.apache.hadoop.util.ProgramDriver;

public class UtilitiesDriver {
	public static void main(String[] args) {
		int exitCode = -1;
	    ProgramDriver pgd = new ProgramDriver();
	    try {
	    	pgd.addClass("inspectvector", VectorInspection.class, "inspect vector");
	    	pgd.addClass("inspecttoken", TokensInspection.class, "inspect token");
	    	pgd.addClass("inspectcluster", ClusterInspection.class, "inspect cluster");
	    	pgd.addClass("datavalid", DataCopyValidation.class, "validate copied data");
	    	pgd.addClass("dataapitest", DataAPITestDriver.class, "test data api");
	    	pgd.addClass("idvalid", IDValidation.class, "validate id list");
	    	pgd.addClass("dict", DictionaryCreate.class, "create dictionary");
	    	pgd.addClass("lucene", TestLuceneClient.class, "test lucene client");
	    	pgd.driver(args);
	    	
	    	// Success
	        exitCode = 0;
	    } catch(Throwable e){
	        e.printStackTrace();
	    }
	    
	    System.exit(exitCode);
	}
}
