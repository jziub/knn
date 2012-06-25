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
# File:  HadoopWritableTranscoder.java
# Description:  
#
# -----------------------------------------------------------------
# 
*/

package edu.indiana.d2i.htrc.io.mem;

import java.io.IOException;

import net.spy.memcached.CachedData;
import net.spy.memcached.transcoders.Transcoder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

public class HadoopWritableTranscoder<T extends Writable> implements Transcoder<T> {

//	private static final int COMPRESSED=2;
//	private Class<?> writableClass;
	
	private T writable;
	private DataOutputBuffer encodeBuffer = new DataOutputBuffer();
	private DataInputBuffer decodeBuffer = new DataInputBuffer(); 
	
	@SuppressWarnings("unchecked")
	public HadoopWritableTranscoder(Configuration conf, Class<?> writableClass) {
//		this.writableClass = writableClass;
		writable = (T) ReflectionUtils.newInstance(writableClass, conf);
	}
	
	@Override
	public boolean asyncDecode(CachedData arg0) {
		return false;
	}

	@Override
	public T decode(CachedData data) {
		try {
			byte[] bytes = data.getData();			
			decodeBuffer.reset(bytes, bytes.length);
			writable.readFields(decodeBuffer);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return writable;
	}

	@Override
	public CachedData encode(T obj) {
		CachedData data = null;
		try {
			obj.write(encodeBuffer);
			byte[] bytes = encodeBuffer.getData();
			data = new CachedData(0, bytes, bytes.length);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return data;
	}

	@Override
	public int getMaxSize() {
		// not in use
		return Integer.MAX_VALUE;
	}

}
