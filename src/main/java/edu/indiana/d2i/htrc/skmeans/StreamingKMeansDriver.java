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
# File:  StreamingKmeansDriver.java
# Description:  
#
# -----------------------------------------------------------------
# 
 */

package edu.indiana.d2i.htrc.skmeans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.knn.means.StreamingKmeans;
import org.apache.mahout.math.MatrixSlice;
import org.apache.mahout.math.VectorWritable;

import edu.indiana.d2i.htrc.util.Utilities;

public class StreamingKMeansDriver extends Configured implements Tool {
	private static final Log logger = LogFactory
			.getLog(StreamingKMeansDriver.class);

	private final int samplesNum = 100;

	private void printUsage() {
		System.out.println("Bad input arguments!");
		System.exit(1);
	}

	private void StreamingKMeansConfigHelper(Configuration conf, String input,
			int maxCluster) throws IOException {
		// get samples to calculate scale factor
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] status = fs.listStatus(new Path(input),
				Utilities.HIDDEN_FILE_FILTER);
		int index = 0 + (int) (Math.random() * (status.length));
		SequenceFile.Reader seqReader = new SequenceFile.Reader(fs,
				status[index].getPath(), conf);

		int count = 0;
		Text key = new Text();
		VectorWritable value = new VectorWritable();
		List<MatrixSlice> slices = new ArrayList<MatrixSlice>();
		while (seqReader.next(key, value) && count < samplesNum) {
			MatrixSlice slice = new MatrixSlice(value.get().clone(), count);
			slices.add(slice);
			count++;
		}

		// set cutoff
		float cutoff = (float) StreamingKmeans.estimateCutoff(slices,
				samplesNum);
		conf.setFloat(StreamingKMeansConfigKeys.CUTOFF, cutoff);
		logger.info("Scale factor (cutoff) is: " + cutoff);

		// set vector dimension
		int dim = value.get().size();
		conf.setInt(StreamingKMeansConfigKeys.VECTOR_DIMENSION, dim);
		logger.info("Dimemsion of a vector is: " + dim);

		// set maximum #cluster
		conf.setInt(StreamingKMeansConfigKeys.MAXCLUSTER, maxCluster);

		// set distance measurement
		conf.set(StreamingKMeansConfigKeys.DIST_MEASUREMENT,
				EuclideanDistanceMeasure.class.getName());
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			printUsage();
		}

		String input = args[0];
		String output = args[1];
		int maxCluster = Integer.valueOf(args[2]);

		logger.info("StreamingKmeansDriver ");
		logger.info(" - input: " + input);
		logger.info(" - output: " + output);
		logger.info(" - maxCluster: " + maxCluster);

		// set job
		Job job = new Job(getConf(), "Streaming KMeans");
		job.setJarByClass(StreamingKMeansDriver.class);
		StreamingKMeansConfigHelper(job.getConfiguration(), input, maxCluster);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(VectorWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(StreamingKMeansCluster.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setMapperClass(StreamingKMeansMapper.class);
		job.setReducerClass(StreamingKMeansReducer.class);

		job.waitForCompletion(true);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new StreamingKMeansDriver(), args);
		System.exit(res);
	}
}
