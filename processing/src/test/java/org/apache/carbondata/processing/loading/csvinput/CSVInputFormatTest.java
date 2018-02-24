/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.carbondata.processing.loading.csvinput;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Test;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CSVInputFormatTest extends TestCase {

  /**
   * generate compressed files, no need to call this method.
   * @throws Exception
   */
  public void generateCompressFiles() throws Exception {
    String pwd = new File("src/test/resources/csv").getCanonicalPath();
    String inputFile = pwd + "/data.csv";
    FileInputStream input = new FileInputStream(inputFile);
    Configuration conf = new Configuration();

    // .gz
    String outputFile = pwd + "/data.csv.gz";
    FileOutputStream output = new FileOutputStream(outputFile);
    GzipCodec gzip = new GzipCodec();
    gzip.setConf(conf);
    CompressionOutputStream outputStream = gzip.createOutputStream(output);
    int i = -1;
    while ((i = input.read()) != -1) {
      outputStream.write(i);
    }
    outputStream.close();
    input.close();

    // .bz2
    input = new FileInputStream(inputFile);
    outputFile = pwd + "/data.csv.bz2";
    output = new FileOutputStream(outputFile);
    BZip2Codec bzip2 = new BZip2Codec();
    bzip2.setConf(conf);
    outputStream = bzip2.createOutputStream(output);
    i = -1;
    while ((i = input.read()) != -1) {
      outputStream.write(i);
    }
    outputStream.close();
    input.close();

    // .snappy
    input = new FileInputStream(inputFile);
    outputFile = pwd + "/data.csv.snappy";
    output = new FileOutputStream(outputFile);
    SnappyCodec snappy = new SnappyCodec();
    snappy.setConf(conf);
    outputStream = snappy.createOutputStream(output);
    i = -1;
    while ((i = input.read()) != -1) {
      outputStream.write(i);
    }
    outputStream.close();
    input.close();

    //.lz4
    input = new FileInputStream(inputFile);
    outputFile = pwd + "/data.csv.lz4";
    output = new FileOutputStream(outputFile);
    Lz4Codec lz4 = new Lz4Codec();
    lz4.setConf(conf);
    outputStream = lz4.createOutputStream(output);
    i = -1;
    while ((i = input.read()) != -1) {
      outputStream.write(i);
    }
    outputStream.close();
    input.close();

  }

  /**
   * CSVCheckMapper check the content of csv files.
   */
  public static class CSVCheckMapper extends Mapper<NullWritable, StringArrayWritable, NullWritable,
      NullWritable> {
    @Override
    protected void map(NullWritable key, StringArrayWritable value, Context context)
        throws IOException, InterruptedException {
      String[] columns = value.get();
      int id = Integer.parseInt(columns[0]);
      int salary = Integer.parseInt(columns[6]);
      Assert.assertEquals(id - 1, salary - 15000);
    }
  }

  /**
   * test read csv files
   * @throws Exception
   */
  @Test public void testReadCSVFiles() throws Exception{
    Configuration conf = new Configuration();
    prepareConf(conf);
    File output = new File("target/output_CSVInputFormatTest");
    conf.set("mapreduce.cluster.local.dir", output.getCanonicalPath());
    Job job = Job.getInstance(conf, "CSVInputFormat_normal");
    job.setJarByClass(CSVInputFormatTest.class);
    job.setMapperClass(CSVCheckMapper.class);
    job.setNumReduceTasks(0);
    job.setInputFormatClass(CSVInputFormat.class);

    String inputFolder = new File("src/test/resources/csv").getCanonicalPath();
    FileInputFormat.addInputPath(job, new Path(inputFolder + File.separator + "data.csv"));
    FileInputFormat.addInputPath(job, new Path(inputFolder + File.separator + "data.csv.bz2"));
    FileInputFormat.addInputPath(job, new Path(inputFolder + File.separator + "data.csv.gz"));
    // FileInputFormat.addInputPath(job, new Path(inputFolder + File.separator + "data.csv.lz4"));
    // FileInputFormat.addInputPath(job, new Path(inputFolder + File.separator + "data.csv.snappy"));

    deleteOutput(output);
    FileOutputFormat.setOutputPath(job, new Path(output.getCanonicalPath()));

    Assert.assertTrue(job.waitForCompletion(true));
    deleteOutput(output);
  }

  private void prepareConf(Configuration conf) {
    conf.setBoolean(CSVInputFormat.HEADER_PRESENT, true);
    conf.set(CSVInputFormat.MAX_COLUMNS, "10");
    conf.set(CSVInputFormat.NUMBER_OF_COLUMNS, "7");
  }

  private void deleteOutput(File output) {
    if (output.exists()) {
      if (output.isDirectory()) {
        for(File file : output.listFiles()) {
          deleteOutput(file);
        }
        output.delete();
      } else {
        output.delete();
      }
    }
  }
}
