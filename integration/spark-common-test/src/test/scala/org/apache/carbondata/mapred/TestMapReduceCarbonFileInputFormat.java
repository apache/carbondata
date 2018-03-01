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

package org.apache.carbondata.mapred;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.hadoop.CarbonProjection;
import org.apache.carbondata.hadoop.api.CarbonFileInputFormat;
import org.apache.carbondata.hadoop.api.CarbonTableInputFormat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Assert;
import org.junit.Test;

public class TestMapReduceCarbonFileInputFormat {

  private static final Log LOG = LogFactory.getLog(TestMapReduceCarbonFileInputFormat.class);

  private int countTheLines(String outPath) throws Exception {
    File file = new File(outPath);
    if (file.exists()) {
      BufferedReader reader = new BufferedReader(new FileReader(file));
      int i = 0;
      while (reader.readLine() != null) {
        i++;
      }
      reader.close();
      return i;
    }
    return 0;
  }

  private int countTheColumns(String outPath) throws Exception {
    File file = new File(outPath);
    if (file.exists()) {
      BufferedReader reader = new BufferedReader(new FileReader(file));
      String[] split = reader.readLine().split(",");
      reader.close();
      return split.length;
    }
    return 0;
  }

  @Test public void testInputFormatMapperReadAllRowsAndColumns() throws Exception {
    try {
      String outPath = "target/output";
      CarbonProjection carbonProjection = new CarbonProjection();
      carbonProjection.addColumn("name");
      carbonProjection.addColumn("age");
      runJob(outPath, carbonProjection, null, "sdkOutputTable", "default",
          "./src/test/resources/carbonFileLevelFormat/WriterOutput/");
      Assert.assertEquals("Count lines are not matching", 100, countTheLines(outPath));
      Assert.assertEquals("Column count are not matching", 2, countTheColumns(outPath));
    } catch (Exception e) {
      e.printStackTrace();
      Assert.assertTrue("failed", false);
      throw e;
    } finally {
    }
  }

  private void runJob(String outPath, CarbonProjection projection, Expression filter,
      String tableName, String databaseName, String tablePath) throws Exception {

    Configuration configuration = new Configuration();
    configuration.set("mapreduce.cluster.local.dir", new File(outPath + "1").getCanonicalPath());
    Job job = Job.getInstance(configuration);
    job.setJarByClass(CarbonFileInputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setMapperClass(Map.class);
    job.setInputFormatClass(CarbonFileInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    //    AbsoluteTableIdentifier abs = StoreCreator.getAbsoluteTableIdentifier();
    if (projection != null) {
      CarbonTableInputFormat.setColumnProjection(job.getConfiguration(), projection);
    }
    if (filter != null) {
      CarbonTableInputFormat.setFilterPredicates(job.getConfiguration(), filter);
    }
    CarbonTableInputFormat.setDatabaseName(job.getConfiguration(), databaseName);
    CarbonTableInputFormat.setTableName(job.getConfiguration(), tableName);
    org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, new Path(tablePath));
    CarbonUtil.deleteFoldersAndFiles(new File(outPath + "1"));
    FileOutputFormat.setOutputPath(job, new Path(outPath + "1"));
    job.getConfiguration().set("outpath", outPath);
    job.getConfiguration().set("query.id", String.valueOf(System.nanoTime()));
    boolean status = job.waitForCompletion(true);
  }


  @Test public void testGetSplits() throws Exception {
    CarbonFileInputFormat carbonFileInputFormat = new CarbonFileInputFormat();
    JobConf jobConf = new JobConf(new Configuration());
    Job job = Job.getInstance(jobConf);
    job.getConfiguration().set("query.id", UUID.randomUUID().toString());
    org.apache.hadoop.mapreduce.lib.input.FileInputFormat
        .addInputPath(job, new Path("./src/test/resources/carbonFileLevelFormat/WriterOutput/"));
    CarbonTableInputFormat.setDatabaseName(job.getConfiguration(), "default");
    CarbonTableInputFormat.setTableName(job.getConfiguration(), "sdkOutputTable");
    // list files to get the carbondata file
    String segmentPath = CarbonTablePath
        .getSegmentPath("./src/test/resources/carbonFileLevelFormat/WriterOutput/Fact" + "/Part0/",
            "null");
    File segmentDir = new File(segmentPath);
    if (segmentDir.exists() && segmentDir.isDirectory()) {
      File[] files = segmentDir.listFiles(new FileFilter() {
        @Override public boolean accept(File pathname) {
          return pathname.getName().endsWith("carbondata");
        }
      });
      if (files != null && files.length > 0) {
        job.getConfiguration().set(CarbonTableInputFormat.INPUT_FILES, files[0].getName());
      }
    }
    List splits = carbonFileInputFormat.getSplits(job);
    Assert.assertTrue(splits != null && splits.size() == 1);
  }

  public static class Map extends Mapper<Void, Object[], Text, Text> {

    private BufferedWriter fileWriter;

    public void setup(Context context) throws IOException, InterruptedException {
      String outPath = context.getConfiguration().get("outpath");
      File outFile = new File(outPath);
      try {
        fileWriter = new BufferedWriter(new FileWriter(outFile));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    public void map(Void key, Object[] value, Context context) throws IOException {
      StringBuilder builder = new StringBuilder();
      for (int i = 0; i < value.length; i++) {
        builder.append(value[i]).append(",");
      }
      fileWriter.write(builder.toString().substring(0, builder.toString().length() - 1));
      fileWriter.newLine();
    }

    @Override public void cleanup(Context context) throws IOException, InterruptedException {
      super.cleanup(context);
      fileWriter.close();
      context.write(new Text(), new Text());
    }
  }
}
