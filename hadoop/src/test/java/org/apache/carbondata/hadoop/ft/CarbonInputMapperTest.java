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
package org.apache.carbondata.hadoop.ft;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.hadoop.CarbonInputFormat;
import org.apache.carbondata.hadoop.CarbonProjection;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.LiteralExpression;
import org.apache.carbondata.core.scan.expression.conditional.EqualToExpression;
import org.apache.carbondata.hadoop.test.util.StoreCreator;

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.Assert;
import org.junit.Test;

public class CarbonInputMapperTest extends TestCase {

  // changed setUp to static init block to avoid un wanted multiple time store creation
  static {
    StoreCreator.createCarbonStore();
  }

  @Test public void testInputFormatMapperReadAllRowsAndColumns() throws Exception {
    try {
      String outPath = "target/output";
      CarbonProjection carbonProjection = new CarbonProjection();
      carbonProjection.addColumn("ID");
      carbonProjection.addColumn("date");
      carbonProjection.addColumn("country");
      carbonProjection.addColumn("name");
      carbonProjection.addColumn("phonetype");
      carbonProjection.addColumn("serialname");
      carbonProjection.addColumn("salary");
      runJob(outPath, carbonProjection, null);
      Assert.assertEquals("Count lines are not matching", 1000, countTheLines(outPath));
      Assert.assertEquals("Column count are not matching", 7, countTheColumns(outPath));
    } catch (Exception e) {
      Assert.assertTrue("failed", false);
      e.printStackTrace();
      throw e;
    }
  }

  @Test public void testInputFormatMapperReadAllRowsAndFewColumns() throws Exception {
    try {
      String outPath = "target/output2";
      CarbonProjection carbonProjection = new CarbonProjection();
      carbonProjection.addColumn("ID");
      carbonProjection.addColumn("country");
      carbonProjection.addColumn("salary");
      runJob(outPath, carbonProjection, null);

      Assert.assertEquals("Count lines are not matching", 1000, countTheLines(outPath));
      Assert.assertEquals("Column count are not matching", 3, countTheColumns(outPath));
    } catch (Exception e) {
      e.printStackTrace();
      Assert.assertTrue("failed", false);
    }
  }

  @Test public void testInputFormatMapperReadAllRowsAndFewColumnsWithFilter() throws Exception {
    try {
      String outPath = "target/output3";
      CarbonProjection carbonProjection = new CarbonProjection();
      carbonProjection.addColumn("ID");
      carbonProjection.addColumn("country");
      carbonProjection.addColumn("salary");
      Expression expression =
          new EqualToExpression(new ColumnExpression("country", DataType.STRING),
              new LiteralExpression("france", DataType.STRING));
      runJob(outPath, carbonProjection, expression);
      Assert.assertEquals("Count lines are not matching", 101, countTheLines(outPath));
      Assert.assertEquals("Column count are not matching", 3, countTheColumns(outPath));
    } catch (Exception e) {
      Assert.assertTrue("failed", false);
    }
  }

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

  public static class Map extends Mapper<Void, Object[], Void, Text> {

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
    }
  }

  private void runJob(String outPath, CarbonProjection projection, Expression filter)
      throws Exception {

    Job job = Job.getInstance(new Configuration());
    job.setJarByClass(CarbonInputMapperTest.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setMapperClass(Map.class);
    //    job.setReducerClass(WordCountReducer.class);
    job.setInputFormatClass(CarbonInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    AbsoluteTableIdentifier abs = StoreCreator.getAbsoluteTableIdentifier();
    if (projection != null) {
      CarbonInputFormat.setColumnProjection(job.getConfiguration(), projection);
    }
    if (filter != null) {
      CarbonInputFormat.setFilterPredicates(job.getConfiguration(), filter);
    }
    FileInputFormat.addInputPath(job, new Path(abs.getTablePath()));
    CarbonUtil.deleteFoldersAndFiles(new File(outPath + "1"));
    FileOutputFormat.setOutputPath(job, new Path(outPath + "1"));
    job.getConfiguration().set("outpath", outPath);
    boolean status = job.waitForCompletion(true);
  }

  public static void main(String[] args) throws Exception {
    new CarbonInputMapperTest().runJob("target/output", null, null);
  }
}
