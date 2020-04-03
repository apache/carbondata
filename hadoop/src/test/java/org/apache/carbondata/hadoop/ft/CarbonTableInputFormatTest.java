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
import java.io.FileFilter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.index.IndexFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.LiteralExpression;
import org.apache.carbondata.core.scan.expression.conditional.EqualToExpression;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.hadoop.CarbonProjection;
import org.apache.carbondata.hadoop.api.CarbonTableInputFormat;
import org.apache.carbondata.hadoop.testutil.StoreCreator;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.Assert;
import org.junit.Test;

public class CarbonTableInputFormatTest {
  // changed setUp to static init block to avoid un wanted multiple time store creation
  private static StoreCreator creator;

  private static CarbonLoadModel loadModel;
  static {
    CarbonProperties.getInstance().
        addProperty(CarbonCommonConstants.CARBON_BADRECORDS_LOC, "/tmp/carbon/badrecords");
    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_SYSTEM_FOLDER_LOCATION, "/tmp/carbon/");
    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_WRITTEN_BY_APPNAME, "CarbonTableInputFormatTest");
    try {
      creator = new StoreCreator(new File("target/store").getAbsolutePath(),
          new File("../hadoop/src/test/resources/data.csv").getCanonicalPath());
      loadModel = creator.createCarbonStore();
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail("create table failed: " + e.getMessage());
    }
  }

  @Test public void testGetFilteredSplits() throws Exception {
    CarbonTableInputFormat carbonInputFormat = new CarbonTableInputFormat();
    JobConf jobConf = new JobConf(new Configuration());
    Job job = Job.getInstance(jobConf);
    job.getConfiguration().set("query.id", UUID.randomUUID().toString());
    String tblPath = creator.getAbsoluteTableIdentifier().getTablePath();
    FileInputFormat.addInputPath(job, new Path(tblPath));
    CarbonTableInputFormat.setDatabaseName(job.getConfiguration(), creator.getAbsoluteTableIdentifier().getDatabaseName());
    CarbonTableInputFormat.setTableName(job.getConfiguration(), creator.getAbsoluteTableIdentifier().getTableName());
    Expression expression = new EqualToExpression(new ColumnExpression("country", DataTypes.STRING),
        new LiteralExpression("china", DataTypes.STRING));
    CarbonTableInputFormat.setFilterPredicates(job.getConfiguration(),
        new IndexFilter(loadModel.getCarbonDataLoadSchema().getCarbonTable(), expression));
    List splits = carbonInputFormat.getSplits(job);

    Assert.assertTrue(splits != null);
    Assert.assertTrue(!splits.isEmpty());
  }

  @Test
  public void testGetSplits() throws Exception {
    CarbonTableInputFormat carbonInputFormat = new CarbonTableInputFormat();
    JobConf jobConf = new JobConf(new Configuration());
    Job job = Job.getInstance(jobConf);
    job.getConfiguration().set("query.id", UUID.randomUUID().toString());
    String tblPath = creator.getAbsoluteTableIdentifier().getTablePath();
    FileInputFormat.addInputPath(job, new Path(tblPath));
    CarbonTableInputFormat.setDatabaseName(job.getConfiguration(), creator.getAbsoluteTableIdentifier().getDatabaseName());
    CarbonTableInputFormat.setTableName(job.getConfiguration(), creator.getAbsoluteTableIdentifier().getTableName());
    // list files to get the carbondata file
    String segmentPath = CarbonTablePath.getSegmentPath(creator.getAbsoluteTableIdentifier().getTablePath(), "0");
    File segmentDir = new File(segmentPath);
    if (segmentDir.exists() && segmentDir.isDirectory()) {
      File[] files = segmentDir.listFiles(new FileFilter() {
        @Override
        public boolean accept(File pathname) {
          return pathname.getName().endsWith("carbondata");
        }
      });
      if (files != null && files.length > 0) {
        job.getConfiguration().set(CarbonTableInputFormat.INPUT_FILES, files[0].getName());
      }
    }
    List splits = carbonInputFormat.getSplits(job);

    Assert.assertTrue(splits != null && splits.size() == 1);
  }

  @Test public void testInputFormatMapperReadAllRowsAndColumns() throws Exception {
    String outPath = "target/output";
    try {
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
      e.printStackTrace();
      Assert.assertTrue("failed", false);
      throw e;
    } finally {
      creator.clearDataMaps();
      FileFactory.deleteAllFilesOfDir(new File(outPath));
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
    } finally {
      creator.clearDataMaps();
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
          new EqualToExpression(new ColumnExpression("country", DataTypes.STRING),
              new LiteralExpression("france", DataTypes.STRING));
      runJob(outPath, carbonProjection, expression);
      Assert.assertEquals("Count lines are not matching", 101, countTheLines(outPath));
      Assert.assertEquals("Column count are not matching", 3, countTheColumns(outPath));
    } catch (Exception e) {
      Assert.assertTrue("failed", false);
    } finally {
      creator.clearDataMaps();
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

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      super.cleanup(context);
      fileWriter.close();
      context.write(new Text(), new Text());
    }
  }

  private void runJob(String outPath, CarbonProjection projection, Expression filter)
      throws Exception {

    Configuration configuration = new Configuration();
    configuration.set("mapreduce.cluster.local.dir", new File(outPath + "1").getCanonicalPath());
    Job job = Job.getInstance(configuration);
    job.setJarByClass(CarbonTableInputFormatTest.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setMapperClass(Map.class);
    job.setInputFormatClass(CarbonTableInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    AbsoluteTableIdentifier abs = creator.getAbsoluteTableIdentifier();
    if (projection != null) {
      CarbonTableInputFormat.setColumnProjection(job.getConfiguration(), projection);
    }
    if (filter != null) {
      CarbonTableInputFormat.setFilterPredicates(job.getConfiguration(),
          new IndexFilter(loadModel.getCarbonDataLoadSchema().getCarbonTable(), filter));
    }
    CarbonTableInputFormat.setDatabaseName(job.getConfiguration(),
        abs.getCarbonTableIdentifier().getDatabaseName());
    CarbonTableInputFormat.setTableName(job.getConfiguration(),
        abs.getCarbonTableIdentifier().getTableName());
    FileInputFormat.addInputPath(job, new Path(abs.getTablePath()));
    CarbonUtil.deleteFoldersAndFiles(new File(outPath + "1"));
    FileOutputFormat.setOutputPath(job, new Path(outPath + "1"));
    job.getConfiguration().set("outpath", outPath);
    job.getConfiguration().set("query.id", String.valueOf(System.nanoTime()));
    boolean status = job.waitForCompletion(true);
  }
}
