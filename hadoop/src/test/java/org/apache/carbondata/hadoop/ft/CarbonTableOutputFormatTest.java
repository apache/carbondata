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

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.hadoop.api.CarbonTableOutputFormat;
import org.apache.carbondata.hadoop.internal.ObjectArrayWritable;
import org.apache.carbondata.hadoop.testutil.StoreCreator;
import org.apache.carbondata.processing.loading.csvinput.CSVInputFormat;
import org.apache.carbondata.processing.loading.csvinput.StringArrayWritable;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CarbonTableOutputFormatTest {

  static CarbonLoadModel carbonLoadModel;

  // changed setUp to static init block to avoid un wanted multiple time store creation
  static {
    CarbonProperties.getInstance().
        addProperty(CarbonCommonConstants.CARBON_BADRECORDS_LOC, "/tmp/carbon/badrecords");
    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_SYSTEM_FOLDER_LOCATION, "/tmp/carbon/");
    try {
      carbonLoadModel = new StoreCreator(new File("target/store").getAbsolutePath(),
          new File("../hadoop/src/test/resources/data.csv").getCanonicalPath()).createTableAndLoadModel();
    } catch (Exception e) {
      Assert.fail("create table failed: " + e.getMessage());
    }
  }


  @Test public void testOutputFormat() throws Exception {
    runJob("");
    String segmentPath = CarbonTablePath.getSegmentPath(carbonLoadModel.getTablePath(), "0");
    File file = new File(segmentPath);
    Assert.assertTrue(file.exists());
    File[] listFiles = file.listFiles(new FilenameFilter() {
      @Override public boolean accept(File dir, String name) {
        return name.endsWith(".carbondata") ||
            name.endsWith(".carbonindex") ||
            name.endsWith(".carbonindexmerge");
      }
    });

    Assert.assertTrue(listFiles.length == 2);
  }

  @After
  public void tearDown() throws Exception {
    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.ENABLE_QUERY_STATISTICS, "true");
  }

  @Before
  public void setUp() throws Exception {
    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.ENABLE_QUERY_STATISTICS, "false");
  }

 public static class Map extends Mapper<NullWritable, StringArrayWritable, NullWritable, ObjectArrayWritable> {

   private ObjectArrayWritable writable = new ObjectArrayWritable();
   @Override protected void map(NullWritable key, StringArrayWritable value, Context context)
       throws IOException, InterruptedException {
     writable.set(value.get());
     context.write(key, writable);
   }
 }

  private void runJob(String outPath) throws Exception {
    Configuration configuration = new Configuration();
    String mrLocalDir = new File(outPath + "1").getCanonicalPath();
    configuration.set("mapreduce.cluster.local.dir", mrLocalDir);
    Job job = Job.getInstance(configuration);
    job.setJarByClass(CarbonTableOutputFormatTest.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(ObjectArrayWritable.class);
    job.setMapperClass(Map.class);
    job.setNumReduceTasks(0);

    FileInputFormat.addInputPath(job, new Path(carbonLoadModel.getFactFilePath()));
    CarbonTableOutputFormat.setLoadModel(job.getConfiguration(), carbonLoadModel);
    CarbonTableOutputFormat.setCarbonTable(job.getConfiguration(), carbonLoadModel.getCarbonDataLoadSchema().getCarbonTable());
    CSVInputFormat.setHeaderExtractionEnabled(job.getConfiguration(), true);
    job.setInputFormatClass(CSVInputFormat.class);
    job.setOutputFormatClass(CarbonTableOutputFormat.class);
    CarbonUtil.deleteFoldersAndFiles(new File(carbonLoadModel.getTablePath() + "1"));
    FileOutputFormat.setOutputPath(job, new Path(carbonLoadModel.getTablePath() + "1"));
    job.getConfiguration().set("outpath", outPath);
    job.getConfiguration().set("query.id", String.valueOf(System.nanoTime()));
    job.waitForCompletion(true);

    CarbonUtil.deleteFoldersAndFiles(new File(mrLocalDir));
  }

}
