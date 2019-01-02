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

package org.apache.carbondata.streaming;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.UUID;

import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.hadoop.testutil.StoreCreator;
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.Assert;
import org.junit.Test;

public class CarbonStreamOutputFormatTest extends TestCase {

  private Configuration hadoopConf;
  private TaskAttemptID taskAttemptId;
  private CarbonLoadModel carbonLoadModel;
  private String tablePath;

  @Override protected void setUp() throws Exception {
    super.setUp();
    JobID jobId = CarbonInputFormatUtil.getJobId(new Date(), 0);
    TaskID taskId = new TaskID(jobId, TaskType.MAP, 0);
    taskAttemptId = new TaskAttemptID(taskId, 0);

    hadoopConf = new Configuration();
    hadoopConf.set("mapred.job.id", jobId.toString());
    hadoopConf.set("mapred.tip.id", taskAttemptId.getTaskID().toString());
    hadoopConf.set("mapred.task.id", taskAttemptId.toString());
    hadoopConf.setBoolean("mapred.task.is.map", true);
    hadoopConf.setInt("mapred.task.partition", 0);

    tablePath = new File("target/stream_output").getCanonicalPath();
    String dbName = "default";
    String tableName = "stream_table_output";
    AbsoluteTableIdentifier identifier =
        AbsoluteTableIdentifier.from(
            tablePath,
            new CarbonTableIdentifier(dbName, tableName, UUID.randomUUID().toString()));

    CarbonTable table = new StoreCreator(new File("target/store").getAbsolutePath(),
        new File("../hadoop/src/test/resources/data.csv").getCanonicalPath()).createTable(identifier);

    String factFilePath = new File("../hadoop/src/test/resources/data.csv").getCanonicalPath();
    carbonLoadModel = StoreCreator.buildCarbonLoadModel(table, factFilePath, identifier);
  }

  @Test public void testSetCarbonLoadModel() {
    try {
      CarbonStreamOutputFormat.setCarbonLoadModel(hadoopConf, carbonLoadModel);
    } catch (IOException e) {
      Assert.assertTrue("Failed to config CarbonLoadModel for CarbonStreamOutputFromat", false);
    }
  }

  @Test public void testGetCarbonLoadModel() {
    try {
      CarbonStreamOutputFormat.setCarbonLoadModel(hadoopConf, carbonLoadModel);
      CarbonLoadModel model = CarbonStreamOutputFormat.getCarbonLoadModel(hadoopConf);

      Assert.assertNotNull("Failed to get CarbonLoadModel", model);
      Assert.assertTrue("CarbonLoadModel should be same with previous",
          carbonLoadModel.getFactTimeStamp() == model.getFactTimeStamp());

    } catch (IOException e) {
      Assert.assertTrue("Failed to get CarbonLoadModel for CarbonStreamOutputFromat", false);
    }
  }

  @Test public void testGetRecordWriter() {
    CarbonStreamOutputFormat outputFormat = new CarbonStreamOutputFormat();
    try {
      CarbonStreamOutputFormat.setCarbonLoadModel(hadoopConf, carbonLoadModel);
      TaskAttemptContext taskAttemptContext =
          new TaskAttemptContextImpl(hadoopConf, taskAttemptId);
      RecordWriter recordWriter = outputFormat.getRecordWriter(taskAttemptContext);
      Assert.assertNotNull("Failed to get CarbonStreamRecordWriter", recordWriter);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.assertTrue(e.getMessage(), false);
    }
  }

  @Override protected void tearDown() throws Exception {
    super.tearDown();
    if (tablePath != null) {
      FileFactory.deleteAllFilesOfDir(new File(tablePath));
    }
  }
}
