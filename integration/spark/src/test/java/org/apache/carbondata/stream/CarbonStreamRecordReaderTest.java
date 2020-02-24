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

package org.apache.carbondata.stream;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.statusmanager.FileFormat;
import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.hadoop.CarbonMultiBlockSplit;
import org.apache.carbondata.hadoop.stream.CarbonStreamInputFormat;
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil;

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.Assert;
import org.junit.Test;

public class CarbonStreamRecordReaderTest extends TestCase {

  private TaskAttemptID taskAttemptId;
  private TaskAttemptContext taskAttemptContext;
  private Configuration hadoopConf;
  private AbsoluteTableIdentifier identifier;
  private String tablePath;


  @Override
  protected void setUp() throws Exception {
    tablePath = new File("target/stream_input").getCanonicalPath();
    String dbName = "default";
    String tableName = "stream_table_input";
    identifier = AbsoluteTableIdentifier.from(
        tablePath,
        new CarbonTableIdentifier(dbName, tableName, UUID.randomUUID().toString()));

    JobID jobId = CarbonInputFormatUtil.getJobId(new Date(), 0);
    TaskID taskId = new TaskID(jobId, TaskType.MAP, 0);
    taskAttemptId = new TaskAttemptID(taskId, 0);

    hadoopConf = new Configuration();
    taskAttemptContext = new TaskAttemptContextImpl(hadoopConf, taskAttemptId);
  }

  private InputSplit buildInputSplit() throws IOException {
    CarbonInputSplit carbonInputSplit = new CarbonInputSplit();
    List<CarbonInputSplit> splitList = new ArrayList<>();
    splitList.add(carbonInputSplit);
    return new CarbonMultiBlockSplit(splitList, new String[] { "localhost" },
        FileFormat.ROW_V1);
  }

  @Test public void testCreateRecordReader() {
    try {
      InputSplit inputSplit = buildInputSplit();
      CarbonStreamInputFormat inputFormat = new CarbonStreamInputFormat();
      RecordReader recordReader = inputFormat.createRecordReader(inputSplit, taskAttemptContext);
      Assert.assertNotNull("Failed to create record reader", recordReader);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.assertTrue(e.getMessage(), false);
    }
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    if (tablePath != null) {
      FileFactory.deleteAllFilesOfDir(new File(tablePath));
    }
  }
}
