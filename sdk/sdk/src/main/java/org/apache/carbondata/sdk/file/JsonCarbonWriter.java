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

package org.apache.carbondata.sdk.file;

import java.io.IOException;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.hadoop.api.CarbonTableOutputFormat;
import org.apache.carbondata.hadoop.internal.ObjectArrayWritable;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

/**
 * Writer Implementation to write Json Record to carbondata file.
 * json writer requires the path of json file and carbon schema.
 */
@InterfaceAudience.User
public class JsonCarbonWriter extends CarbonWriter {
  private RecordWriter<NullWritable, ObjectArrayWritable> recordWriter;
  private TaskAttemptContext context;
  private ObjectArrayWritable writable;

  JsonCarbonWriter(CarbonLoadModel loadModel, Configuration configuration) throws IOException {
    CarbonTableOutputFormat.setLoadModel(configuration, loadModel);
    CarbonTableOutputFormat outputFormat = new CarbonTableOutputFormat();
    JobID jobId = new JobID(UUID.randomUUID().toString(), 0);
    Random random = new Random();
    TaskID task = new TaskID(jobId, TaskType.MAP, random.nextInt());
    TaskAttemptID attemptID = new TaskAttemptID(task, random.nextInt());
    TaskAttemptContextImpl context = new TaskAttemptContextImpl(configuration, attemptID);
    this.recordWriter = outputFormat.getRecordWriter(context);
    this.context = context;
    this.writable = new ObjectArrayWritable();
  }

  /**
   * Write single row data, accepts one row of data as json string
   *
   * @param object (json row as a string)
   * @throws IOException
   */
  @Override
  public void write(Object object) throws IOException {
    Objects.requireNonNull(object, "Input cannot be null");
    try {
      String[] jsonString = new String[1];
      jsonString[0] = (String) object;
      writable.set(jsonString);
      recordWriter.write(NullWritable.get(), writable);
    } catch (Exception e) {
      close();
      throw new IOException(e);
    }
  }

  /**
   * Flush and close the writer
   */
  @Override
  public void close() throws IOException {
    try {
      recordWriter.close(context);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }
}
