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

package org.apache.carbondata.store;

import java.io.IOException;
import java.util.HashMap;
import java.util.UUID;

import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.hadoop.api.CarbonTableOutputFormat;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.util.CarbonLoaderUtil;
import org.apache.carbondata.spark.util.DataLoadingUtil;
import org.apache.carbondata.store.api.Segment;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

public class BatchSegment implements Segment {
  private CarbonTable table;
  private CarbonTableOutputFormat format;
  private Configuration hadoopConf;
  private JobID jobId;
  private JobContext jobContext;
  private int taskId;
  private OutputCommitter committer;
  private boolean opened;

  private BatchSegment(TableImpl table) {
    this.table = table.getMeta();
    this.opened = false;
    this.taskId = 0;
    this.hadoopConf = new Configuration();
  }

  public static BatchSegment newInstace(TableImpl table) {
    return new BatchSegment(table);
  }

  @Override
  public void open() throws IOException {
    if (opened) throw new IllegalStateException("already opened");
    CarbonLoadModel loadModel =
        DataLoadingUtil.buildCarbonLoadModelJava(table, new HashMap<String, String>());
    CarbonLoaderUtil.readAndUpdateLoadProgressInTableMeta(loadModel, false);
    this.hadoopConf = new Configuration();
    CarbonTableOutputFormat.setLoadModel(hadoopConf, loadModel);
    this.format = new CarbonTableOutputFormat();
    this.jobId = new JobID(UUID.randomUUID().toString(), 0);
    this.jobContext = Job.getInstance(hadoopConf);
    TaskAttemptContextImpl context = new TaskAttemptContextImpl(hadoopConf, getTaskAttemptID());
    this.committer = format.getOutputCommitter(context);
    this.opened = true;
  }

  @Override
  public void commit() throws IOException {
    if (!opened) throw new IllegalStateException("Segment not opened");
    committer.commitJob(jobContext);
    opened = false;
  }

  @Override
  public void abort() throws IOException {
    if (!opened) throw new IllegalStateException("Segment not opened");
    committer.abortJob(jobContext, JobStatus.State.FAILED);
    opened = false;
  }

  @Override
  public CarbonFileWriterImpl newWriter() throws IOException {
    if (!opened) throw new IllegalStateException("Segment not opened");
    TaskAttemptContextImpl context = new TaskAttemptContextImpl(hadoopConf, getTaskAttemptID());
    return new CarbonFileWriterImpl(format, context);
  }

  private TaskAttemptID getTaskAttemptID() {
    TaskID task = new TaskID(jobId, TaskType.MAP, taskId);
    return new TaskAttemptID(task, taskId++);
  }
}
