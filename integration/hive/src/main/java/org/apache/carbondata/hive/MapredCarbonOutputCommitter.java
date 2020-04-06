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

package org.apache.carbondata.hive;

import java.io.IOException;
import java.util.Random;
import java.util.UUID;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.metadata.SegmentFileStore;
import org.apache.carbondata.core.util.ObjectSerializationUtil;
import org.apache.carbondata.core.util.ThreadLocalSessionInfo;
import org.apache.carbondata.hadoop.api.CarbonOutputCommitter;
import org.apache.carbondata.hadoop.api.CarbonTableOutputFormat;
import org.apache.carbondata.hive.util.HiveCarbonUtil;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.log4j.Logger;

public class MapredCarbonOutputCommitter extends OutputCommitter {

  private CarbonOutputCommitter carbonOutputCommitter;

  private final Logger LOGGER =
      LogServiceFactory.getLogService(this.getClass().getName());

  @Override
  public void setupJob(JobContext jobContext) throws IOException {
    Random random = new Random();
    JobID jobId = new JobID(UUID.randomUUID().toString(), 0);
    TaskID task = new TaskID(jobId, TaskType.MAP, random.nextInt());
    TaskAttemptID attemptID = new TaskAttemptID(task, random.nextInt());
    org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl context =
        new TaskAttemptContextImpl(jobContext.getJobConf(), attemptID);
    CarbonLoadModel carbonLoadModel = null;
    String encodedString = jobContext.getJobConf().get(CarbonTableOutputFormat.LOAD_MODEL);
    if (encodedString != null) {
      carbonLoadModel =
          (CarbonLoadModel) ObjectSerializationUtil.convertStringToObject(encodedString);
    }
    if (null == carbonLoadModel) {
      ThreadLocalSessionInfo.setConfigurationToCurrentThread(jobContext.getConfiguration());
      String a = jobContext.getJobConf().get(JobConf.MAPRED_MAP_TASK_ENV);
      carbonLoadModel = HiveCarbonUtil.getCarbonLoadModel(jobContext.getConfiguration());
      CarbonTableOutputFormat.setLoadModel(jobContext.getConfiguration(), carbonLoadModel);
      String loadModelStr = jobContext.getConfiguration().get(CarbonTableOutputFormat.LOAD_MODEL);
      jobContext.getJobConf().set(JobConf.MAPRED_MAP_TASK_ENV, a + ",carbon=" + loadModelStr);
      jobContext.getJobConf().set(JobConf.MAPRED_REDUCE_TASK_ENV, a + ",carbon=" + loadModelStr);
    }
    carbonOutputCommitter =
        new CarbonOutputCommitter(new Path(carbonLoadModel.getTablePath()), context);
    carbonOutputCommitter.setupJob(jobContext);
  }

  @Override
  public void setupTask(TaskAttemptContext taskAttemptContext) throws IOException {

  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) throws IOException {
    return false;
  }

  @Override
  public void commitTask(TaskAttemptContext taskAttemptContext) throws IOException {

  }

  @Override
  public void abortTask(TaskAttemptContext taskAttemptContext) throws IOException {

  }

  @Override
  public void abortJob(JobContext jobContext, int status) throws IOException {
    if (carbonOutputCommitter != null) {
      carbonOutputCommitter.abortJob(jobContext, JobStatus.State.FAILED);
      throw new RuntimeException("Failed to commit Job");
    }
  }

  @Override
  public void commitJob(JobContext jobContext) throws IOException {
    try {
      Configuration configuration = jobContext.getConfiguration();
      CarbonLoadModel carbonLoadModel = MapredCarbonOutputFormat.getLoadModel(configuration);
      ThreadLocalSessionInfo.unsetAll();
      SegmentFileStore.writeSegmentFile(carbonLoadModel.getCarbonDataLoadSchema().getCarbonTable(),
          carbonLoadModel.getSegmentId(), String.valueOf(carbonLoadModel.getFactTimeStamp()));
      SegmentFileStore
          .mergeIndexAndWriteSegmentFile(carbonLoadModel.getCarbonDataLoadSchema().getCarbonTable(),
              carbonLoadModel.getSegmentId(), String.valueOf(carbonLoadModel.getFactTimeStamp()));
      CarbonTableOutputFormat.setLoadModel(configuration, carbonLoadModel);
      carbonOutputCommitter.commitJob(jobContext);
    } catch (Exception e) {
      LOGGER.error(e);
      throw e;
    }
  }
}
