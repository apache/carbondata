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

package org.apache.carbondata.hadoop.api;

import java.io.IOException;

import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.util.CarbonLoaderUtil;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

/**
 * Outputcommitter which manages the segments during loading.
 */
public class CarbonOutputCommitter extends FileOutputCommitter {

  public CarbonOutputCommitter(Path outputPath, TaskAttemptContext context) throws IOException {
    super(outputPath, context);
  }

  /**
   * Update the tablestatus with inprogress while setup the job.
   *
   * @param context
   * @throws IOException
   */
  @Override public void setupJob(JobContext context) throws IOException {
    super.setupJob(context);
    boolean overwriteSet = CarbonTableOutputFormat.isOverwriteSet(context.getConfiguration());
    CarbonLoadModel loadModel = CarbonTableOutputFormat.getLoadModel(context.getConfiguration());
    try {
      CarbonLoaderUtil.readAndUpdateLoadProgressInTableMeta(loadModel, overwriteSet);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
    CarbonTableOutputFormat.setLoadModel(context.getConfiguration(), loadModel);
  }

  /**
   * Update the tablestatus as success after job is success
   *
   * @param context
   * @throws IOException
   */
  @Override public void commitJob(JobContext context) throws IOException {
    super.commitJob(context);
    boolean overwriteSet = CarbonTableOutputFormat.isOverwriteSet(context.getConfiguration());
    CarbonLoadModel loadModel = CarbonTableOutputFormat.getLoadModel(context.getConfiguration());
    try {
      LoadMetadataDetails newMetaEntry =
          loadModel.getLoadMetadataDetails().get(loadModel.getLoadMetadataDetails().size() - 1);
      CarbonLoaderUtil.populateNewLoadMetaEntry(newMetaEntry, SegmentStatus.SUCCESS,
          loadModel.getFactTimeStamp(), true);
      CarbonUtil.addDataIndexSizeIntoMetaEntry(newMetaEntry, loadModel.getSegmentId(),
          loadModel.getCarbonDataLoadSchema().getCarbonTable());
      CarbonLoaderUtil.recordNewLoadMetadata(newMetaEntry, loadModel, false, overwriteSet);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  /**
   * Update the tablestatus as fail if any fail happens.
   *
   * @param context
   * @param state
   * @throws IOException
   */
  @Override public void abortJob(JobContext context, JobStatus.State state) throws IOException {
    super.abortJob(context, state);
    CarbonLoadModel loadModel = CarbonTableOutputFormat.getLoadModel(context.getConfiguration());
    try {
      CarbonLoaderUtil.updateTableStatusForFailure(loadModel);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

}
