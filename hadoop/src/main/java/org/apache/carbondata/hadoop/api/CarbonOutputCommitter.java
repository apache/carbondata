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
import java.util.*;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.PartitionMapFileStore;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.mutate.CarbonUpdateUtil;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.core.writer.CarbonIndexFileMergeWriter;
import org.apache.carbondata.events.OperationContext;
import org.apache.carbondata.events.OperationListenerBus;
import org.apache.carbondata.hadoop.util.ObjectSerializationUtil;
import org.apache.carbondata.processing.loading.events.LoadEvents;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.util.CarbonLoaderUtil;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

/**
 * Outputcommitter which manages the segments during loading.It commits segment information to the
 * tablestatus file upon success or fail.
 */
public class CarbonOutputCommitter extends FileOutputCommitter {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonOutputCommitter.class.getName());

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
    CarbonLoaderUtil.readAndUpdateLoadProgressInTableMeta(loadModel, overwriteSet);
    CarbonLoaderUtil.checkAndCreateCarbonDataLocation(loadModel.getSegmentId(),
        loadModel.getCarbonDataLoadSchema().getCarbonTable());
    CarbonTableOutputFormat.setLoadModel(context.getConfiguration(), loadModel);
  }

  @Override public void setupTask(TaskAttemptContext context) throws IOException {
    super.setupTask(context);
  }

  /**
   * Update the tablestatus as success after job is success
   *
   * @param context
   * @throws IOException
   */
  @Override public void commitJob(JobContext context) throws IOException {
    try {
      super.commitJob(context);
    } catch (IOException e) {
      // ignore, in case of concurrent load it try to remove temporary folders by other load may
      // cause file not found exception. This will not impact carbon load,
      LOGGER.warn(e.getMessage());
    }
    boolean overwriteSet = CarbonTableOutputFormat.isOverwriteSet(context.getConfiguration());
    CarbonLoadModel loadModel = CarbonTableOutputFormat.getLoadModel(context.getConfiguration());
    LoadMetadataDetails newMetaEntry = loadModel.getCurrentLoadMetadataDetail();
    String segmentPath =
        CarbonTablePath.getSegmentPath(loadModel.getTablePath(), loadModel.getSegmentId());
    // Merge all partition files into a single file.
    new PartitionMapFileStore().mergePartitionMapFiles(segmentPath,
        loadModel.getFactTimeStamp() + "");
    CarbonLoaderUtil.populateNewLoadMetaEntry(newMetaEntry, SegmentStatus.SUCCESS,
        loadModel.getFactTimeStamp(), true);
    CarbonTable carbonTable = loadModel.getCarbonDataLoadSchema().getCarbonTable();
    long segmentSize = CarbonLoaderUtil
        .addDataIndexSizeIntoMetaEntry(newMetaEntry, loadModel.getSegmentId(), carbonTable);
    if (segmentSize > 0 || overwriteSet) {
      String operationContextStr =
          context.getConfiguration().get(
              CarbonTableOutputFormat.OPERATION_CONTEXT,
              null);
      if (operationContextStr != null) {
        OperationContext operationContext =
            (OperationContext) ObjectSerializationUtil.convertStringToObject(operationContextStr);
        LoadEvents.LoadTablePreStatusUpdateEvent event =
            new LoadEvents.LoadTablePreStatusUpdateEvent(carbonTable.getCarbonTableIdentifier(),
                loadModel);
        try {
          OperationListenerBus.getInstance().fireEvent(event, operationContext);
        } catch (Exception e) {
          throw new IOException(e);
        }
      }
      CarbonLoaderUtil.recordNewLoadMetadata(newMetaEntry, loadModel, false, overwriteSet);
      mergeCarbonIndexFiles(segmentPath);
      String updateTime =
          context.getConfiguration().get(CarbonTableOutputFormat.UPADTE_TIMESTAMP, null);
      String segmentsToBeDeleted =
          context.getConfiguration().get(CarbonTableOutputFormat.SEGMENTS_TO_BE_DELETED, "");
      List<String> segmentDeleteList = Arrays.asList(segmentsToBeDeleted.split(","));
      if (updateTime != null) {
        Set<String> segmentSet = new HashSet<>(
            new SegmentStatusManager(carbonTable.getAbsoluteTableIdentifier())
                .getValidAndInvalidSegments().getValidSegments());
        CarbonUpdateUtil.updateTableMetadataStatus(
            segmentSet,
            carbonTable,
            updateTime,
            true,
            segmentDeleteList);
      }
    } else {
      CarbonLoaderUtil.updateTableStatusForFailure(loadModel);
    }
  }

  /**
   * Merge index files to a new single file.
   */
  private void mergeCarbonIndexFiles(String segmentPath) throws IOException {
    boolean mergeIndex = false;
    try {
      mergeIndex = Boolean.parseBoolean(CarbonProperties.getInstance().getProperty(
          CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT,
          CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT_DEFAULT));
    } catch (Exception e) {
      mergeIndex = Boolean.parseBoolean(
          CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT_DEFAULT);
    }
    if (mergeIndex) {
      new CarbonIndexFileMergeWriter().mergeCarbonIndexFilesOfSegment(segmentPath);
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
    CarbonLoaderUtil.updateTableStatusForFailure(loadModel);
    LOGGER.error("Loading failed with job status : " + state);
  }

}
