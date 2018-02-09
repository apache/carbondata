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
import org.apache.carbondata.core.metadata.PartitionMapFileStore;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.mutate.CarbonUpdateUtil;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.CarbonSessionInfo;
import org.apache.carbondata.core.util.ThreadLocalSessionInfo;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.events.OperationContext;
import org.apache.carbondata.events.OperationListenerBus;
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
      Object operationContext = getOperationContext();
      if (operationContext != null) {
        LoadEvents.LoadTablePreStatusUpdateEvent event =
            new LoadEvents.LoadTablePreStatusUpdateEvent(carbonTable.getCarbonTableIdentifier(),
                loadModel);
        LoadEvents.LoadTablePostStatusUpdateEvent postStatusUpdateEvent =
            new LoadEvents.LoadTablePostStatusUpdateEvent(loadModel);
        try {
          OperationListenerBus.getInstance().fireEvent(event, (OperationContext) operationContext);
          OperationListenerBus.getInstance().fireEvent(postStatusUpdateEvent,
              (OperationContext) operationContext);
        } catch (Exception e) {
          throw new IOException(e);
        }
      }
      String uniqueId = null;
      if (overwriteSet) {
        uniqueId = overwritePartitions(loadModel);
      }
      CarbonLoaderUtil.recordNewLoadMetadata(newMetaEntry, loadModel, false, false);
      if (operationContext != null) {
        LoadEvents.LoadTableMergePartitionEvent loadTableMergePartitionEvent =
            new LoadEvents.LoadTableMergePartitionEvent(segmentPath);
        try {
          OperationListenerBus.getInstance()
              .fireEvent(loadTableMergePartitionEvent, (OperationContext) operationContext);
        } catch (Exception e) {
          throw new IOException(e);
        }
      }
      String updateTime =
          context.getConfiguration().get(CarbonTableOutputFormat.UPADTE_TIMESTAMP, null);
      String segmentsToBeDeleted =
          context.getConfiguration().get(CarbonTableOutputFormat.SEGMENTS_TO_BE_DELETED, "");
      List<String> segmentDeleteList = Arrays.asList(segmentsToBeDeleted.split(","));
      Set<String> segmentSet = new HashSet<>(
          new SegmentStatusManager(carbonTable.getAbsoluteTableIdentifier())
              .getValidAndInvalidSegments().getValidSegments());
      if (updateTime != null) {
        CarbonUpdateUtil.updateTableMetadataStatus(
            segmentSet,
            carbonTable,
            updateTime,
            true,
            segmentDeleteList);
      } else if (uniqueId != null) {
        // Update the loadstatus with update time to clear cache from driver.
        CarbonUpdateUtil.updateTableMetadataStatus(
            segmentSet,
            carbonTable,
            uniqueId,
            true,
            new ArrayList<String>());
      }
    } else {
      CarbonLoaderUtil.updateTableStatusForFailure(loadModel);
    }
  }

  /**
   * Overwrite the partitions in case of overwrite query. It just updates the partition map files
   * of all segment files.
   *
   * @param loadModel
   * @return
   * @throws IOException
   */
  private String overwritePartitions(CarbonLoadModel loadModel) throws IOException {
    CarbonTable table = loadModel.getCarbonDataLoadSchema().getCarbonTable();
    String currentSegmentPath =
        CarbonTablePath.getSegmentPath(loadModel.getTablePath(), loadModel.getSegmentId());
    PartitionMapFileStore partitionMapFileStore = new PartitionMapFileStore();
    partitionMapFileStore.readAllPartitionsOfSegment(currentSegmentPath);
    List<List<String>> partitionsToDrop =
        new ArrayList<List<String>>(partitionMapFileStore.getPartitionMap().values());
    if (partitionsToDrop.size() > 0) {
      List<String> validSegments =
          new SegmentStatusManager(table.getAbsoluteTableIdentifier()).getValidAndInvalidSegments()
              .getValidSegments();
      String uniqueId = String.valueOf(System.currentTimeMillis());
      try {
        // First drop the partitions from partition mapper files of each segment
        for (String segment : validSegments) {
          new PartitionMapFileStore()
              .dropPartitions(CarbonTablePath.getSegmentPath(table.getTablePath(), segment),
                  new ArrayList<List<String>>(partitionsToDrop), uniqueId, false);

        }
      } catch (Exception e) {
        // roll back the drop partitions from carbon store
        for (String segment : validSegments) {
          new PartitionMapFileStore()
              .commitPartitions(CarbonTablePath.getSegmentPath(table.getTablePath(), segment),
                  uniqueId, false, table.getTablePath(), partitionsToDrop.get(0));
        }
      }
      // Commit the removed partitions in carbon store.
      for (String segment : validSegments) {
        new PartitionMapFileStore()
            .commitPartitions(CarbonTablePath.getSegmentPath(table.getTablePath(), segment),
                uniqueId, true, table.getTablePath(), partitionsToDrop.get(0));
      }
      return uniqueId;
    }
    return null;
  }

  private Object getOperationContext() {
    // when validate segments is disabled in thread local update it to CarbonTableInputFormat
    CarbonSessionInfo carbonSessionInfo = ThreadLocalSessionInfo.getCarbonSessionInfo();
    if (carbonSessionInfo != null) {
      return carbonSessionInfo.getThreadParams().getExtraInfo("partition.operationcontext");
    }
    return null;
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
