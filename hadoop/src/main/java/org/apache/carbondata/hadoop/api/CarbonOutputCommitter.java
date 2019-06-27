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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datamap.status.DataMapStatusManager;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.locks.CarbonLockFactory;
import org.apache.carbondata.core.locks.ICarbonLock;
import org.apache.carbondata.core.locks.LockUsage;
import org.apache.carbondata.core.metadata.SegmentFileStore;
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
import org.apache.log4j.Logger;

/**
 * Outputcommitter which manages the segments during loading.It commits segment information to the
 * tablestatus file upon success or fail.
 */
public class CarbonOutputCommitter extends FileOutputCommitter {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(CarbonOutputCommitter.class.getName());

  private ICarbonLock segmentLock;

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
    if (loadModel.getSegmentId() == null) {
      CarbonLoaderUtil.readAndUpdateLoadProgressInTableMeta(loadModel, overwriteSet);
    }
    // Take segment lock
    segmentLock = CarbonLockFactory.getCarbonLockObj(
        loadModel.getCarbonDataLoadSchema().getCarbonTable().getAbsoluteTableIdentifier(),
        CarbonTablePath.addSegmentPrefix(loadModel.getSegmentId()) + LockUsage.LOCK);
    if (!segmentLock.lockWithRetries()) {
      throw new RuntimeException("Already segment is locked for loading, not supposed happen");
    }
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
    String readPath = CarbonTablePath.getSegmentFilesLocation(loadModel.getTablePath())
        + CarbonCommonConstants.FILE_SEPARATOR
        + loadModel.getSegmentId() + "_" + loadModel.getFactTimeStamp() + ".tmp";
    // Merge all partition files into a single file.
    String segmentFileName = SegmentFileStore.genSegmentFileName(
        loadModel.getSegmentId(), String.valueOf(loadModel.getFactTimeStamp()));
    SegmentFileStore.SegmentFile segmentFile = SegmentFileStore
        .mergeSegmentFiles(readPath, segmentFileName,
            CarbonTablePath.getSegmentFilesLocation(loadModel.getTablePath()));
    if (segmentFile != null) {
      if (null == newMetaEntry) {
        throw new RuntimeException("Internal Error");
      }
      // Move all files from temp directory of each segment to partition directory
      SegmentFileStore.moveFromTempFolder(segmentFile,
          loadModel.getSegmentId() + "_" + loadModel.getFactTimeStamp() + ".tmp",
          loadModel.getTablePath());
      newMetaEntry.setSegmentFile(segmentFileName + CarbonTablePath.SEGMENT_EXT);
    }
    OperationContext operationContext = (OperationContext) getOperationContext();
    CarbonTable carbonTable = loadModel.getCarbonDataLoadSchema().getCarbonTable();
    String uuid = "";
    if (loadModel.getCarbonDataLoadSchema().getCarbonTable().isChildDataMap() &&
        operationContext != null) {
      uuid = operationContext.getProperty("uuid").toString();
    }

    SegmentFileStore.updateSegmentFile(carbonTable, loadModel.getSegmentId(),
        segmentFileName + CarbonTablePath.SEGMENT_EXT,
        carbonTable.getCarbonTableIdentifier().getTableId(),
        new SegmentFileStore(carbonTable.getTablePath(),
            segmentFileName + CarbonTablePath.SEGMENT_EXT));

    CarbonLoaderUtil
        .populateNewLoadMetaEntry(newMetaEntry, SegmentStatus.SUCCESS, loadModel.getFactTimeStamp(),
            true);
    long segmentSize = CarbonLoaderUtil
        .addDataIndexSizeIntoMetaEntry(newMetaEntry, loadModel.getSegmentId(), carbonTable);
    if (segmentSize > 0 || overwriteSet) {
      if (operationContext != null) {
        operationContext
            .setProperty("current.segmentfile", newMetaEntry.getSegmentFile());
        LoadEvents.LoadTablePreStatusUpdateEvent event =
            new LoadEvents.LoadTablePreStatusUpdateEvent(carbonTable.getCarbonTableIdentifier(),
                loadModel);
        try {
          OperationListenerBus.getInstance().fireEvent(event, operationContext);
        } catch (Exception e) {
          throw new IOException(e);
        }
      }
      String uniqueId = null;
      if (overwriteSet) {
        if (!loadModel.isCarbonTransactionalTable()) {
          CarbonLoaderUtil.deleteNonTransactionalTableForInsertOverwrite(loadModel);
        } else {
          if (segmentSize == 0) {
            newMetaEntry.setSegmentStatus(SegmentStatus.MARKED_FOR_DELETE);
          }
          uniqueId = overwritePartitions(loadModel, newMetaEntry, uuid);
        }
      } else {
        CarbonLoaderUtil.recordNewLoadMetadata(newMetaEntry, loadModel, false, false, uuid);
      }
      DataMapStatusManager.disableAllLazyDataMaps(carbonTable);
      if (operationContext != null) {
        LoadEvents.LoadTablePostStatusUpdateEvent postStatusUpdateEvent =
            new LoadEvents.LoadTablePostStatusUpdateEvent(loadModel);
        try {
          OperationListenerBus.getInstance()
              .fireEvent(postStatusUpdateEvent, operationContext);
        } catch (Exception e) {
          throw new IOException(e);
        }
      }
      String updateTime =
          context.getConfiguration().get(CarbonTableOutputFormat.UPADTE_TIMESTAMP, null);
      String segmentsToBeDeleted =
          context.getConfiguration().get(CarbonTableOutputFormat.SEGMENTS_TO_BE_DELETED, "");
      List<Segment> segmentDeleteList = Segment.toSegmentList(segmentsToBeDeleted.split(","), null);
      Set<Segment> segmentSet = new HashSet<>(
          new SegmentStatusManager(carbonTable.getAbsoluteTableIdentifier(),
              context.getConfiguration()).getValidAndInvalidSegments(carbonTable.isChildTable())
              .getValidSegments());
      if (updateTime != null) {
        CarbonUpdateUtil.updateTableMetadataStatus(segmentSet, carbonTable, updateTime, true,
            segmentDeleteList);
      } else if (uniqueId != null) {
        CarbonUpdateUtil.updateTableMetadataStatus(segmentSet, carbonTable, uniqueId, true,
            segmentDeleteList);
      }
    } else {
      CarbonLoaderUtil.updateTableStatusForFailure(loadModel);
    }
    if (segmentLock != null) {
      segmentLock.unlock();
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
  private String overwritePartitions(CarbonLoadModel loadModel, LoadMetadataDetails newMetaEntry,
      String uuid) throws IOException {
    CarbonTable table = loadModel.getCarbonDataLoadSchema().getCarbonTable();
    SegmentFileStore fileStore = new SegmentFileStore(loadModel.getTablePath(),
        loadModel.getSegmentId() + "_" + loadModel.getFactTimeStamp()
            + CarbonTablePath.SEGMENT_EXT);
    List<PartitionSpec> partitionSpecs = fileStore.getPartitionSpecs();

    if (partitionSpecs != null && partitionSpecs.size() > 0) {
      List<Segment> validSegments =
          new SegmentStatusManager(table.getAbsoluteTableIdentifier())
              .getValidAndInvalidSegments(table.isChildTable()).getValidSegments();
      String uniqueId = String.valueOf(System.currentTimeMillis());
      List<String> tobeUpdatedSegs = new ArrayList<>();
      List<String> tobeDeletedSegs = new ArrayList<>();
      // First drop the partitions from partition mapper files of each segment
      for (Segment segment : validSegments) {
        new SegmentFileStore(table.getTablePath(), segment.getSegmentFileName())
            .dropPartitions(segment, partitionSpecs, uniqueId, tobeDeletedSegs, tobeUpdatedSegs);

      }
      newMetaEntry.setUpdateStatusFileName(uniqueId);
      // Commit the removed partitions in carbon store.
      CarbonLoaderUtil.recordNewLoadMetadata(newMetaEntry, loadModel, false, false, uuid,
          Segment.toSegmentList(tobeDeletedSegs, null),
          Segment.toSegmentList(tobeUpdatedSegs, null));
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
   * Update the tablestatus as fail if any fail happens.And also clean up the temp folders if any
   * are existed.
   *
   * @param context
   * @param state
   * @throws IOException
   */
  @Override public void abortJob(JobContext context, JobStatus.State state) throws IOException {
    try {
      super.abortJob(context, state);
      CarbonLoadModel loadModel = CarbonTableOutputFormat.getLoadModel(context.getConfiguration());
      CarbonLoaderUtil.updateTableStatusForFailure(loadModel);
      String segmentFileName = loadModel.getSegmentId() + "_" + loadModel.getFactTimeStamp();
      LoadMetadataDetails metadataDetail = loadModel.getCurrentLoadMetadataDetail();
      if (metadataDetail != null) {
        // In case the segment file is already created for this job then just link it so that it
        // will be used while cleaning.
        if (!metadataDetail.getSegmentStatus().equals(SegmentStatus.SUCCESS)) {
          String readPath = CarbonTablePath.getSegmentFilesLocation(loadModel.getTablePath())
              + CarbonCommonConstants.FILE_SEPARATOR + segmentFileName
              + CarbonTablePath.SEGMENT_EXT;
          if (FileFactory.getCarbonFile(readPath).exists()) {
            metadataDetail.setSegmentFile(segmentFileName + CarbonTablePath.SEGMENT_EXT);
          }
        }
      }
      // Clean the temp files
      CarbonFile segTmpFolder = FileFactory.getCarbonFile(
          CarbonTablePath.getSegmentFilesLocation(loadModel.getTablePath())
              + CarbonCommonConstants.FILE_SEPARATOR + segmentFileName + ".tmp");
      // delete temp segment folder
      if (segTmpFolder.exists()) {
        FileFactory.deleteAllCarbonFilesOfDir(segTmpFolder);
      }
      CarbonFile segmentFilePath = FileFactory.getCarbonFile(
          CarbonTablePath.getSegmentFilesLocation(loadModel.getTablePath())
              + CarbonCommonConstants.FILE_SEPARATOR + segmentFileName
              + CarbonTablePath.SEGMENT_EXT);
      // Delete the temp data folders of this job if exists
      if (segmentFilePath.exists()) {
        SegmentFileStore fileStore = new SegmentFileStore(loadModel.getTablePath(),
            segmentFileName + CarbonTablePath.SEGMENT_EXT);
        SegmentFileStore.removeTempFolder(fileStore.getLocationMap(), segmentFileName + ".tmp",
            loadModel.getTablePath());
      }
      LOGGER.error("Loading failed with job status : " + state);
    } finally {
      if (segmentLock != null) {
        segmentLock.unlock();
      }
    }
  }

}
