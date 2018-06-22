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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datamap.status.DataMapStatusManager;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.locks.CarbonLockFactory;
import org.apache.carbondata.core.locks.ICarbonLock;
import org.apache.carbondata.core.locks.LockUsage;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.SegmentFileStore;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.mutate.CarbonUpdateUtil;
import org.apache.carbondata.core.statusmanager.SegmentDetailVO;
import org.apache.carbondata.core.statusmanager.SegmentManager;
import org.apache.carbondata.core.statusmanager.SegmentManagerHelper;
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
    AbsoluteTableIdentifier identifier =
        loadModel.getCarbonDataLoadSchema().getCarbonTable().getAbsoluteTableIdentifier();
    if (loadModel.getSegmentId() == null) {
      SegmentStatus status;
      if (overwriteSet) {
        status = SegmentStatus.INSERT_OVERWRITE_IN_PROGRESS;

      } else {
        status = SegmentStatus.INSERT_IN_PROGRESS;
      }
      SegmentDetailVO segmentVO = SegmentManagerHelper
          .createSegmentVO(loadModel.getSegmentId(), status, loadModel.getFactTimeStamp());
      SegmentDetailVO newSegment = new SegmentManager().createNewSegment(identifier, segmentVO);
      loadModel.setCurrentDetailVO(newSegment);
    }
    // Take segment lock
    segmentLock = CarbonLockFactory.getCarbonLockObj(
        identifier,
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
    SegmentDetailVO currentDetailVO = new SegmentDetailVO().setSegmentId(loadModel.getSegmentId());
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
      // Move all files from temp directory of each segment to partition directory
      SegmentFileStore.moveFromTempFolder(segmentFile,
          loadModel.getSegmentId() + "_" + loadModel.getFactTimeStamp() + ".tmp",
          loadModel.getTablePath());
      currentDetailVO.setSegmentFileName(segmentFileName + CarbonTablePath.SEGMENT_EXT);
    }
    OperationContext operationContext = (OperationContext) getOperationContext();
    String uuid = getUUID(loadModel, operationContext);
    currentDetailVO.setLoadEndTime(System.currentTimeMillis()).setTransactionId(uuid)
        .setStatus(SegmentStatus.SUCCESS.toString());
    CarbonTable carbonTable = loadModel.getCarbonDataLoadSchema().getCarbonTable();
    long segmentSize = CarbonLoaderUtil
        .addDataIndexSizeIntoMetaEntry(currentDetailVO, loadModel.getSegmentId(), carbonTable);
    if (segmentSize > 0 || overwriteSet) {
      if (operationContext != null && carbonTable.hasAggregationDataMap()) {
        operationContext
            .setProperty("current.segmentfile", currentDetailVO.getSegmentFileName());
        LoadEvents.LoadTablePreStatusUpdateEvent event =
            new LoadEvents.LoadTablePreStatusUpdateEvent(carbonTable.getCarbonTableIdentifier(),
                loadModel);
        try {
          OperationListenerBus.getInstance().fireEvent(event, operationContext);
        } catch (Exception e) {
          throw new IOException(e);
        }
      }
      if (overwriteSet) {
        if (!loadModel.isCarbonTransactionalTable()) {
          CarbonLoaderUtil.deleteNonTransactionalTableForInsertOverwrite(loadModel);
        } else {
          if (segmentSize == 0) {
            currentDetailVO.setStatus(SegmentStatus.MARKED_FOR_DELETE.toString());
          }
          overwritePartitions(loadModel, currentDetailVO);
        }
      } else {
        new SegmentManager().commitLoadSegment(
            carbonTable.getAbsoluteTableIdentifier(),
            currentDetailVO);
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
          new SegmentManager().getValidSegments(carbonTable.getAbsoluteTableIdentifier())
              .getValidSegments());
      if (updateTime != null) {
        CarbonUpdateUtil.updateTableMetadataStatus(segmentSet, carbonTable, updateTime, true,
            segmentDeleteList);
      }
    } else {
      SegmentDetailVO detailVO = SegmentManagerHelper
          .updateFailStatusAndGetSegmentVO(loadModel.getSegmentId(), uuid);
      new SegmentManager().commitLoadSegment(carbonTable.getAbsoluteTableIdentifier(), detailVO);
    }
    if (segmentLock != null) {
      segmentLock.unlock();
    }
  }

  private String getUUID(CarbonLoadModel loadModel, OperationContext operationContext) {
    String uuid = "";
    if (loadModel.getCarbonDataLoadSchema().getCarbonTable().isChildDataMap() &&
        operationContext != null) {
      uuid = operationContext.getProperty("uuid").toString();
    }
    return uuid;
  }

  /**
   * Overwrite the partitions in case of overwrite query. It just updates the partition map files
   * of all segment files.
   *
   * @param loadModel
   * @return
   * @throws IOException
   */
  private boolean overwritePartitions(CarbonLoadModel loadModel, SegmentDetailVO detailVO) throws IOException {
    CarbonTable table = loadModel.getCarbonDataLoadSchema().getCarbonTable();
    return new SegmentManager()
        .commitOverwritePartitionSegment(table.getAbsoluteTableIdentifier(), detailVO);
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
      OperationContext operationContext = (OperationContext) getOperationContext();
      String uuid = getUUID(loadModel, operationContext);
      String segmentFileName = loadModel.getSegmentId() + "_" + loadModel.getFactTimeStamp();
      SegmentDetailVO segmentDetailVO = loadModel.getCurrentDetailVO();
      SegmentDetailVO detailVO = SegmentManagerHelper
          .updateFailStatusAndGetSegmentVO(loadModel.getSegmentId(), uuid);
      if (segmentDetailVO != null) {
        // In case the segment file is already created for this job then just link it so that it
        // will be used while cleaning.
        if (!segmentDetailVO.getStatus().equals(SegmentStatus.SUCCESS.toString())) {
          String readPath = CarbonTablePath.getSegmentFilesLocation(loadModel.getTablePath())
              + CarbonCommonConstants.FILE_SEPARATOR + segmentFileName
              + CarbonTablePath.SEGMENT_EXT;
          if (FileFactory.getCarbonFile(readPath).exists()) {
            detailVO.setSegmentFileName(segmentFileName + CarbonTablePath.SEGMENT_EXT);
          }
        }
      }
      new SegmentManager().commitLoadSegment(
          loadModel.getCarbonDataLoadSchema().getCarbonTable().getAbsoluteTableIdentifier(),
          detailVO);
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
