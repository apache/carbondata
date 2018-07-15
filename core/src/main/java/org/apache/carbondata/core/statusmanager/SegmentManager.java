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
package org.apache.carbondata.core.statusmanager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.locks.CarbonLockFactory;
import org.apache.carbondata.core.locks.CarbonLockUtil;
import org.apache.carbondata.core.locks.ICarbonLock;
import org.apache.carbondata.core.locks.LockUsage;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.SegmentFileStore;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.mutate.CarbonUpdateUtil;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.LiteralExpression;
import org.apache.carbondata.core.scan.expression.conditional.EqualToExpression;
import org.apache.carbondata.core.scan.expression.conditional.InExpression;
import org.apache.carbondata.core.scan.expression.conditional.ListExpression;
import org.apache.carbondata.core.util.DeleteLoadFolders;
import org.apache.carbondata.core.util.path.CarbonTablePath;

/**
 * Manages the segments of a table.
 */
public class SegmentManager {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(SegmentManager.class.getName());

  private SegmentStore segmentStore = new FileBasedSegmentStore();

  public SegmentDetailVO createNewOverwriteSegment(AbsoluteTableIdentifier identifier,
      SegmentDetailVO detailVO) {
    return createNewSegment(identifier, detailVO, true);
  }

  public SegmentDetailVO createNewSegment(AbsoluteTableIdentifier identifier,
      SegmentDetailVO detailVO) {
    return createNewSegment(identifier, detailVO, false);
  }

  /**
   * Create new segment for loading. It updates segmentId, load start time,
   * and status as LOAD_IN_PROGRESS
   */
  private SegmentDetailVO createNewSegment(AbsoluteTableIdentifier identifier,
      SegmentDetailVO detailVO, boolean overwrite) {
    if (detailVO.getStatus() == null) {
      if (!overwrite) {
        detailVO.setStatus(SegmentStatus.INSERT_IN_PROGRESS.toString());
      } else {
        detailVO.setStatus(SegmentStatus.INSERT_OVERWRITE_IN_PROGRESS.toString());
      }
    }
    if (detailVO.getLoadStartTime() == null) {
      detailVO.setLoadStartTime(System.currentTimeMillis());
    }
    List<Expression> filters = new ArrayList<>();
    List<Expression> listExps = new ArrayList<>();
    listExps
        .add(new LiteralExpression(SegmentStatus.INSERT_IN_PROGRESS.toString(), DataTypes.STRING));
    listExps.add(new LiteralExpression(SegmentStatus.INSERT_OVERWRITE_IN_PROGRESS.toString(),
        DataTypes.STRING));
    filters.add(new InExpression(new ColumnExpression("segmentStatus", DataTypes.STRING),
        new ListExpression(listExps)));
    filters.add(new EqualToExpression(new ColumnExpression("tableId", DataTypes.STRING),
        new LiteralExpression(identifier.getCarbonTableIdentifier().getTableId(),
            DataTypes.STRING)));
    List<SegmentDetailVO> segments = segmentStore.getSegments(identifier, filters);
    for (SegmentDetailVO detail : segments) {
      if (detail.getStatus().equals(SegmentStatus.INSERT_OVERWRITE_IN_PROGRESS.toString())
          && isLoadInProgress(identifier, detail.getSegmentId())) {
        throw new RuntimeException("Already insert overwrite is in progress");
      } else if (detailVO.getStatus().equals(SegmentStatus.INSERT_OVERWRITE_IN_PROGRESS.toString())
          && detail.getStatus().equals(SegmentStatus.INSERT_IN_PROGRESS.toString())
          && isLoadInProgress(identifier, detail.getSegmentId())) {
        throw new RuntimeException("Already insert into or load is in progress");
      }
    }
    try {
      segmentStore.generateSegmentIdAndInsert(identifier, detailVO);
    } catch (IOException e) {
      // TODO Create SegmentManagementException
      throw new RuntimeException(e);
    }
    return detailVO;
  }

  /**
   * Return true if the specified `loadName` is in progress, by checking the load lock.
   */
  public static Boolean isLoadInProgress(AbsoluteTableIdentifier absoluteTableIdentifier,
      String loadName) {
    ICarbonLock segmentLock = CarbonLockFactory.getCarbonLockObj(absoluteTableIdentifier,
        CarbonTablePath.addSegmentPrefix(loadName) + LockUsage.LOCK);
    try {
      return !segmentLock.lockWithRetries(1, 0);
    } finally {
      segmentLock.unlock();
    }
  }

  /**
   * It gives the valid segments from the store at this point of time.
   *
   * @param identifier
   * @return
   */
  public SegmentDetailVO getSegment(AbsoluteTableIdentifier identifier, String segmentId) {
    List<Expression> filters = new ArrayList<>();
    filters.add(new EqualToExpression(new ColumnExpression("tableId", DataTypes.STRING),
        new LiteralExpression(identifier.getCarbonTableIdentifier().getTableId(),
            DataTypes.STRING)));
    filters.add(
        new EqualToExpression(new ColumnExpression(SegmentDetailVO.SEGMENT_ID, DataTypes.STRING),
            new LiteralExpression(segmentId, DataTypes.STRING)));

    List<SegmentDetailVO> segments = segmentStore.getSegments(identifier, filters);
    if (segments.size() > 0) {
      return segments.get(0);
    }
    return null;
  }

  /**
   * It gives the valid segments from the store at this point of time.
   *
   * @param identifier
   * @return
   */
  public SegmentsHolder getValidSegments(AbsoluteTableIdentifier identifier) {
    List<Expression> filters = new ArrayList<>();
    List<Expression> listExps = new ArrayList<>();
    listExps.add(new LiteralExpression(SegmentStatus.SUCCESS.toString(), DataTypes.STRING));
    listExps
        .add(new LiteralExpression(SegmentStatus.MARKED_FOR_UPDATE.toString(), DataTypes.STRING));
    listExps.add(
        new LiteralExpression(SegmentStatus.LOAD_PARTIAL_SUCCESS.toString(), DataTypes.STRING));
    listExps.add(new LiteralExpression(SegmentStatus.STREAMING.toString(), DataTypes.STRING));
    listExps
        .add(new LiteralExpression(SegmentStatus.STREAMING_FINISH.toString(), DataTypes.STRING));
    filters.add(new InExpression(new ColumnExpression(SegmentDetailVO.STATUS, DataTypes.STRING),
        new ListExpression(listExps)));
    filters.add(new EqualToExpression(new ColumnExpression("tableId", DataTypes.STRING),
        new LiteralExpression(identifier.getCarbonTableIdentifier().getTableId(),
            DataTypes.STRING)));

    List<SegmentDetailVO> segments = segmentStore.getSegments(identifier, filters);

    return new SegmentsHolder(segments);
  }

  /**
   * It gives the valid segments from the store at this point of time.
   *
   * @param identifier
   * @return
   */
  public SegmentsHolder getInvalidSegments(AbsoluteTableIdentifier identifier) {
    List<Expression> filters = new ArrayList<>();
    List<Expression> listExps = new ArrayList<>();
    listExps.add(new LiteralExpression(SegmentStatus.LOAD_FAILURE.toString(), DataTypes.STRING));
    listExps.add(new LiteralExpression(SegmentStatus.COMPACTED.toString(), DataTypes.STRING));
    listExps
        .add(new LiteralExpression(SegmentStatus.MARKED_FOR_DELETE.toString(), DataTypes.STRING));
    filters.add(new InExpression(new ColumnExpression(SegmentDetailVO.STATUS, DataTypes.STRING),
        new ListExpression(listExps)));
    filters.add(new EqualToExpression(new ColumnExpression("tableId", DataTypes.STRING),
        new LiteralExpression(identifier.getCarbonTableIdentifier().getTableId(),
            DataTypes.STRING)));

    List<SegmentDetailVO> invalidSegments = segmentStore.getSegments(identifier, filters);

    return new SegmentsHolder(invalidSegments);
  }

  /**
   * It gives the valid segments from the store at this point of time.
   *
   * @param identifier
   * @return
   */
  public SegmentsHolder getAllSegments(AbsoluteTableIdentifier identifier) {
    List<Expression> filters = new ArrayList<>();
    filters.add(new EqualToExpression(new ColumnExpression("tableId", DataTypes.STRING),
        new LiteralExpression(identifier.getCarbonTableIdentifier().getTableId(),
            DataTypes.STRING)));
    List<SegmentDetailVO> allSegments = segmentStore.getSegments(identifier, filters);
    return new SegmentsHolder(allSegments);
  }

  /**
   * It gives the history segments from the store.
   *
   * @param identifier
   * @return
   */
  public SegmentsHolder getAllHistorySegments(AbsoluteTableIdentifier identifier) {
    // TODO
    return new SegmentsHolder(new ArrayList<SegmentDetailVO>());
  }

  /**
   * After dataloading is completed, this commit should be called. It inserts one new segment
   * to segment store. After commit success this segment will be available for reading in case of
   * success status.
   *
   * @param identifier
   * @param detailVO
   * @return
   */
  public boolean commitLoadSegment(AbsoluteTableIdentifier identifier, SegmentDetailVO detailVO)
      throws IOException {
    if (detailVO.getSegmentId() == null) {
      throw new UnsupportedOperationException("SegmentId cannot be null during commit");
    }
    List<SegmentDetailVO> detailVOS = new ArrayList<>();
    detailVOS.add(detailVO);
    List<CarbonFile> staleFolders = new ArrayList<>();
    if (detailVO.getStatus() != null && detailVO.getStatus()
        .equals(SegmentStatus.MARKED_FOR_DELETE.toString())) {
      addToStaleFolders(identifier, staleFolders, detailVO.getSegmentId());
    }

    boolean status = segmentStore.updateSegments(identifier, detailVOS);
    if (!FileFactory.deleteAllCarbonFiles(staleFolders)) {
      LOGGER.error("Failed to delete stale folder: " + staleFolders.get(0).getAbsolutePath());
    }
    return status;
  }

  private void addToStaleFolders(AbsoluteTableIdentifier identifier, List<CarbonFile> staleFolders,
      String segmentId) throws IOException {
    String path = CarbonTablePath.getSegmentPath(identifier.getTablePath(), segmentId);
    // add to the deletion list only if file exist else HDFS file system will throw
    // exception while deleting the file if file path does not exist
    if (FileFactory.isFileExist(path, FileFactory.getFileType(path))) {
      staleFolders.add(FileFactory.getCarbonFile(path));
    }
  }

  public boolean updateSegments(AbsoluteTableIdentifier identifier, List<SegmentDetailVO> detailVOs)
      throws IOException {
    for (SegmentDetailVO detailVO : detailVOs) {
      if (detailVO.getSegmentId() == null) {
        throw new UnsupportedOperationException("SegmentId cannot be null during commit");
      }
    }

    boolean status = segmentStore.updateSegments(identifier, detailVOs);
    return status;
  }

  /**
   * Commit the compact segment. Deleting the
   * old segments and adding the newly compacted segmented should be done in a transaction
   *
   * @param tableId
   * @param compactedSegment
   * @param oldSegments
   */
  public void commitCompactedSegment(String tableId, Segment compactedSegment,
      List<Segment> oldSegments) {

  }

  /**
   * Uses for committing the overwrite segment(it is the segment created for insert overwrite case).
   * In this case all old segments need to be invalidated and new segment should be added in a
   * transaction.
   *
   * @param identifier
   * @param detailVO
   */
  public boolean commitOverwriteSegment(AbsoluteTableIdentifier identifier,
      SegmentDetailVO detailVO) throws IOException {
    if (detailVO.getSegmentId() == null) {
      throw new UnsupportedOperationException("SegmentId cannot be null during commit");
    }
    List<SegmentDetailVO> detailVOS = new ArrayList<>();
    detailVOS.add(detailVO);
    List<CarbonFile> staleFolders = new ArrayList<>();
    if (detailVO.getStatus() != null && detailVO.getStatus()
        .equals(SegmentStatus.MARKED_FOR_DELETE.toString())) {
      addToStaleFolders(identifier, staleFolders, detailVO.getSegmentId());
    }

    for (SegmentDetailVO vo : getAllSegments(identifier).getAllSegments()) {
      if (!vo.getStatus().equals(SegmentStatus.INSERT_OVERWRITE_IN_PROGRESS.toString()) && !detailVO
          .getSegmentId().equals(vo.getSegmentId())) {
        detailVOS.add(new SegmentDetailVO().setSegmentId(vo.getSegmentId())
            .setStatus(SegmentStatus.MARKED_FOR_DELETE.toString()));
        addToStaleFolders(identifier, staleFolders, vo.getSegmentId());
      }
    }
    boolean status = segmentStore.updateSegments(identifier, detailVOS);
    if (!FileFactory.deleteAllCarbonFiles(staleFolders)) {
      LOGGER.error("Failed to delete stale folder: " + staleFolders.get(0).getAbsolutePath());
    }
    return status;
  }

  /**
   * Uses for committing the overwrite segment(it is the segment created for insert overwrite case).
   * In this case all old segments need to be invalidated and new segment should be added in a
   * transaction.
   *
   * @param identifier
   * @param detailVO
   */
  public boolean commitOverwritePartitionSegment(AbsoluteTableIdentifier identifier,
      SegmentDetailVO detailVO) throws IOException {
    SegmentFileStore fileStore = new SegmentFileStore(identifier.getTablePath(),
        detailVO.getSegmentId() + "_" + detailVO.getLoadStartTime() + CarbonTablePath.SEGMENT_EXT);
    List<PartitionSpec> partitionSpecs = fileStore.getPartitionSpecs();

    if (partitionSpecs != null && partitionSpecs.size() > 0) {
      List<Expression> filters = new ArrayList<>();
      filters.add(new EqualToExpression(new ColumnExpression("segmentStatus", DataTypes.STRING),
          new LiteralExpression(SegmentStatus.SUCCESS.toString(), DataTypes.STRING)));
      filters.add(new EqualToExpression(new ColumnExpression("tableId", DataTypes.STRING),
          new LiteralExpression(identifier.getCarbonTableIdentifier().getTableId(),
              DataTypes.STRING)));
      List<SegmentDetailVO> segments = segmentStore.getSegments(identifier, filters);
      String uniqueId = String.valueOf(System.currentTimeMillis());
      List<SegmentDetailVO> tobeUpdatedSegs = new ArrayList<>();
      // First drop the partitions from partition mapper files of each segment
      for (SegmentDetailVO segment : segments) {
        new SegmentFileStore(identifier.getTablePath(), segment.getSegmentFileName())
            .dropPartitions(new Segment(segment.getSegmentId(), segment.getSegmentFileName()),
                partitionSpecs, uniqueId, tobeUpdatedSegs);
      }
      detailVO.setUpdateStatusFilename(CarbonUpdateUtil.getUpdateStatusFileName(uniqueId));
      tobeUpdatedSegs.add(detailVO);

      // Commit the removed partitions in carbon store.
      boolean status = segmentStore.updateSegments(identifier, tobeUpdatedSegs);

      return status;
    }
    return true;
  }

  /**
   * clean the stale segments from segment store.
   *
   * @param tableId
   * @return
   */
  void cleanInvalidSegments(String tableId) {

  }

  /**
   * Removes/invalidates all the segments which are less than passed load time.
   *
   * @param tableId
   * @param loadTime
   * @return
   */
  boolean deleteSegmentByLoadTime(String tableId, long loadTime) {
    return false;
  }

  private static class ReturnTuple {
    List<SegmentDetailVO> allSegments;
    boolean isUpdateRequired;

    ReturnTuple(List<SegmentDetailVO> allSegments, boolean isUpdateRequired) {
      this.allSegments = allSegments;
      this.isUpdateRequired = isUpdateRequired;
    }
  }

  private ReturnTuple isUpdationRequired(boolean isForceDeletion, CarbonTable carbonTable) {
    List<SegmentDetailVO> allSegments =
        getAllSegments(carbonTable.getAbsoluteTableIdentifier()).getAllSegments();

    // Delete marked loads
    boolean isUpdationRequired = DeleteLoadFolders
        .deleteLoadFoldersFromFileSystem(carbonTable.getAbsoluteTableIdentifier(), isForceDeletion,
            allSegments);
    return new ReturnTuple(allSegments, isUpdationRequired);
  }

  public void deleteLoadsAndUpdateMetadata(CarbonTable carbonTable, boolean isForceDeletion,
      List<PartitionSpec> partitionSpecs) throws IOException {
    // delete the expired segment lock files
    CarbonLockUtil.deleteExpiredSegmentLockFiles(carbonTable);
    if (isSegmentStatusDeletionRequired(carbonTable)) {
      ReturnTuple tuple = isUpdationRequired(isForceDeletion, carbonTable);
      if (tuple.isUpdateRequired) {
        // TODO
      }
    }
  }

  /**
   * Get the number of invisible segment info from segment info list.
   */
  private int countInvisibleSegments(List<SegmentDetailVO> segmentList) {
    int invisibleSegmentCnt = 0;
    if (segmentList.size() != 0) {
      for (SegmentDetailVO eachSeg : segmentList) {
        // can not remove segment 0, there are some info will be used later
        // for example: updateStatusFileName
        // also can not remove the max segment id,
        // otherwise will impact the generation of segment id
        if (!eachSeg.getVisibility()) {
          invisibleSegmentCnt += 1;
        }
      }
    }
    return invisibleSegmentCnt;
  }

  private boolean isSegmentStatusDeletionRequired(CarbonTable carbonTable) {
    List<SegmentDetailVO> allSegments =
        getAllSegments(carbonTable.getAbsoluteTableIdentifier()).getAllSegments();
    if (allSegments.size() > 0) {
      for (SegmentDetailVO oneRow : allSegments) {
        if ((SegmentStatus.MARKED_FOR_DELETE.toString().equals(oneRow.getStatus())
            || SegmentStatus.COMPACTED.toString().equals(oneRow.getStatus())
            || SegmentStatus.INSERT_IN_PROGRESS.toString().equals(oneRow.getStatus())
            || SegmentStatus.INSERT_OVERWRITE_IN_PROGRESS.toString().equals(oneRow.getStatus()))
            && oneRow.getVisibility()) {
          return true;
        }
      }
    }
    return false;
  }

}