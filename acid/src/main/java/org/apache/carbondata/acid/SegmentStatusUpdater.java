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

package org.apache.carbondata.acid;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.LiteralExpression;
import org.apache.carbondata.core.scan.expression.conditional.EqualToExpression;
import org.apache.carbondata.core.scan.expression.conditional.InExpression;
import org.apache.carbondata.core.scan.expression.conditional.ListExpression;
import org.apache.carbondata.core.statusmanager.SegmentStatus;

import org.apache.log4j.Logger;


/**
 * Updater for table status. It uses the segmentStore interface to update or insert the segment status.
 */
public class SegmentStatusUpdater {


  private AbsoluteTableIdentifier identifier;
  private SegmentStore segmentStore;

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(SegmentStatusUpdater.class.getName());

  public SegmentStatusUpdater(AbsoluteTableIdentifier identifier, SegmentStore segmentStore) {
    this.identifier = identifier;
    this.segmentStore = segmentStore;
  }

  /**
   * Create a new segment with status load in progress and generate newsegmentId.
   * * @param detailVO SegmentDetailsVO ,user can pass empty object if no extra parameters need to be updated while creating segment.Otherwise he can pass it with updated parameters
   *
   * @return SegmentDetailsVO of newly created segment.
   */
  public SegmentDetailVO createNewSegment(SegmentDetailVO detailVO) {
    return createNewSegment(detailVO, false);
  }

  /**
   * Create a new overwrite segment with status overwrite in progress and generate new segmentId.
   * * @param detailVO SegmentDetailsVO ,user can pass empty object if no extra parameters need to be
   * updated while creating segment.Otherwise he can pass it with updated parameters
   *
   * @return SegmentDetailsVO of newly created overwrite segment.
   */
  public SegmentDetailVO createNewOverwriteSegment(SegmentDetailVO detailVO) {
    return createNewSegment(detailVO, true);
  }

  /**
   * Create new segment for loading. It updates segmentId, load start time,
   * and status as LOAD_IN_PROGRESS
   */
  private SegmentDetailVO createNewSegment(SegmentDetailVO detailVO, boolean overwrite) {
   /* if (detailVO.getStatus() == null) {
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
    // TODO: use the reader interface for getSegments
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
    }*/
    return detailVO;
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
  public boolean commitLoadSegment(SegmentDetailVO detailVO) throws IOException {
    /*if (detailVO.getSegmentId() == null) {
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
    return status;*/
    return false;
  }

  /**
   * Uses for committing the overwrite segment(it is the segment created for insert overwrite case).
   * In this case all old segments need to be invalidated and new segment should be added in a
   * transaction.
   *
   * @param identifier
   * @param detailVO
   */
  // TODO: commit can be used for
  public boolean commitOverwriteSegment(SegmentDetailVO detailVO) throws IOException {
    if (detailVO.getSegmentId() == null) {
      throw new UnsupportedOperationException("SegmentId cannot be null during commit");
    }
    List<SegmentDetailVO> detailVOS = new ArrayList<>();
    detailVOS.add(detailVO);
    List<CarbonFile> staleFolders = new ArrayList<>();
    /*if (detailVO.getStatus() != null && detailVO.getStatus()
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
    return status;*/
    return false;
  }

  /**
   * Segments which are passed will be updated into table status
   * * @param detailVOs List of SegmentDetailsVO to be updated in table status.
   *
   * @return
   */
  public boolean updateSegments(List<SegmentDetailVO> detailVOs) throws IOException {
    /*for (SegmentDetailVO detailVO : detailVOs) {
      if (detailVO.getSegmentId() == null) {
        throw new UnsupportedOperationException("SegmentId cannot be null during commit");
      }
    }

    boolean status = segmentStore.updateSegments(identifier, detailVOs);
    return status;*/
    return false;
  }







  /*  *//**
   * Removes/invalidates all the segments which are passed.
   ** @param segmentIds List of segments to be removed.
   * @return List of invalid segments.
   *//*
  public List<String> deleteSegmentsBySegmentIds(List<String> segmentIds)

  *//**
   * Removes/invalidates all the segments which are less than passed load time.
   ** @param loadTime timestamp which is less than it will be removed.
   * @return Boolean whether deletion success or fail
   *//*
  public boolean deleteSegmentsByLoadTime(long loadTime)

//  /**
//   * It releases the lock and any other resources if it holds.
//   */
  //  public void close()
  //
  //  /**
  //   * While opening the updater, it acquires the metadata lock and returns the
  //   * new SegmentStatusUpdater.
  //   ** @param identifier   identifier of table to read table status.
  //   * @param segmentStore Interface implementation of SegmentStore.
  //   * @return new instance of SegmentStatusUpdater
  //   */
  //  static SegmentStatusUpdater open()


}
