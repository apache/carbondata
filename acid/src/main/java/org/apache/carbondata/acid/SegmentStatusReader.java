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

import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.locks.CarbonLockFactory;
import org.apache.carbondata.core.locks.ICarbonLock;
import org.apache.carbondata.core.locks.LockUsage;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.LiteralExpression;
import org.apache.carbondata.core.scan.expression.conditional.EqualToExpression;
import org.apache.carbondata.core.scan.expression.conditional.InExpression;
import org.apache.carbondata.core.scan.expression.conditional.ListExpression;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.util.path.CarbonTablePath;

/**
 * Reader for segment/table status/ files.
 * It uses a segment store interface to read the segment status.
 */
public class SegmentStatusReader {

  private final AbsoluteTableIdentifier identifier;
  private final SegmentStore segmentStore; // can be singleton

  /**
   * Constructor to create SegmentStatusReader.
   *
   * @param identifier   of table to read table status
   * @param segmentStore Interface implementation of SegmentStore
   */
  SegmentStatusReader(AbsoluteTableIdentifier identifier, SegmentStore segmentStore) {
    this.identifier = identifier;
    this.segmentStore = segmentStore;
  }
}

  /*
   * It gives the valid segments from the store at this point of time.
   *
   * @param identifier
   * @return

  public SegmentDetailVO getSegment(String segmentId) {
    List<Expression> filters = new ArrayList<>();
    filters.add(new EqualToExpression(new ColumnExpression("tableId", DataTypes.STRING),
        new LiteralExpression(identifier.getCarbonTableIdentifier().getTableId(),
            DataTypes.STRING)));
    filters.add(
        new EqualToExpression(new ColumnExpression(SegmentDetailVO.SEGMENT_ID, DataTypes.STRING),
            new LiteralExpression(segmentId, DataTypes.STRING)));
    // TODO: why list here ?
    List<SegmentDetailVO> segments = segmentStore.getSegments(identifier, filters);
    if (segments.size() > 0) {
      //TODO: when it can be more than one ?
      return segments.get(0);
    }
    return null;
  }

  *
   * It gives the valid segments from the store at this point of time.
   *
   * @param identifier
   * @return

  public SegmentsHolder getValidSegments() {
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

  *
   * It gives the valid segments from the store at this point of time.
   *
   * @param identifier
   * @return

  public SegmentsHolder getInvalidSegments() {
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

    // load metadetails is not exposed to user, db guys no need of it. only for file based it is used.
  }

  *
   * It gives the valid segments from the store at this point of time.
   *
   * @param identifier
   * @return

  public SegmentsHolder getAllSegments() {
    List<Expression> filters = new ArrayList<>();
    filters.add(new EqualToExpression(new ColumnExpression("tableId", DataTypes.STRING),
        new LiteralExpression(identifier.getCarbonTableIdentifier().getTableId(),
            DataTypes.STRING)));
    List<SegmentDetailVO> allSegments = segmentStore.getSegments(identifier, filters);
    return new SegmentsHolder(allSegments);
  }

  *
   * It gives the history segments from the store.
   *
   * @param identifier
   * @return

  public SegmentsHolder getAllHistorySegments() {
    // TODO
    return new SegmentsHolder(new ArrayList<SegmentDetailVO>());
  }

  *
   * Return true if the specified `loadName` is in progress, by checking the load lock.

  public static Boolean isLoadInProgress(String loadName) {
    ICarbonLock segmentLock = CarbonLockFactory.getCarbonLockObj(,
        CarbonTablePath.addSegmentPrefix(loadName) + LockUsage.LOCK);
    try {
      return !segmentLock.lockWithRetries(1, 0);
    } finally {
      segmentLock.unlock();
    }
  }



  //  TODO:
  //  public boolean isOverwriteInProgressInTable()
  //  public Boolean isCompactionInProgress()
  //  public long getTableStatusLastModifiedTime()

}

*/
