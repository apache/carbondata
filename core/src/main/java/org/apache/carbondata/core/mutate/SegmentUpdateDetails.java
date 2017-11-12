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

package org.apache.carbondata.core.mutate;

import java.io.Serializable;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.statusmanager.SegmentStatus;

/**
 * This class stores the segment details of table update status file
 */
public class SegmentUpdateDetails implements Serializable {

  private static final long serialVersionUID = 1206104914918491724L;
  private String segmentName;
  private String blockName;
  private SegmentStatus segmentStatus;
  private String deleteDeltaEndTimestamp = "";
  private String deleteDeltaStartTimestamp = "";
  private String actualBlockName;
  private String deletedRowsInBlock = "0";

  /**
   * LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(SegmentUpdateDetails.class.getName());

  public String getDeleteDeltaEndTimestamp() {
    return deleteDeltaEndTimestamp;
  }

  public void setDeleteDeltaEndTimestamp(String deleteDeltaEndTimestamp) {
    this.deleteDeltaEndTimestamp = deleteDeltaEndTimestamp;
  }

  public String getSegmentName() {
    return segmentName;
  }

  public void setSegmentName(String segmentName) {
    this.segmentName = segmentName;
  }

  public String getBlockName() {
    return blockName;
  }

  public void setBlockName(String blockName) {
    this.blockName = blockName;
  }

  public String getDeleteDeltaStartTimestamp() {
    return deleteDeltaStartTimestamp;
  }

  public void setDeleteDeltaStartTimestamp(String deleteDeltaStartTimestamp) {
    this.deleteDeltaStartTimestamp = deleteDeltaStartTimestamp;
  }

  public void setSegmentStatus(SegmentStatus segmentStatus) {
    this.segmentStatus = segmentStatus;
  }

  public SegmentStatus getSegmentStatus() {
    return this.segmentStatus;
  }

  @Override public int hashCode() {
    final int prime = 31;
    int result = segmentName.hashCode();
    result = prime * result + blockName.hashCode();
    return result;
  }

  @Override public boolean equals(Object obj) {
    if (obj == null) {
      return false;

    }
    if (!(obj instanceof SegmentUpdateDetails)) {
      return false;
    }
    SegmentUpdateDetails other = (SegmentUpdateDetails) obj;
    if (segmentName == null) {
      if (other.segmentName != null) {
        return false;
      }
    } else if (!segmentName.equals(other.segmentName)) {
      return false;
    }
    if (blockName == null) {
      if (other.blockName != null) {
        return false;
      }
    } else if (!blockName.equals(other.blockName)) {
      return false;
    }
    return true;
  }

  /**
   * return deleteDeltaTime as long
   *
   * @return
   */
  public long getDeleteDeltaEndTimeAsLong() {
    return getTimeStampAsLong(deleteDeltaEndTimestamp);
  }

  /**
   * return deleteDeltaTime as long
   *
   * @return
   */
  public long getDeleteDeltaStartTimeAsLong() {

    return getTimeStampAsLong(deleteDeltaStartTimestamp);
  }

  /**
   * returns complete block name
   *
   * @return
   */
  public String getActualBlockName() {
    return actualBlockName;
  }

  public void setActualBlockName(String actualBlockName) {
    this.actualBlockName = actualBlockName;
  }

  /**
   * returns timestamp as long value
   *
   * @param timtstamp
   * @return
   */
  private Long getTimeStampAsLong(String timtstamp) {
    long longValue = 0;
    try {
      longValue = Long.parseLong(timtstamp);
    } catch (NumberFormatException nfe) {
      if (LOGGER.isDebugEnabled()) {
        String errorMsg = "Invalid timestamp : " + timtstamp;
        LOGGER.debug(errorMsg);
      }
    }
    return longValue;
  }

  public String getDeletedRowsInBlock() {
    return deletedRowsInBlock;
  }

  public void setDeletedRowsInBlock(String deletedRowsInBlock) {
    this.deletedRowsInBlock = deletedRowsInBlock;
  }
}
