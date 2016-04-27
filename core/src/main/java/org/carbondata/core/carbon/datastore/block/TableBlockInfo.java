/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.carbondata.core.carbon.datastore.block;

import java.io.Serializable;

import org.carbondata.core.carbon.path.CarbonTablePath.DataFileUtil;

/**
 * class will be used to pass the block detail detail will be passed form driver
 * to all the executor to load the b+ tree
 */
public class TableBlockInfo implements Serializable, Comparable<TableBlockInfo> {

  /**
   * serialization id
   */
  private static final long serialVersionUID = -6502868998599821172L;

  /**
   * full qualified file path of the block
   */
  private String filePath;

  /**
   * block offset in the file
   */
  private long blockOffset;

  /**
   * id of the segment this will be used to sort the blocks
   */
  private int segmentId;

  /**
   * locations to store the host name and other detail
   */
  private String[] locations;

  public TableBlockInfo(String filePath, long blockOffset, int segmentId, String[] locations) {
    this.filePath = filePath;
    this.blockOffset = blockOffset;
    this.segmentId = segmentId;
    this.locations = locations;
  }

  /**
   * @return the filePath
   */
  public String getFilePath() {
    return filePath;
  }

  /**
   * @param filePath the filePath to set
   */
  public void setFilePath(String filePath) {
    this.filePath = filePath;
  }

  /**
   * @return the blockOffset
   */
  public long getBlockOffset() {
    return blockOffset;
  }

  /**
   * @param blockOffset the blockOffset to set
   */
  public void setBlockOffset(long blockOffset) {
    this.blockOffset = blockOffset;
  }

  /**
   * @return the segmentId
   */
  public int getSegmentId() {
    return segmentId;
  }

  /**
   * @return the locations
   */
  public String[] getLocations() {
    return locations;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.lang.Object#hashCode()
   */
  @Override public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (segmentId ^ (segmentId >>> 32));
    result = prime * result + (int) (blockOffset ^ (blockOffset >>> 32));
    result = prime * result + ((filePath == null) ? 0 : filePath.hashCode());
    return result;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof TableBlockInfo)) {
      return false;
    }
    TableBlockInfo other = (TableBlockInfo) obj;
    if (segmentId != other.segmentId) {
      return false;
    }
    if (blockOffset != other.blockOffset) {
      return false;
    }
    if (filePath == null) {
      if (other.filePath != null) {
        return false;
      }
    } else if (!filePath.equals(other.filePath)) {
      return false;
    }
    return true;
  }

  /**
   * Below method will used to compare to TableBlockInfos object this will
   * used for sorting Comparison logic is: 1. compare segment id if segment id
   * is same 2. compare task id if task id is same 3. compare offsets of the
   * block
   */
  @Override public int compareTo(TableBlockInfo other) {

    int compareResult = 0;
    // get the segment id
    compareResult = segmentId - other.segmentId;
    if (compareResult != 0) {
      return compareResult;
    }
    // Comparing the time task id of the file to other
    // if both the task id of the file is same then we need to compare the
    // offset of
    // the file
    String firstTaskId = DataFileUtil.getTaskNo(filePath);
    String otherTaskId = DataFileUtil.getTaskNo(filePath);
    if (firstTaskId.compareTo(otherTaskId) < 1) {
      return 1;
    } else if (firstTaskId.compareTo(otherTaskId) > 1) {
      return -1;
    }
    // offset, no sure about the current structure of the
    // file name
    if (blockOffset < other.blockOffset) {
      return 1;
    } else if (blockOffset > other.blockOffset) {
      return -1;
    }
    return 0;
  }
}
