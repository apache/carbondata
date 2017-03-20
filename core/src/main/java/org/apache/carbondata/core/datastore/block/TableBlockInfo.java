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
package org.apache.carbondata.core.datastore.block;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.core.util.path.CarbonTablePath.DataFileUtil;

/**
 * class will be used to pass the block detail detail will be passed form driver
 * to all the executor to load the b+ tree
 */
public class TableBlockInfo implements Distributable, Serializable {

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
   * length of the block
   */
  private long blockLength;

  /**
   * id of the segment this will be used to sort the blocks
   */
  private String segmentId;

  private String[] locations;

  private ColumnarFormatVersion version;
  /**
   * The class holds the blockletsinfo
   */
  private BlockletInfos blockletInfos = new BlockletInfos();

  /**
   * map of block location and storage id
   */
  private Map<String, String> blockStorageIdMap =
          new HashMap<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

  public TableBlockInfo(String filePath, long blockOffset, String segmentId, String[] locations,
      long blockLength, ColumnarFormatVersion version) {
    this.filePath = FileFactory.getUpdatedFilePath(filePath);
    this.blockOffset = blockOffset;
    this.segmentId = segmentId;
    this.locations = locations;
    this.blockLength = blockLength;
    this.version = version;
  }

  /**
   * constructor to initialize the TbaleBlockInfo with BlockletInfos
   *
   * @param filePath
   * @param blockOffset
   * @param segmentId
   * @param locations
   * @param blockLength
   * @param blockletInfos
   */
  public TableBlockInfo(String filePath, long blockOffset, String segmentId, String[] locations,
      long blockLength, BlockletInfos blockletInfos, ColumnarFormatVersion version) {
    this(filePath, blockOffset, segmentId, locations, blockLength, version);
    this.blockletInfos = blockletInfos;
  }

  /**
   * constructor to initialize the TableBlockInfo with blockStorageIdMap
   *
   * @param filePath
   * @param blockOffset
   * @param segmentId
   * @param locations
   * @param blockLength
   * @param blockletInfos
   * @param version
   * @param blockStorageIdMap
   */
  public TableBlockInfo(String filePath, long blockOffset, String segmentId, String[] locations,
      long blockLength, BlockletInfos blockletInfos, ColumnarFormatVersion version,
      Map<String, String> blockStorageIdMap) {
    this(filePath, blockOffset, segmentId, locations, blockLength, blockletInfos, version);
    this.blockStorageIdMap = blockStorageIdMap;
  }

  /**
   * @return the filePath
   */
  public String getFilePath() {
    return filePath;
  }

  /**
   * @return the blockOffset
   */
  public long getBlockOffset() {
    return blockOffset;
  }

  public void setBlockOffset(long blockOffset) {
    this.blockOffset = blockOffset;
  }

  /**
   * @return the segmentId
   */
  public String getSegmentId() {
    return segmentId;
  }

  /**
   * @return the blockLength
   */
  public long getBlockLength() {
    return blockLength;
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
    if (!segmentId.equals(other.segmentId)) {
      return false;
    }
    if (blockOffset != other.blockOffset) {
      return false;
    }
    if (blockLength != other.blockLength) {
      return false;
    }
    if (filePath == null && other.filePath != null) {
      return false;
    } else if (filePath != null && other.filePath == null) {
      return false;
    } else if (!filePath.equals(other.filePath)) {
      return false;
    }
    if (blockletInfos.getStartBlockletNumber() != other.blockletInfos.getStartBlockletNumber()) {
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
  @Override public int compareTo(Distributable other) {

    int compareResult = 0;
    // get the segment id
    // converr seg ID to double.

    double seg1 = Double.parseDouble(segmentId);
    double seg2 = Double.parseDouble(((TableBlockInfo) other).segmentId);
    if (seg1 - seg2 < 0) {
      return -1;
    }
    if (seg1 - seg2 > 0) {
      return 1;
    }

    // Comparing the time task id of the file to other
    // if both the task id of the file is same then we need to compare the
    // offset of
    // the file
    if (CarbonTablePath.isCarbonDataFile(filePath)) {
      int compare = ByteUtil.compare(DataFileUtil.getTaskNo(filePath).getBytes(),
          DataFileUtil.getTaskNo(((TableBlockInfo) other).filePath).getBytes());
      if (compare != 0) {
        return compare;
      }
      // compare the part no of both block info
      int firstPartNo = Integer.parseInt(DataFileUtil.getPartNo(filePath));
      int SecondPartNo =
          Integer.parseInt(DataFileUtil.getPartNo(((TableBlockInfo) other).filePath));
      compareResult = firstPartNo - SecondPartNo;
    } else {
      compareResult = filePath.compareTo(((TableBlockInfo) other).getFilePath());
    }
    if (compareResult != 0) {
      return compareResult;
    }
    //compare result is not 0 then return
    // if part no is also same then compare the offset and length of the block
    if (blockOffset + blockLength
        < ((TableBlockInfo) other).blockOffset + ((TableBlockInfo) other).blockLength) {
      return -1;
    } else if (blockOffset + blockLength
        > ((TableBlockInfo) other).blockOffset + ((TableBlockInfo) other).blockLength) {
      return 1;
    }
    //compare the startBlockLetNumber
    int diffStartBlockLetNumber =
        blockletInfos.getStartBlockletNumber() - ((TableBlockInfo) other).blockletInfos
            .getStartBlockletNumber();
    if (diffStartBlockLetNumber < 0) {
      return -1;
    }
    if (diffStartBlockLetNumber > 0) {
      return 1;
    }
    return 0;
  }

  @Override public int hashCode() {
    int result = filePath.hashCode();
    result = 31 * result + (int) (blockOffset ^ (blockOffset >>> 32));
    result = 31 * result + (int) (blockLength ^ (blockLength >>> 32));
    result = 31 * result + segmentId.hashCode();
    result = 31 * result + blockletInfos.getStartBlockletNumber();
    return result;
  }

  @Override public String[] getLocations() {
    return locations;
  }

  /**
   * returns BlockletInfos
   *
   * @return
   */
  public BlockletInfos getBlockletInfos() {
    return blockletInfos;
  }

  /**
   * set the blocklestinfos
   *
   * @param blockletInfos
   */
  public void setBlockletInfos(BlockletInfos blockletInfos) {
    this.blockletInfos = blockletInfos;
  }

  public ColumnarFormatVersion getVersion() {
    return version;
  }

  public void setVersion(ColumnarFormatVersion version) {
    this.version = version;
  }

  /**
   * returns the storage location vs storage id map
   *
   * @return
   */
  public Map<String, String> getBlockStorageIdMap() {
    return this.blockStorageIdMap;
  }

  /**
   * method to storage location vs storage id map
   *
   * @param blockStorageIdMap
   */
  public void setBlockStorageIdMap(Map<String, String> blockStorageIdMap) {
    this.blockStorageIdMap = blockStorageIdMap;
  }
}
