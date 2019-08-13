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

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.indexstore.BlockletDetailInfo;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.core.util.path.CarbonTablePath.DataFileUtil;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

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
   * file size of the block
   */
  private long fileSize;

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
  private Segment segment;

  /**
   * id of the Blocklet.
   */
  private String blockletId;

  private String[] locations;

  private ColumnarFormatVersion version;

  /**
   * flag to determine whether the data block is from old store (version 1.1)
   * or current store
   */
  private boolean isDataBlockFromOldStore;
  /**
   * The class holds the blockletsinfo
   */
  private BlockletInfos blockletInfos = new BlockletInfos();

  /**
   * map of block location and storage id
   */
  private Map<String, String> blockStorageIdMap =
      new HashMap<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

  /**
   * delete delta files path for this block
   */
  private String[] deletedDeltaFilePath;

  private BlockletDetailInfo detailInfo;

  private String dataMapWriterPath;

  private transient DataFileFooter dataFileFooter;

  /**
   * true when index file does't have blocklet information
   */
  private boolean isLegacyStore;

  /**
   * comparator to sort by block size in descending order.
   * Since each line is not exactly the same, the size of a InputSplit may differs,
   * so we allow some deviation for these splits.
   */
  public static final Comparator<Distributable> DATA_SIZE_DESC_COMPARATOR =
      new Comparator<Distributable>() {
        @Override public int compare(Distributable o1, Distributable o2) {
          long diff =
              ((TableBlockInfo) o1).getBlockLength() - ((TableBlockInfo) o2).getBlockLength();
          return diff < 0 ? 1 : (diff == 0 ? 0 : -1);
        }
      };

  public TableBlockInfo(String filePath, long blockOffset, String segmentId,
      String[] locations, long blockLength, ColumnarFormatVersion version,
      String[] deletedDeltaFilePath) {
    this.filePath = FileFactory.getUpdatedFilePath(filePath);
    this.blockletId = "0";
    this.blockOffset = blockOffset;
    this.segment = Segment.toSegment(segmentId);
    this.locations = locations;
    this.blockLength = blockLength;
    this.version = version;
    this.deletedDeltaFilePath = deletedDeltaFilePath;
  }

  public TableBlockInfo() {

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
      long blockLength, BlockletInfos blockletInfos, ColumnarFormatVersion version,
      String[] deletedDeltaFilePath) {
    this(filePath, blockOffset, segmentId, locations, blockLength, version,
        deletedDeltaFilePath);
    this.blockletInfos = blockletInfos;
  }

  /**
   * constructor to initialize the TableBlockInfo with blockletIds
   *
   * @param filePath
   * @param blockOffset
   * @param segmentId
   * @param locations
   * @param blockLength
   * @param blockletInfos
   */
  public TableBlockInfo(String filePath, String blockletId, long blockOffset, String segmentId,
      String[] locations, long blockLength, BlockletInfos blockletInfos,
      ColumnarFormatVersion version, String[] deletedDeltaFilePath) {
    this(filePath, blockOffset, segmentId, locations, blockLength, blockletInfos, version,
        deletedDeltaFilePath);
    this.blockletId = blockletId;
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
  public TableBlockInfo(String filePath, String blockletId, long blockOffset, String segmentId,
      String[] locations, long blockLength, BlockletInfos blockletInfos,
      ColumnarFormatVersion version, Map<String, String> blockStorageIdMap,
      String[] deletedDeltaFilePath) {
    this(filePath, blockletId, blockOffset, segmentId, locations, blockLength, blockletInfos,
        version, deletedDeltaFilePath);
    this.blockStorageIdMap = blockStorageIdMap;
  }

  /**
   * Create copy of TableBlockInfo object
   */
  public TableBlockInfo copy() {
    TableBlockInfo info = new TableBlockInfo();
    info.filePath = filePath;
    info.blockOffset = blockOffset;
    info.blockLength = blockLength;
    info.segment = segment;
    info.blockletId = blockletId;
    info.locations = locations;
    info.version = version;
    info.isDataBlockFromOldStore = isDataBlockFromOldStore;
    info.blockletInfos = blockletInfos;
    info.blockStorageIdMap = blockStorageIdMap;
    info.deletedDeltaFilePath = deletedDeltaFilePath;
    info.detailInfo = detailInfo.copy();
    info.dataMapWriterPath = dataMapWriterPath;
    info.isLegacyStore = isLegacyStore;
    return info;
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
    if (segment == null) {
      return null;
    } else {
      return segment.getSegmentNo();
    }
  }

  public Segment getSegment() {
    return segment;
  }

  /**
   * @return the blockLength
   */
  public long getBlockLength() {
    if (blockLength == 0) {
      Path path = new Path(filePath);
      try {
        FileSystem fs = path.getFileSystem(FileFactory.getConfiguration());
        blockLength = fs.listStatus(path)[0].getLen();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
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
    if (!segment.equals(other.segment)) {
      return false;
    }
    if (blockOffset != other.blockOffset) {
      return false;
    }
    if (blockLength != other.blockLength) {
      return false;
    }

    if (null == filePath || null == other.filePath) {
      return  false;
    }

    if (!filePath.equals(other.filePath)) {
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

    double seg1 = Double.parseDouble(segment.getSegmentNo());
    double seg2 = Double.parseDouble(((TableBlockInfo) other).segment.getSegmentNo());
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
      int compare = ByteUtil.compare(DataFileUtil.getTaskNo(filePath)
              .getBytes(Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET)),
          DataFileUtil.getTaskNo(((TableBlockInfo) other).filePath)
              .getBytes(Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET)));
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
    result = 31 * result + segment.hashCode();
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

  public String[] getDeletedDeltaFilePath() {
    return deletedDeltaFilePath;
  }

  public void setFilePath(String filePath) {
    this.filePath = filePath;
  }

  public long getFileSize() {
    return fileSize;
  }

  public void setFileSize(long fileSize) {
    this.fileSize = fileSize;
  }

  public BlockletDetailInfo getDetailInfo() {
    return detailInfo;
  }

  public void setDetailInfo(BlockletDetailInfo detailInfo) {
    this.detailInfo = detailInfo;
  }

  public String getBlockletId() {
    return blockletId;
  }

  public void setBlockletId(String blockletId) {
    this.blockletId = blockletId;
  }

  public boolean isDataBlockFromOldStore() {
    return isDataBlockFromOldStore;
  }

  public void setDataBlockFromOldStore(boolean dataBlockFromOldStore) {
    isDataBlockFromOldStore = dataBlockFromOldStore;
  }

  public String getDataMapWriterPath() {
    return dataMapWriterPath;
  }

  public void setDataMapWriterPath(String dataMapWriterPath) {
    this.dataMapWriterPath = dataMapWriterPath;
  }

  public DataFileFooter getDataFileFooter() {
    return dataFileFooter;
  }

  public void setDataFileFooter(DataFileFooter dataFileFooter) {
    this.dataFileFooter = dataFileFooter;
  }

  public boolean isLegacyStore() {
    return isLegacyStore;
  }

  public void setLegacyStore(boolean legacyStore) {
    isLegacyStore = legacyStore;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("TableBlockInfo{");
    sb.append("filePath='").append(filePath).append('\'');
    sb.append(", blockOffset=").append(blockOffset);
    sb.append(", blockLength=").append(blockLength);
    sb.append(", segment='").append(segment.toString()).append('\'');
    sb.append(", blockletId='").append(blockletId).append('\'');
    sb.append(", locations=").append(Arrays.toString(locations));
    sb.append('}');
    return sb.toString();
  }
}
