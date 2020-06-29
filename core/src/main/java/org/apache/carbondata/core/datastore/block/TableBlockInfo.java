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

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.index.Segment;
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

  private String[] locations;

  private ColumnarFormatVersion version;

  /**
   * delete delta files path for this block
   */
  private String[] deletedDeltaFilePath;

  private BlockletDetailInfo detailInfo;

  private String indexWriterPath;

  private transient DataFileFooter dataFileFooter;

  /**
   * comparator to sort by block size in descending order.
   * Since each line is not exactly the same, the size of a InputSplit may differs,
   * so we allow some deviation for these splits.
   */
  public static final Comparator<Distributable> DATA_SIZE_DESC_COMPARATOR =
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2023
      new Comparator<Distributable>() {
        @Override
        public int compare(Distributable o1, Distributable o2) {
          long diff =
              ((TableBlockInfo) o1).getBlockLength() - ((TableBlockInfo) o2).getBlockLength();
          return diff < 0 ? 1 : (diff == 0 ? 0 : -1);
        }
      };

  public TableBlockInfo(String filePath, long blockOffset, String segmentId,
      String[] locations, long blockLength, ColumnarFormatVersion version,
      String[] deletedDeltaFilePath) {
    this.filePath = FileFactory.getUpdatedFilePath(filePath);
    this.blockOffset = blockOffset;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2428
    this.segment = Segment.toSegment(segmentId);
    this.locations = locations;
    this.blockLength = blockLength;
    this.version = version;
    this.deletedDeltaFilePath = deletedDeltaFilePath;
  }

  public TableBlockInfo() {

  }

  /**
   * Create copy of TableBlockInfo object
   */
  public TableBlockInfo copy() {
    TableBlockInfo info = new TableBlockInfo();
    info.filePath = filePath;
    info.blockOffset = blockOffset;
    info.blockLength = blockLength;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2428
    info.segment = segment;
    info.locations = locations;
    info.version = version;
    info.deletedDeltaFilePath = deletedDeltaFilePath;
    info.detailInfo = detailInfo.copy();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765
    info.indexWriterPath = indexWriterPath;
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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2428
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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1537
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
  @Override
  public boolean equals(Object obj) {
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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2428
    if (!segment.equals(other.segment)) {
      return false;
    }
    if (blockOffset != other.blockOffset) {
      return false;
    }
    if (blockLength != other.blockLength) {
      return false;
    }

//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1386
    if (null == filePath || null == other.filePath) {
      return  false;
    }

//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3659
    if (!new Path(filePath).equals(new Path(other.filePath))) {
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
  @Override
  public int compareTo(Distributable other) {

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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1326
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
    return 0;
  }

  @Override
  public int hashCode() {
    int result = filePath.hashCode();
    result = 31 * result + (int) (blockOffset ^ (blockOffset >>> 32));
    result = 31 * result + (int) (blockLength ^ (blockLength >>> 32));
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2428
    result = 31 * result + segment.hashCode();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3684
    result = 31 * result;
    return result;
  }

  @Override
  public String[] getLocations() {
    return locations;
  }

  public ColumnarFormatVersion getVersion() {
    return version;
  }

  public void setVersion(ColumnarFormatVersion version) {
    this.version = version;
  }

  public String[] getDeletedDeltaFilePath() {
    return deletedDeltaFilePath;
  }

  public void setFilePath(String filePath) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1232
    this.filePath = filePath;
  }

  public long getFileSize() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3523
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

  public String getIndexWriterPath() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765
    return indexWriterPath;
  }

  public void setIndexWriterPath(String indexWriterPath) {
    this.indexWriterPath = indexWriterPath;
  }

  public DataFileFooter getDataFileFooter() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3200
    return dataFileFooter;
  }

  public void setDataFileFooter(DataFileFooter dataFileFooter) {
    this.dataFileFooter = dataFileFooter;
  }

  @Override
  public String toString() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2023
    final StringBuilder sb = new StringBuilder("TableBlockInfo{");
    sb.append("filePath='").append(filePath).append('\'');
    sb.append(", blockOffset=").append(blockOffset);
    sb.append(", blockLength=").append(blockLength);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2428
    sb.append(", segment='").append(segment.toString()).append('\'');
    sb.append(", locations=").append(Arrays.toString(locations));
    sb.append('}');
    return sb.toString();
  }
}
