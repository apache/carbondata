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
package org.apache.carbondata.hive;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.block.BlockletInfos;
import org.apache.carbondata.core.datastore.block.Distributable;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.mutate.UpdateVO;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.hadoop.internal.index.Block;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;

public class CarbonHiveInputSplit extends FileSplit
    implements Distributable, Serializable, Writable, Block {

  private static final long serialVersionUID = 3520344046772190208L;
  public String taskId;

  private String segmentId;

  private String bucketId;
  /*
   * Invalid segments that need to be removed in task side index
   */
  private List<String> invalidSegments;

  /*
   * Number of BlockLets in a block
   */
  private int numberOfBlocklets;

  private ColumnarFormatVersion version;

  /**
   * map of blocklocation and storage id
   */
  private Map<String, String> blockStorageIdMap =
      new HashMap<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

  private List<UpdateVO> invalidTimestampsList;

  public CarbonHiveInputSplit() {
    segmentId = null;
    taskId = "0";
    bucketId = "0";
    numberOfBlocklets = 0;
    invalidSegments = new ArrayList<>();
    version = CarbonProperties.getInstance().getFormatVersion();
  }

  public CarbonHiveInputSplit(String segmentId, Path path, long start, long length,
      String[] locations, ColumnarFormatVersion version) {
    super(path, start, length, locations);
    this.segmentId = segmentId;
    this.taskId = CarbonTablePath.DataFileUtil.getTaskNo(path.getName());
    this.bucketId = CarbonTablePath.DataFileUtil.getBucketNo(path.getName());
    this.invalidSegments = new ArrayList<>();
    this.version = version;
  }

  public CarbonHiveInputSplit(String segmentId, Path path, long start, long length,
      String[] locations, int numberOfBlocklets, ColumnarFormatVersion version) {
    this(segmentId, path, start, length, locations, version);
    this.numberOfBlocklets = numberOfBlocklets;
  }

  /**
   * Constructor to initialize the CarbonInputSplit with blockStorageIdMap
   *
   * @param segmentId
   * @param path
   * @param start
   * @param length
   * @param locations
   * @param numberOfBlocklets
   * @param version
   * @param blockStorageIdMap
   */
  public CarbonHiveInputSplit(String segmentId, Path path, long start, long length,
      String[] locations, int numberOfBlocklets, ColumnarFormatVersion version,
      Map<String, String> blockStorageIdMap) {
    this(segmentId, path, start, length, locations, numberOfBlocklets, version);
    this.blockStorageIdMap = blockStorageIdMap;
  }

  public static CarbonHiveInputSplit from(String segmentId, FileSplit split,
                                          ColumnarFormatVersion version)
    throws IOException {
    return new CarbonHiveInputSplit(segmentId, split.getPath(), split.getStart(), split.getLength(),
      split.getLocations(), version);
  }

  public static List<TableBlockInfo> createBlocks(List<CarbonHiveInputSplit> splitList) {
    List<TableBlockInfo> tableBlockInfoList = new ArrayList<>();
    for (CarbonHiveInputSplit split : splitList) {
      BlockletInfos blockletInfos =
          new BlockletInfos(split.getNumberOfBlocklets(), 0, split.getNumberOfBlocklets());
      try {
        tableBlockInfoList.add(
            new TableBlockInfo(split.getPath().toString(), split.getStart(), split.getSegmentId(),
            split.getLocations(), split.getLength(), blockletInfos, split.getVersion()));
      } catch (IOException e) {
        throw new RuntimeException("fail to get location of split: " + split, e);
      }
    }
    return tableBlockInfoList;
  }

  public static TableBlockInfo getTableBlockInfo(CarbonHiveInputSplit inputSplit) {
    BlockletInfos blockletInfos =
        new BlockletInfos(inputSplit.getNumberOfBlocklets(), 0, inputSplit.getNumberOfBlocklets());
    try {
      return new TableBlockInfo(inputSplit.getPath().toString(), inputSplit.getStart(),
        inputSplit.getSegmentId(), inputSplit.getLocations(), inputSplit.getLength(),
        blockletInfos, inputSplit.getVersion());
    } catch (IOException e) {
      throw new RuntimeException("fail to get location of split: " + inputSplit, e);
    }
  }

  public String getSegmentId() {
    return segmentId;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    this.segmentId = in.readUTF();
    this.version = ColumnarFormatVersion.valueOf(in.readShort());
    this.bucketId = in.readUTF();
    int numInvalidSegment = in.readInt();
    invalidSegments = new ArrayList<>(numInvalidSegment);
    for (int i = 0; i < numInvalidSegment; i++) {
      invalidSegments.add(in.readUTF());
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeUTF(segmentId);
    out.writeShort(version.number());
    out.writeUTF(bucketId);
    out.writeInt(invalidSegments.size());
    for (String invalidSegment : invalidSegments) {
      out.writeUTF(invalidSegment);
    }
  }

  public List<String> getInvalidSegments() {
    return invalidSegments;
  }

  public void setInvalidSegments(List<String> invalidSegments) {
    this.invalidSegments = invalidSegments;
  }

  public void setInvalidTimestampRange(List<UpdateVO> invalidTimestamps) {
    invalidTimestampsList = invalidTimestamps;
  }

  public List<UpdateVO> getInvalidTimestampRange() {
    return invalidTimestampsList;
  }

  /**
   * returns the number of blocklets
   *
   * @return
   */
  public int getNumberOfBlocklets() {
    return numberOfBlocklets;
  }

  public ColumnarFormatVersion getVersion() {
    return version;
  }

  public void setVersion(ColumnarFormatVersion version) {
    this.version = version;
  }

  public String getBucketId() {
    return bucketId;
  }

  @Override
  public int compareTo(Distributable o) {
    if (o == null) {
      return -1;
    }
    CarbonHiveInputSplit other = (CarbonHiveInputSplit) o;
    int compareResult = 0;
    // get the segment id
    // converr seg ID to double.

    double seg1 = Double.parseDouble(segmentId);
    double seg2 = Double.parseDouble(other.getSegmentId());
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
    String filePath1 = this.getPath().getName();
    String filePath2 = other.getPath().getName();
    if (CarbonTablePath.isCarbonDataFile(filePath1)) {
      int firstTaskId = Integer.parseInt(CarbonTablePath.DataFileUtil.getTaskNo(filePath1));
      int otherTaskId = Integer.parseInt(CarbonTablePath.DataFileUtil.getTaskNo(filePath2));
      if (firstTaskId != otherTaskId) {
        return firstTaskId - otherTaskId;
      }

      int firstBucketNo = Integer.parseInt(CarbonTablePath.DataFileUtil.getBucketNo(filePath1));
      int otherBucketNo = Integer.parseInt(CarbonTablePath.DataFileUtil.getBucketNo(filePath2));
      if (firstBucketNo != otherBucketNo) {
        return firstBucketNo - otherBucketNo;
      }

      // compare the part no of both block info
      int firstPartNo = Integer.parseInt(CarbonTablePath.DataFileUtil.getPartNo(filePath1));
      int SecondPartNo = Integer.parseInt(CarbonTablePath.DataFileUtil.getPartNo(filePath2));
      compareResult = firstPartNo - SecondPartNo;
    } else {
      compareResult = filePath1.compareTo(filePath2);
    }
    if (compareResult != 0) {
      return compareResult;
    }
    return 0;
  }

  @Override
  public String getBlockPath() {
    return getPath().getName();
  }

  @Override
  public List<Long> getMatchedBlocklets() {
    return null;
  }

  @Override
  public boolean fullScan() {
    return true;
  }

  /**
   * returns map of blocklocation and storage id
   *
   * @return
   */
  public Map<String, String> getBlockStorageIdMap() {
    return blockStorageIdMap;
  }
}
