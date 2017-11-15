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
package org.apache.carbondata.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datastore.block.BlockletInfos;
import org.apache.carbondata.core.datastore.block.Distributable;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.indexstore.BlockletDetailInfo;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.mutate.UpdateVO;
import org.apache.carbondata.core.statusmanager.FileFormat;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.hadoop.internal.index.Block;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Carbon input split to allow distributed read of CarbonTableInputFormat.
 */
public class CarbonInputSplit extends FileSplit
    implements Distributable, Serializable, Writable, Block {

  private static final long serialVersionUID = 3520344046772190207L;
  public String taskId;

  private String segmentId;

  private String bucketId;

  private String blockletId;
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

  /**
   * list of delete delta files for split
   */
  private String[] deleteDeltaFiles;

  private BlockletDetailInfo detailInfo;

  private FileFormat fileFormat = FileFormat.COLUMNAR_V3;

  private String dataMapWritePath;

  public CarbonInputSplit() {
    segmentId = null;
    taskId = "0";
    bucketId = "0";
    blockletId = "0";
    numberOfBlocklets = 0;
    invalidSegments = new ArrayList<>();
    version = CarbonProperties.getInstance().getFormatVersion();
  }

  private CarbonInputSplit(String segmentId, String blockletId, Path path, long start, long length,
      String[] locations, ColumnarFormatVersion version, String[] deleteDeltaFiles,
      String dataMapWritePath) {
    super(path, start, length, locations);
    this.segmentId = segmentId;
    String taskNo = CarbonTablePath.DataFileUtil.getTaskNo(path.getName());
    if (taskNo.contains("_")) {
      taskNo = taskNo.split("_")[0];
    }
    this.taskId = taskNo;
    this.bucketId = CarbonTablePath.DataFileUtil.getBucketNo(path.getName());
    this.blockletId = blockletId;
    this.invalidSegments = new ArrayList<>();
    this.version = version;
    this.deleteDeltaFiles = deleteDeltaFiles;
    this.dataMapWritePath = dataMapWritePath;
  }

  public CarbonInputSplit(String segmentId, String blockletId, Path path, long start, long length,
      String[] locations, int numberOfBlocklets, ColumnarFormatVersion version,
      String[] deleteDeltaFiles) {
    this(segmentId, blockletId, path, start, length, locations, version, deleteDeltaFiles, null);
    this.numberOfBlocklets = numberOfBlocklets;
  }

  public CarbonInputSplit(String segmentId, Path path, long start, long length, String[] locations,
      FileFormat fileFormat) {
    super(path, start, length, locations);
    this.segmentId = segmentId;
    this.fileFormat = fileFormat;
    taskId = "0";
    bucketId = "0";
    blockletId = "0";
    numberOfBlocklets = 0;
    invalidSegments = new ArrayList<>();
    version = CarbonProperties.getInstance().getFormatVersion();
  }

  public CarbonInputSplit(String segmentId, Path path, long start, long length, String[] locations,
      String[] inMemoryHosts, FileFormat fileFormat) {
    super(path, start, length, locations, inMemoryHosts);
    this.segmentId = segmentId;
    this.fileFormat = fileFormat;
    taskId = "0";
    bucketId = "0";
    blockletId = "0";
    numberOfBlocklets = 0;
    invalidSegments = new ArrayList<>();
    version = CarbonProperties.getInstance().getFormatVersion();
  }

  /**
   * Constructor to initialize the CarbonInputSplit with blockStorageIdMap
   * @param segmentId
   * @param path
   * @param start
   * @param length
   * @param locations
   * @param numberOfBlocklets
   * @param version
   * @param blockStorageIdMap
   */
  public CarbonInputSplit(String segmentId, String blockletId, Path path, long start, long length,
      String[] locations, int numberOfBlocklets, ColumnarFormatVersion version,
      Map<String, String> blockStorageIdMap, String[] deleteDeltaFiles) {
    this(segmentId, blockletId, path, start, length, locations, numberOfBlocklets, version,
        deleteDeltaFiles);
    this.blockStorageIdMap = blockStorageIdMap;
  }

  public static CarbonInputSplit from(String segmentId, String blockletId, FileSplit split,
      ColumnarFormatVersion version, String dataMapWritePath) throws IOException {
    return new CarbonInputSplit(segmentId, blockletId, split.getPath(), split.getStart(),
        split.getLength(), split.getLocations(), version, null, dataMapWritePath);
  }

  public static List<TableBlockInfo> createBlocks(List<CarbonInputSplit> splitList) {
    List<TableBlockInfo> tableBlockInfoList = new ArrayList<>();
    for (CarbonInputSplit split : splitList) {
      BlockletInfos blockletInfos =
          new BlockletInfos(split.getNumberOfBlocklets(), 0, split.getNumberOfBlocklets());
      try {
        TableBlockInfo blockInfo =
            new TableBlockInfo(split.getPath().toString(), split.blockletId, split.getStart(),
                split.getSegmentId(), split.getLocations(), split.getLength(), blockletInfos,
                split.getVersion(), split.getDeleteDeltaFiles());
        blockInfo.setDetailInfo(split.getDetailInfo());
        blockInfo.setDataMapWriterPath(split.dataMapWritePath);
        blockInfo.setBlockOffset(split.getDetailInfo().getBlockFooterOffset());
        tableBlockInfoList.add(blockInfo);
      } catch (IOException e) {
        throw new RuntimeException("fail to get location of split: " + split, e);
      }
    }
    return tableBlockInfoList;
  }

  public static TableBlockInfo getTableBlockInfo(CarbonInputSplit inputSplit) {
    BlockletInfos blockletInfos =
        new BlockletInfos(inputSplit.getNumberOfBlocklets(), 0, inputSplit.getNumberOfBlocklets());
    try {
      TableBlockInfo blockInfo =
          new TableBlockInfo(inputSplit.getPath().toString(), inputSplit.blockletId,
              inputSplit.getStart(), inputSplit.getSegmentId(), inputSplit.getLocations(),
              inputSplit.getLength(), blockletInfos, inputSplit.getVersion(),
              inputSplit.getDeleteDeltaFiles());
      blockInfo.setDetailInfo(inputSplit.getDetailInfo());
      blockInfo.setBlockOffset(inputSplit.getDetailInfo().getBlockFooterOffset());
      return blockInfo;
    } catch (IOException e) {
      throw new RuntimeException("fail to get location of split: " + inputSplit, e);
    }
  }

  public String getSegmentId() {
    return segmentId;
  }

  @Override public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    this.segmentId = in.readUTF();
    this.version = ColumnarFormatVersion.valueOf(in.readShort());
    this.bucketId = in.readUTF();
    this.blockletId = in.readUTF();
    int numInvalidSegment = in.readInt();
    invalidSegments = new ArrayList<>(numInvalidSegment);
    for (int i = 0; i < numInvalidSegment; i++) {
      invalidSegments.add(in.readUTF());
    }
    int numberOfDeleteDeltaFiles = in.readInt();
    deleteDeltaFiles = new String[numberOfDeleteDeltaFiles];
    for (int i = 0; i < numberOfDeleteDeltaFiles; i++) {
      deleteDeltaFiles[i] = in.readUTF();
    }
    boolean detailInfoExists = in.readBoolean();
    if (detailInfoExists) {
      detailInfo = new BlockletDetailInfo();
      detailInfo.readFields(in);
    }
    boolean dataMapWriterPathExists = in.readBoolean();
    if (dataMapWriterPathExists) {
      dataMapWritePath = in.readUTF();
    }
  }

  @Override public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeUTF(segmentId);
    out.writeShort(version.number());
    out.writeUTF(bucketId);
    out.writeUTF(blockletId);
    out.writeInt(invalidSegments.size());
    for (String invalidSegment : invalidSegments) {
      out.writeUTF(invalidSegment);
    }
    out.writeInt(null != deleteDeltaFiles ? deleteDeltaFiles.length : 0);
    if (null != deleteDeltaFiles) {
      for (int i = 0; i < deleteDeltaFiles.length; i++) {
        out.writeUTF(deleteDeltaFiles[i]);
      }
    }
    out.writeBoolean(detailInfo != null);
    if (detailInfo != null) {
      detailInfo.write(out);
    }
    out.writeBoolean(dataMapWritePath != null);
    if (dataMapWritePath != null) {
      out.writeUTF(dataMapWritePath);
    }
  }

  public List<String> getInvalidSegments() {
    return invalidSegments;
  }

  public void setInvalidSegments(List<Segment> invalidSegments) {
    List<String> invalidSegmentIds = new ArrayList<>();
    for (Segment segment: invalidSegments) {
      invalidSegmentIds.add(segment.getSegmentNo());
    }
    this.invalidSegments = invalidSegmentIds;
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

  public String getBlockletId() { return blockletId; }

  @Override public int compareTo(Distributable o) {
    if (o == null) {
      return -1;
    }
    CarbonInputSplit other = (CarbonInputSplit) o;
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
      byte[] firstTaskId = CarbonTablePath.DataFileUtil.getTaskNo(filePath1)
          .getBytes(Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
      byte[] otherTaskId = CarbonTablePath.DataFileUtil.getTaskNo(filePath2)
          .getBytes(Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
      int compare = ByteUtil.compare(firstTaskId, otherTaskId);
      if (compare != 0) {
        return compare;
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

  @Override public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (!(obj instanceof CarbonInputSplit)) {
      return false;
    }
    CarbonInputSplit other = (CarbonInputSplit) obj;
    return 0 == this.compareTo(other);
  }

  @Override public int hashCode() {
    int result = taskId.hashCode();
    result = 31 * result + segmentId.hashCode();
    result = 31 * result + bucketId.hashCode();
    result = 31 * result + invalidSegments.hashCode();
    result = 31 * result + numberOfBlocklets;
    return result;
  }

  @Override public String getBlockPath() {
    return getPath().getName();
  }

  @Override public List<Long> getMatchedBlocklets() {
    return null;
  }

  @Override public boolean fullScan() {
    return true;
  }

  /**
   * returns map of blocklocation and storage id
   * @return
   */
  public Map<String, String> getBlockStorageIdMap() {
    return blockStorageIdMap;
  }

  public String[] getDeleteDeltaFiles() {
    return deleteDeltaFiles;
  }

  public void setDeleteDeltaFiles(String[] deleteDeltaFiles) {
    this.deleteDeltaFiles = deleteDeltaFiles;
  }

  public BlockletDetailInfo getDetailInfo() {
    return detailInfo;
  }

  public void setDetailInfo(BlockletDetailInfo detailInfo) {
    this.detailInfo = detailInfo;
  }

  public FileFormat getFileFormat() {
    return fileFormat;
  }

  public void setFormat(FileFormat fileFormat) {
    this.fileFormat = fileFormat;
  }
}
