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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datastore.block.BlockletInfos;
import org.apache.carbondata.core.datastore.block.Distributable;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.indexstore.BlockletDetailInfo;
import org.apache.carbondata.core.indexstore.blockletindex.BlockletDataMapRowIndexes;
import org.apache.carbondata.core.indexstore.row.DataMapRow;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.statusmanager.FileFormat;
import org.apache.carbondata.core.util.BlockletDataMapUtil;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.hadoop.internal.index.Block;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.SplitLocationInfo;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Carbon input split to allow distributed read of CarbonTableInputFormat.
 */
public class CarbonInputSplit extends FileSplit
    implements Distributable, Serializable, Writable, Block {

  private static final long serialVersionUID = 3520344046772190207L;
  public String taskId;

  private Segment segment;

  private String bucketId;

  private String blockletId;

  /*
   * Number of BlockLets in a block
   */
  private int numberOfBlocklets;

  private ColumnarFormatVersion version;

  /**
   * list of delete delta files for split
   */
  private String[] deleteDeltaFiles;

  private BlockletDetailInfo detailInfo;

  private FileFormat fileFormat = FileFormat.COLUMNAR_V3;

  private String dataMapWritePath;
  /**
   * validBlockletIds will contain the valid blocklted ids for a given block that contains the data
   * after pruning from driver. These will be used in executor for further pruning of blocklets
   */
  private Set<Integer> validBlockletIds;

  private transient DataMapRow dataMapRow;

  private transient int[] columnCardinality;

  private transient boolean isLegacyStore;

  private transient List<ColumnSchema> columnSchema;

  private boolean useMinMaxForPruning = true;

  private boolean isBlockCache = true;

  private String filePath;

  private long start;

  private long length;

  private String[] location;

  private transient SplitLocationInfo[] hostInfos;

  private transient Path path;

  private transient String blockPath;

  public CarbonInputSplit() {
    segment = null;
    taskId = "0";
    bucketId = "0";
    blockletId = "0";
    numberOfBlocklets = 0;
    version = CarbonProperties.getInstance().getFormatVersion();
  }

  private CarbonInputSplit(String segmentId, String blockletId, String filePath, long start,
      long length, ColumnarFormatVersion version, String[] deleteDeltaFiles,
      String dataMapWritePath) {
    this.filePath = filePath;
    this.start = start;
    this.length = length;
    if (null != segmentId) {
      this.segment = Segment.toSegment(segmentId);
    }
    String taskNo = CarbonTablePath.DataFileUtil.getTaskNo(this.filePath);
    if (taskNo.contains("_")) {
      taskNo = taskNo.split("_")[0];
    }
    this.taskId = taskNo;
    this.bucketId = CarbonTablePath.DataFileUtil.getBucketNo(this.filePath);
    this.blockletId = blockletId;
    this.version = version;
    this.deleteDeltaFiles = deleteDeltaFiles;
    this.dataMapWritePath = dataMapWritePath;
  }

  public CarbonInputSplit(String segmentId, String blockletId, String filePath, long start,
      long length, String[] locations, int numberOfBlocklets, ColumnarFormatVersion version,
      String[] deleteDeltaFiles) {
    this(segmentId, blockletId, filePath, start, length, version, deleteDeltaFiles, null);
    this.location = locations;
    this.numberOfBlocklets = numberOfBlocklets;
  }
  public CarbonInputSplit(String segmentId, String filePath, long start, long length,
      String[] locations, FileFormat fileFormat) {
    this.filePath = filePath;
    this.start = start;
    this.length = length;
    this.location = locations;
    this.segment = Segment.toSegment(segmentId);
    this.fileFormat = fileFormat;
    taskId = "0";
    bucketId = "0";
    blockletId = "0";
    numberOfBlocklets = 0;
    version = CarbonProperties.getInstance().getFormatVersion();
  }

  public CarbonInputSplit(String segmentId, String filePath, long start, long length,
      String[] locations, String[] inMemoryHosts, FileFormat fileFormat) {
    this.filePath = filePath;
    this.start = start;
    this.length = length;
    this.location = locations;
    this.hostInfos = new SplitLocationInfo[inMemoryHosts.length];
    for (int i = 0; i < inMemoryHosts.length; i++) {
      // because N will be tiny, scanning is probably faster than a HashSet
      boolean inMemory = false;
      for (String inMemoryHost : inMemoryHosts) {
        if (inMemoryHost.equals(inMemoryHosts[i])) {
          inMemory = true;
          break;
        }
      }
      hostInfos[i] = new SplitLocationInfo(inMemoryHosts[i], inMemory);
    }
    this.segment = Segment.toSegment(segmentId);
    this.fileFormat = fileFormat;
    taskId = "0";
    bucketId = "0";
    blockletId = "0";
    numberOfBlocklets = 0;
    version = CarbonProperties.getInstance().getFormatVersion();
  }

  public static CarbonInputSplit from(String segmentId, String blockletId, String path, long start,
      long length, ColumnarFormatVersion version, String dataMapWritePath) throws IOException {
    return new CarbonInputSplit(segmentId, blockletId, path, start, length, version, null,
        dataMapWritePath);
  }

  public static List<TableBlockInfo> createBlocks(List<CarbonInputSplit> splitList) {
    List<TableBlockInfo> tableBlockInfoList = new ArrayList<>();
    for (CarbonInputSplit split : splitList) {
      BlockletInfos blockletInfos =
          new BlockletInfos(split.getNumberOfBlocklets(), 0, split.getNumberOfBlocklets());
      try {
        TableBlockInfo blockInfo =
            new TableBlockInfo(split.getFilePath(), split.blockletId, split.getStart(),
                split.getSegment().toString(), split.getLocations(), split.getLength(),
                blockletInfos, split.getVersion(), split.getDeleteDeltaFiles());
        blockInfo.setDetailInfo(split.getDetailInfo());
        blockInfo.setDataMapWriterPath(split.dataMapWritePath);
        if (split.getDetailInfo() != null) {
          blockInfo.setBlockOffset(split.getDetailInfo().getBlockFooterOffset());
        }
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
          new TableBlockInfo(inputSplit.getFilePath(), inputSplit.blockletId,
              inputSplit.getStart(), inputSplit.getSegment().toString(), inputSplit.getLocations(),
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
    if (segment != null) {
      return segment.getSegmentNo();
    } else {
      return null;
    }
  }

  public Segment getSegment() {
    return segment;
  }


  @Override public void readFields(DataInput in) throws IOException {
    this.filePath = in.readUTF();
    this.start = in.readLong();
    this.length = in.readLong();
    this.segment = Segment.toSegment(in.readUTF());
    this.version = ColumnarFormatVersion.valueOf(in.readShort());
    this.bucketId = in.readUTF();
    this.blockletId = in.readUTF();
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
    int validBlockletIdCount = in.readShort();
    validBlockletIds = new HashSet<>(validBlockletIdCount);
    for (int i = 0; i < validBlockletIdCount; i++) {
      validBlockletIds.add((int) in.readShort());
    }
  }

  @Override public void write(DataOutput out) throws IOException {
    out.writeUTF(filePath);
    out.writeLong(start);
    out.writeLong(length);
    out.writeUTF(segment.toString());
    out.writeShort(version.number());
    out.writeUTF(bucketId);
    out.writeUTF(blockletId);
    out.writeInt(null != deleteDeltaFiles ? deleteDeltaFiles.length : 0);
    if (null != deleteDeltaFiles) {
      for (int i = 0; i < deleteDeltaFiles.length; i++) {
        out.writeUTF(deleteDeltaFiles[i]);
      }
    }
    out.writeBoolean(detailInfo != null || dataMapRow != null);
    if (detailInfo != null) {
      detailInfo.write(out);
    } else if (dataMapRow != null) {
      writeBlockletDetailsInfo(out);
    }
    out.writeBoolean(dataMapWritePath != null);
    if (dataMapWritePath != null) {
      out.writeUTF(dataMapWritePath);
    }
    out.writeShort(getValidBlockletIds().size());
    for (Integer blockletId : getValidBlockletIds()) {
      out.writeShort(blockletId);
    }
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

    double seg1 = Double.parseDouble(segment.getSegmentNo());
    double seg2 = Double.parseDouble(other.segment.getSegmentNo());
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
    String filePath1 = this.getFilePath();
    String filePath2 = other.getFilePath();
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
    result = 31 * result + segment.hashCode();
    result = 31 * result + bucketId.hashCode();
    result = 31 * result + numberOfBlocklets;
    return result;
  }

  @Override public String getBlockPath() {
    if (null == blockPath) {
      blockPath = getPath().getName();
    }
    return blockPath;
  }

  @Override public List<Long> getMatchedBlocklets() {
    return null;
  }

  @Override public boolean fullScan() {
    return true;
  }

  /**
   * returns map of blocklocation and storage id
   *
   * @return
   */
  public Map<String, String> getBlockStorageIdMap() {
    return new HashMap<>();
  }

  public String[] getDeleteDeltaFiles() {
    return deleteDeltaFiles;
  }

  public void setDeleteDeltaFiles(String[] deleteDeltaFiles) {
    this.deleteDeltaFiles = deleteDeltaFiles;
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

  public Set<Integer> getValidBlockletIds() {
    if (null == validBlockletIds) {
      validBlockletIds = new HashSet<>();
    }
    return validBlockletIds;
  }

  public void setValidBlockletIds(Set<Integer> validBlockletIds) {
    this.validBlockletIds = validBlockletIds;
  }

  public void setDataMapWritePath(String dataMapWritePath) {
    this.dataMapWritePath = dataMapWritePath;
  }

  public void setSegment(Segment segment) {
    this.segment = segment;
  }

  public String getDataMapWritePath() {
    return dataMapWritePath;
  }

  public void setDataMapRow(DataMapRow dataMapRow) {
    this.dataMapRow = dataMapRow;
  }

  public void setColumnCardinality(int[] columnCardinality) {
    this.columnCardinality = columnCardinality;
  }

  public void setLegacyStore(boolean legacyStore) {
    isLegacyStore = legacyStore;
  }

  public void setColumnSchema(List<ColumnSchema> columnSchema) {
    this.columnSchema = columnSchema;
  }

  public void setUseMinMaxForPruning(boolean useMinMaxForPruning) {
    this.useMinMaxForPruning = useMinMaxForPruning;
  }

  public void setIsBlockCache(boolean isBlockCache) {
    this.isBlockCache = isBlockCache;
  }

  private void writeBlockletDetailsInfo(DataOutput out) throws IOException {
    out.writeInt(this.dataMapRow.getInt(BlockletDataMapRowIndexes.ROW_COUNT_INDEX));
    if (this.isBlockCache) {
      out.writeShort(0);
    } else {
      out.writeShort(this.dataMapRow.getShort(BlockletDataMapRowIndexes.BLOCKLET_PAGE_COUNT_INDEX));
    }
    out.writeShort(this.dataMapRow.getShort(BlockletDataMapRowIndexes.VERSION_INDEX));
    out.writeShort(Short.parseShort(this.blockletId));
    out.writeShort(this.columnCardinality.length);
    for (int i = 0; i < this.columnCardinality.length; i++) {
      out.writeInt(this.columnCardinality[i]);
    }
    out.writeLong(this.dataMapRow.getLong(BlockletDataMapRowIndexes.SCHEMA_UPADATED_TIME_INDEX));
    out.writeBoolean(false);
    out.writeLong(this.dataMapRow.getLong(BlockletDataMapRowIndexes.BLOCK_FOOTER_OFFSET));
    // write -1 if columnSchemaBinary is null so that at the time of reading it can distinguish
    // whether schema is written or not
    if (null != this.columnSchema) {
      byte[] columnSchemaBinary = BlockletDataMapUtil.convertSchemaToBinary(this.columnSchema);
      out.writeInt(columnSchemaBinary.length);
      out.write(columnSchemaBinary);
    } else {
      // write -1 if columnSchemaBinary is null so that at the time of reading it can distinguish
      // whether schema is written or not
      out.writeInt(-1);
    }
    if (this.isBlockCache) {
      out.writeInt(0);
      out.write(new byte[0]);
    } else {
      byte[] blockletInfoBinary =
          this.dataMapRow.getByteArray(BlockletDataMapRowIndexes.BLOCKLET_INFO_INDEX);
      out.writeInt(blockletInfoBinary.length);
      out.write(blockletInfoBinary);
    }
    out.writeLong(getLength());
    out.writeBoolean(this.isLegacyStore);
    out.writeBoolean(this.useMinMaxForPruning);
  }

  public BlockletDetailInfo getDetailInfo() {
    if (null != dataMapRow && detailInfo == null) {
      detailInfo = new BlockletDetailInfo();
      detailInfo
          .setRowCount(this.dataMapRow.getInt(BlockletDataMapRowIndexes.ROW_COUNT_INDEX));
      detailInfo
          .setVersionNumber(this.dataMapRow.getShort(BlockletDataMapRowIndexes.VERSION_INDEX));
      detailInfo.setBlockletId(Short.parseShort(this.blockletId));
      detailInfo.setDimLens(this.columnCardinality);
      detailInfo.setSchemaUpdatedTimeStamp(
          this.dataMapRow.getLong(BlockletDataMapRowIndexes.SCHEMA_UPADATED_TIME_INDEX));
      detailInfo.setBlockFooterOffset(
          this.dataMapRow.getLong(BlockletDataMapRowIndexes.BLOCK_FOOTER_OFFSET));
      detailInfo
          .setBlockSize(getLength());
      detailInfo.setLegacyStore(isLegacyStore);
      detailInfo.setUseMinMaxForPruning(useMinMaxForPruning);
      if (!this.isBlockCache) {
        detailInfo.setColumnSchemas(this.columnSchema);
        detailInfo.setPagesCount(
            this.dataMapRow.getShort(BlockletDataMapRowIndexes.BLOCKLET_PAGE_COUNT_INDEX));
        detailInfo.setBlockletInfoBinary(
            this.dataMapRow.getByteArray(BlockletDataMapRowIndexes.BLOCKLET_INFO_INDEX));
      } else {
        detailInfo.setBlockletInfoBinary(new byte[0]);
      }
      if (location == null) {
        try {
          location = new String(dataMapRow.getByteArray(BlockletDataMapRowIndexes.LOCATIONS),
              CarbonCommonConstants.DEFAULT_CHARSET).split(",");
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
      dataMapRow = null;
    }
    return detailInfo;
  }

  @Override
  public SplitLocationInfo[] getLocationInfo() throws IOException {
    return hostInfos;
  }

  /**
   * The file containing this split's data.
   */
  public Path getPath() {
    if (path == null) {
      path = new Path(filePath);
      return path;
    }
    return path;
  }

  public String getFilePath() {
    return this.filePath;
  }

  /** The position of the first byte in the file to process. */
  public long getStart() { return start; }

  @Override
  public long getLength() {
    if (length == -1) {
      if (null != dataMapRow) {
        length = this.dataMapRow.getLong(BlockletDataMapRowIndexes.BLOCK_LENGTH);
      } else if (null != detailInfo) {
        length = detailInfo.getBlockSize();
      }
    }
    return length;
  }

  @Override
  public String toString() { return filePath + ":" + start + "+" + length; }

  @Override public String[] getLocations() throws IOException {
    if (this.location == null && dataMapRow == null) {
      return new String[] {};
    } else if (dataMapRow != null) {
      location = new String(dataMapRow.getByteArray(BlockletDataMapRowIndexes.LOCATIONS),
          CarbonCommonConstants.DEFAULT_CHARSET).split(",");
    }
    return this.location;
  }
}
