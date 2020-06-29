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

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
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
import org.apache.carbondata.core.datastore.block.Distributable;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.index.Segment;
import org.apache.carbondata.core.indexstore.BlockletDetailInfo;
import org.apache.carbondata.core.indexstore.blockletindex.BlockletIndexRowIndexes;
import org.apache.carbondata.core.indexstore.row.IndexRow;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.statusmanager.FileFormat;
import org.apache.carbondata.core.stream.ExtendedByteArrayInputStream;
import org.apache.carbondata.core.stream.ExtendedDataInputStream;
import org.apache.carbondata.core.util.BlockletIndexUtil;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
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

  private String indexWritePath;
  /**
   * validBlockletIds will contain the valid blocklted ids for a given block that contains the data
   * after pruning from driver. These will be used in executor for further pruning of blocklets
   */
  private Set<Integer> validBlockletIds;

  private transient IndexRow indexRow;

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

  /**
   * used in case of index server, all the fields which is required
   * only in case in executor not need to deseralize and will be kept as
   * byte array and duing write method directly it will be written to output stream
   */
  private byte[] serializeData;

  /**
   * start position of fields
   */
  private int offset;

  /**
   * actual length of data
   */
  private int actualLen;

  /**
   * in case of index server block cache no need to write detail info, filepath, blocklet id
   * bucket id to reduce the serialize data size, this parameter will be used to check whether
   * its a index server flow or not
   */
  private boolean writeDetailInfo = true;

  /**
   * TODO remove this code after Index server count(*) optimization
   * only used for index server, once index server handled count star push down
   * below row count is not required
   */
  private int rowCount;

  public CarbonInputSplit() {
    segment = null;
    taskId = "0";
    bucketId = "0";
    blockletId = "0";
    numberOfBlocklets = 0;
    version = CarbonProperties.getInstance().getFormatVersion();
  }

  /**
   * Will be used in case of index server
   * @param serializeLen
   * @param in
   * @param filePath
   * @param allLocation
   * @param blockletId
   * @throws IOException
   */
  public CarbonInputSplit(int serializeLen, DataInput in, String filePath, String[] allLocation,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3447
      String blockletId) throws IOException {
    this.filePath = filePath;
    this.blockletId = blockletId;
    // getting the underline stream to get the actual position of the fileds which won't be
    // deseralize as its used by executor
    ExtendedByteArrayInputStream underlineStream =
        ((ExtendedDataInputStream) in).getUnderlineStream();
    // current position
    int currentPosition = underlineStream.getPosition();
    // number of locations
    short numberOfLocations = in.readShort();
    if (numberOfLocations > 0) {
      // used locations for this split
      this.location = new String[numberOfLocations];
      for (int i = 0; i < location.length; i++) {
        location[i] = allLocation[in.readShort()];
      }
    }
    // get start
    this.start = in.readLong();
    this.length = in.readLong();
    this.version = ColumnarFormatVersion.valueOf(in.readShort());
    // will be removed after count(*) optmization in case of index server
    this.rowCount = in.readInt();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3460
    if (in.readBoolean()) {
      int numberOfDeleteDeltaFiles = in.readInt();
      deleteDeltaFiles = new String[numberOfDeleteDeltaFiles];
      for (int i = 0; i < numberOfDeleteDeltaFiles; i++) {
        deleteDeltaFiles[i] = in.readUTF();
      }
    }
    // after deseralizing required field get the start position of field which will be only used
    // in executor
    int leftoverPosition = underlineStream.getPosition();
    // position of next split
    int newPosition = currentPosition + serializeLen;
    // setting the position to next split
    underlineStream.setPosition(newPosition);
    this.serializeData = underlineStream.getBuffer();
    this.offset = leftoverPosition;
    this.actualLen = serializeLen - (leftoverPosition - currentPosition);
    String taskNo = CarbonTablePath.DataFileUtil.getTaskNo(this.filePath);
    if (taskNo.contains("_")) {
      taskNo = taskNo.split("_")[0];
    }
    this.taskId = taskNo;
    this.bucketId = CarbonTablePath.DataFileUtil.getBucketNo(this.filePath);
  }

  private CarbonInputSplit(String segmentId, String blockletId, String filePath, long start,
      long length, ColumnarFormatVersion version, String[] deleteDeltaFiles,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765
      String indexWritePath) {
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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765
    this.indexWritePath = indexWritePath;
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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765
      long length, ColumnarFormatVersion version, String indexWritePath) {
    return new CarbonInputSplit(segmentId, blockletId, path, start, length, version, null,
        indexWritePath);
  }

  public static List<TableBlockInfo> createBlocks(List<CarbonInputSplit> splitList) {
    List<TableBlockInfo> tableBlockInfoList = new ArrayList<>();
    for (CarbonInputSplit split : splitList) {
      try {
        TableBlockInfo blockInfo =
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3684
            new TableBlockInfo(split.getFilePath(), split.getStart(),
                split.getSegment().toString(), split.getLocations(), split.getLength(),
                split.getVersion(), split.getDeleteDeltaFiles());
        blockInfo.setDetailInfo(split.getDetailInfo());
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765
        blockInfo.setIndexWriterPath(split.indexWritePath);
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
    try {
      TableBlockInfo blockInfo =
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3684
          new TableBlockInfo(inputSplit.getFilePath(),
              inputSplit.getStart(), inputSplit.getSegment().toString(), inputSplit.getLocations(),
              inputSplit.getLength(), inputSplit.getVersion(),
              inputSplit.getDeleteDeltaFiles());
      blockInfo.setDetailInfo(inputSplit.getDetailInfo());
      if (null != inputSplit.getDetailInfo()) {
        blockInfo.setBlockOffset(inputSplit.getDetailInfo().getBlockFooterOffset());
      }
      return blockInfo;
    } catch (IOException e) {
      throw new RuntimeException("fail to get location of split: " + inputSplit, e);
    }
  }

  public String getSegmentId() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3447
    derserializeField();
    if (segment != null) {
      return segment.getSegmentNo();
    } else {
      return null;
    }
  }

  public Segment getSegment() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3447
    derserializeField();
    return segment;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    // if serializeData is not null it means fields which is present below if condition are alredy
    // deserialize  org.apache.carbondata.hadoop.CarbonInputSplit#CarbonInputSplit(
    // int, java.io.DataInput, java.lang.String, java.lang.String[], java.lang.String)
    if (null == serializeData) {
      this.filePath = in.readUTF();
      this.start = in.readLong();
      this.length = in.readLong();
      this.version = ColumnarFormatVersion.valueOf(in.readShort());
      this.rowCount = in.readInt();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3460
      if (in.readBoolean()) {
        int numberOfDeleteDeltaFiles = in.readInt();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3440
        deleteDeltaFiles = new String[numberOfDeleteDeltaFiles];
        for (int i = 0; i < numberOfDeleteDeltaFiles; i++) {
          deleteDeltaFiles[i] = in.readUTF();
        }
      }
      this.bucketId = in.readUTF();
    }
    this.blockletId = in.readUTF();
    this.segment = Segment.toSegment(in.readUTF());
    boolean detailInfoExists = in.readBoolean();
    if (detailInfoExists) {
      detailInfo = new BlockletDetailInfo();
      detailInfo.readFields(in);
    }
    boolean indexWriterPathExists = in.readBoolean();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765
    if (indexWriterPathExists) {
      indexWritePath = in.readUTF();
    }
    int validBlockletIdCount = in.readShort();
    validBlockletIds = new HashSet<>(validBlockletIdCount);
    for (int i = 0; i < validBlockletIdCount; i++) {
      validBlockletIds.add((int) in.readShort());
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    // if serializeData is not null then its a index server flow so write fields
    // which is already deserialize and write serializeData to output stream
    if (null != serializeData) {
      out.writeUTF(filePath);
      out.writeLong(start);
      out.writeLong(length);
      out.writeShort(version.number());
      out.writeInt(rowCount);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3460
      writeDeleteDeltaFile(out);
      out.writeUTF(bucketId);
      out.write(serializeData, offset, actualLen);
      return;
    }
    // please refer writeDetailInfo doc
    if (null != filePath) {
      out.writeUTF(filePath);
    }
    out.writeLong(start);
    out.writeLong(length);
    out.writeShort(version.number());
    //TODO remove this code once count(*) optmization is added in case of index server
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3704
    if (null != indexRow) {
      out.writeInt(this.indexRow.getInt(BlockletIndexRowIndexes.ROW_COUNT_INDEX));
    } else if (null != detailInfo) {
      out.writeInt(this.detailInfo.getRowCount());
    } else {
      out.writeInt(0);
    }
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3460
    writeDeleteDeltaFile(out);
    if (null != bucketId) {
      out.writeUTF(bucketId);
    }
    out.writeUTF(blockletId);
    out.writeUTF(segment.toString());
    // please refer writeDetailInfo doc
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3704
    out.writeBoolean(writeDetailInfo && (detailInfo != null || indexRow != null));
    if (writeDetailInfo && detailInfo != null) {
      detailInfo.write(out);
      // please refer writeDetailInfo doc
    } else if (writeDetailInfo && indexRow != null) {
      writeBlockletDetailsInfo(out);
    }
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765
    out.writeBoolean(indexWritePath != null);
    if (indexWritePath != null) {
      out.writeUTF(indexWritePath);
    }
    out.writeShort(getValidBlockletIds().size());
    for (Integer blockletId : getValidBlockletIds()) {
      out.writeShort(blockletId);
    }
  }

  private void writeDeleteDeltaFile(DataOutput out) throws IOException {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3460
    if (deleteDeltaFiles != null) {
      out.writeBoolean(true);
      out.writeInt(deleteDeltaFiles.length);
      if (null != deleteDeltaFiles) {
        for (int i = 0; i < deleteDeltaFiles.length; i++) {
          out.writeUTF(deleteDeltaFiles[i]);
        }
      }
    } else {
      out.writeBoolean(false);
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

  public String getBlockletId() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3572
    return blockletId;
  }

  @Override
  public int compareTo(Distributable o) {
    if (o == null) {
      return -1;
    }
    CarbonInputSplit other = (CarbonInputSplit) o;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3447
    derserializeField();
    other.derserializeField();
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

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (!(obj instanceof CarbonInputSplit)) {
      return false;
    }
    CarbonInputSplit other = (CarbonInputSplit) obj;
    return 0 == this.compareTo(other);
  }

  @Override
  public int hashCode() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3447
    derserializeField();
    int result = taskId.hashCode();
    result = 31 * result + segment.hashCode();
    result = 31 * result + bucketId.hashCode();
    result = 31 * result + numberOfBlocklets;
    return result;
  }

  @Override
  public String getBlockPath() {
    if (null == blockPath) {
      blockPath = getPath().getName();
    }
    return blockPath;
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

  public void setIndexWritePath(String indexWritePath) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765
    this.indexWritePath = indexWritePath;
  }

  public void setSegment(Segment segment) {
    this.segment = segment;
  }

  public String getIndexWritePath() {
    return indexWritePath;
  }

  public void setIndexRow(IndexRow indexRow) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3704
    this.indexRow = indexRow;
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

  public boolean isBlockCache() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3447
    return this.isBlockCache;
  }

  private void writeBlockletDetailsInfo(DataOutput out) throws IOException {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3704
    out.writeInt(this.indexRow.getInt(BlockletIndexRowIndexes.ROW_COUNT_INDEX));
    if (this.isBlockCache) {
      out.writeShort(0);
    } else {
      out.writeShort(this.indexRow.getShort(BlockletIndexRowIndexes.BLOCKLET_PAGE_COUNT_INDEX));
    }
    out.writeShort(this.indexRow.getShort(BlockletIndexRowIndexes.VERSION_INDEX));
    out.writeShort(Short.parseShort(this.blockletId));
    out.writeLong(this.indexRow.getLong(BlockletIndexRowIndexes.SCHEMA_UPADATED_TIME_INDEX));
    out.writeBoolean(false);
    out.writeLong(this.indexRow.getLong(BlockletIndexRowIndexes.BLOCK_FOOTER_OFFSET));
    // write -1 if columnSchemaBinary is null so that at the time of reading it can distinguish
    // whether schema is written or not
    if (null != this.columnSchema) {
      byte[] columnSchemaBinary = BlockletIndexUtil.convertSchemaToBinary(this.columnSchema);
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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3704
          this.indexRow.getByteArray(BlockletIndexRowIndexes.BLOCKLET_INFO_INDEX);
      out.writeInt(blockletInfoBinary.length);
      out.write(blockletInfoBinary);
    }
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3328
    out.writeLong(getLength());
    out.writeBoolean(this.useMinMaxForPruning);
  }

  public BlockletDetailInfo getDetailInfo() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3704
    if (null != indexRow && detailInfo == null) {
      detailInfo = new BlockletDetailInfo();
      detailInfo
          .setRowCount(this.indexRow.getInt(BlockletIndexRowIndexes.ROW_COUNT_INDEX));
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3447
      rowCount = detailInfo.getRowCount();
      detailInfo
          .setVersionNumber(this.indexRow.getShort(BlockletIndexRowIndexes.VERSION_INDEX));
      detailInfo.setBlockletId(Short.parseShort(this.blockletId));
      detailInfo.setSchemaUpdatedTimeStamp(
          this.indexRow.getLong(BlockletIndexRowIndexes.SCHEMA_UPADATED_TIME_INDEX));
      detailInfo.setBlockFooterOffset(
          this.indexRow.getLong(BlockletIndexRowIndexes.BLOCK_FOOTER_OFFSET));
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3447
      start = detailInfo.getBlockFooterOffset();
      detailInfo
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3328
          .setBlockSize(getLength());
      length = detailInfo.getBlockSize();
      detailInfo.setUseMinMaxForPruning(useMinMaxForPruning);
      if (!this.isBlockCache) {
        detailInfo.setColumnSchemas(this.columnSchema);
        detailInfo.setPagesCount(
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3704
            this.indexRow.getShort(BlockletIndexRowIndexes.BLOCKLET_PAGE_COUNT_INDEX));
        detailInfo.setBlockletInfoBinary(
            this.indexRow.getByteArray(BlockletIndexRowIndexes.BLOCKLET_INFO_INDEX));
      } else {
        detailInfo.setBlockletInfoBinary(new byte[0]);
      }
      if (location == null) {
        try {
          location = new String(indexRow.getByteArray(BlockletIndexRowIndexes.LOCATIONS),
              CarbonCommonConstants.DEFAULT_CHARSET).split(",");
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
      indexRow = null;
    }
    return detailInfo;
  }

  @Override
  public SplitLocationInfo[] getLocationInfo() {
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

  public IndexRow getIndexRow() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3751
    return indexRow;
  }

  public String getFilePath() {
    return this.filePath;
  }

  /** The position of the first byte in the file to process. */
  public long getStart() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3572
    return start;
  }

  /**
   * In case of index server detail info won't be present
   * so footer offsets needs to be written correctly, so updating the length
   *
   */
  public void updateFooteroffset() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3447
    if (isBlockCache && start == 0) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3704
      if (null != indexRow) {
        start = this.indexRow.getLong(BlockletIndexRowIndexes.BLOCK_FOOTER_OFFSET);
      } else if (null != detailInfo) {
        start = detailInfo.getBlockFooterOffset();
      }
    }
  }

  public void updateBlockLength() {
    if (length == -1) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3704
      if (null != indexRow) {
        length = this.indexRow.getLong(BlockletIndexRowIndexes.BLOCK_LENGTH);
      } else if (null != detailInfo) {
        length = detailInfo.getBlockSize();
      }
    }
  }

  @Override
  public long getLength() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3447
    updateBlockLength();
    return length;
  }

  @Override
  public String toString() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3572
    return filePath + ":" + start + "+" + length;
  }

  @Override
  public String[] getLocations() throws IOException {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3704
    if (this.location == null && indexRow == null) {
      return new String[] {};
    } else if (indexRow != null) {
      location = new String(indexRow.getByteArray(BlockletIndexRowIndexes.LOCATIONS),
          CarbonCommonConstants.DEFAULT_CHARSET).split(",");
    }
    return this.location;
  }

  public void setLocation(String[] location) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3337
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3306
    this.location = location;
  }

  /**
   * In case of index server block cache no need to write details info as its
   * heavy object.
   * @param writeDetailInfo
   */
  public void setWriteDetailInfo(boolean writeDetailInfo) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3447
    this.writeDetailInfo = writeDetailInfo;
  }

  /**
   * Below method will be used to serialize the input split in case of
   * index server
   * @param out
   * @param uniqueLocationMap
   * @throws IOException
   */
  public void serializeFields(DataOutput out, Map<String, Short> uniqueLocationMap)
      throws IOException {
    final String[] locations = getLocations();
    if (null != locations) {
      out.writeShort(locations.length);
      // below code is to get the unique locations across all the block
      for (String loc : locations) {
        Short pos = uniqueLocationMap.get(loc);
        if (null == pos) {
          pos = (short) uniqueLocationMap.size();
          uniqueLocationMap.put(loc, pos);
        }
        out.writeShort(pos);
      }
    } else {
      out.writeShort(0);
    }
    write(out);
  }

  /**
   * This method will be used to deserialize fields
   * in case of index server
   */
  private void derserializeField() {
    if (null != serializeData) {
      DataInputStream in = null;
      try {
        ByteArrayInputStream bis = new ByteArrayInputStream(serializeData, offset, actualLen);
        in = new DataInputStream(bis);
        readFields(in);
        serializeData = null;
      } catch (IOException e) {
        throw new RuntimeException(e);
      } finally {
        if (null != in) {
          CarbonUtil.closeStreams(in);
        }
      }
    }
  }

  public int getRowCount() {
    return rowCount;
  }

  public void setStart(long start) {
    this.start = start;
  }

  public void setFilePath(String filePath) {
    this.filePath = filePath;
  }

  public void setBucketId(String bucketId) {
    this.bucketId = bucketId;
  }

}
