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

package org.apache.carbondata.streaming.segment;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.datastore.page.statistics.SimpleStatsResult;
import org.apache.carbondata.core.locks.CarbonLockFactory;
import org.apache.carbondata.core.locks.ICarbonLock;
import org.apache.carbondata.core.locks.LockUsage;
import org.apache.carbondata.core.metadata.blocklet.index.BlockletMinMaxIndex;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.reader.CarbonIndexFileReader;
import org.apache.carbondata.core.statusmanager.FileFormat;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonMetadataUtil;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.core.util.comparator.Comparator;
import org.apache.carbondata.core.util.comparator.SerializableComparator;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.core.writer.CarbonIndexFileWriter;
import org.apache.carbondata.format.BlockIndex;
import org.apache.carbondata.format.BlockletIndex;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.streaming.CarbonStreamRecordWriter;
import org.apache.carbondata.streaming.index.StreamFileIndex;

import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * streaming segment manager
 */
public class StreamSegment {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(StreamSegment.class.getName());

  /**
   * get stream segment or create new stream segment if not exists
   */
  public static String open(CarbonTable table) throws IOException {
    SegmentStatusManager segmentStatusManager =
        new SegmentStatusManager(table.getAbsoluteTableIdentifier());
    ICarbonLock carbonLock = segmentStatusManager.getTableStatusLock();
    try {
      if (carbonLock.lockWithRetries()) {
        LOGGER.info(
            "Acquired lock for table" + table.getDatabaseName() + "." + table.getTableName()
                + " for stream table get or create segment");

        LoadMetadataDetails[] details =
            SegmentStatusManager.readLoadMetadata(
                CarbonTablePath.getMetadataPath(table.getTablePath()));
        LoadMetadataDetails streamSegment = null;
        for (LoadMetadataDetails detail : details) {
          if (FileFormat.ROW_V1 == detail.getFileFormat()) {
            if (SegmentStatus.STREAMING == detail.getSegmentStatus()) {
              streamSegment = detail;
              break;
            }
          }
        }
        if (null == streamSegment) {
          int segmentId = SegmentStatusManager.createNewSegmentId(details);
          LoadMetadataDetails newDetail = new LoadMetadataDetails();
          newDetail.setPartitionCount("0");
          newDetail.setLoadName("" + segmentId);
          newDetail.setFileFormat(FileFormat.ROW_V1);
          newDetail.setLoadStartTime(System.currentTimeMillis());
          newDetail.setSegmentStatus(SegmentStatus.STREAMING);

          LoadMetadataDetails[] newDetails = new LoadMetadataDetails[details.length + 1];
          int i = 0;
          for (; i < details.length; i++) {
            newDetails[i] = details[i];
          }
          newDetails[i] = newDetail;
          SegmentStatusManager.writeLoadDetailsIntoFile(
              CarbonTablePath.getTableStatusFilePath(table.getTablePath()), newDetails);
          return newDetail.getLoadName();
        } else {
          return streamSegment.getLoadName();
        }
      } else {
        LOGGER.error(
            "Not able to acquire the lock for stream table get or create segment for table " + table
                .getDatabaseName() + "." + table.getTableName());
        throw new IOException("Failed to get stream segment");
      }
    } finally {
      if (carbonLock.unlock()) {
        LOGGER.info("Table unlocked successfully after stream table get or create segment" + table
            .getDatabaseName() + "." + table.getTableName());
      } else {
        LOGGER.error(
            "Unable to unlock table lock for stream table" + table.getDatabaseName() + "." + table
                .getTableName() + " during stream table get or create segment");
      }
    }
  }

  /**
   * marker old stream segment to finished status and create new stream segment
   */
  public static String close(CarbonTable table, String segmentId)
      throws IOException {
    SegmentStatusManager segmentStatusManager =
        new SegmentStatusManager(table.getAbsoluteTableIdentifier());
    ICarbonLock carbonLock = segmentStatusManager.getTableStatusLock();
    try {
      if (carbonLock.lockWithRetries()) {
        LOGGER.info(
            "Acquired lock for table" + table.getDatabaseName() + "." + table.getTableName()
                + " for stream table finish segment");

        LoadMetadataDetails[] details =
            SegmentStatusManager.readLoadMetadata(
                CarbonTablePath.getMetadataPath(table.getTablePath()));
        for (LoadMetadataDetails detail : details) {
          if (segmentId.equals(detail.getLoadName())) {
            detail.setLoadEndTime(System.currentTimeMillis());
            detail.setSegmentStatus(SegmentStatus.STREAMING_FINISH);
            break;
          }
        }

        int newSegmentId = SegmentStatusManager.createNewSegmentId(details);
        LoadMetadataDetails newDetail = new LoadMetadataDetails();
        newDetail.setPartitionCount("0");
        newDetail.setLoadName("" + newSegmentId);
        newDetail.setFileFormat(FileFormat.ROW_V1);
        newDetail.setLoadStartTime(System.currentTimeMillis());
        newDetail.setSegmentStatus(SegmentStatus.STREAMING);

        LoadMetadataDetails[] newDetails = new LoadMetadataDetails[details.length + 1];
        int i = 0;
        for (; i < details.length; i++) {
          newDetails[i] = details[i];
        }
        newDetails[i] = newDetail;
        SegmentStatusManager
            .writeLoadDetailsIntoFile(CarbonTablePath.getTableStatusFilePath(
                table.getTablePath()), newDetails);
        return newDetail.getLoadName();
      } else {
        LOGGER.error(
            "Not able to acquire the lock for stream table status updation for table " + table
                .getDatabaseName() + "." + table.getTableName());
        throw new IOException("Failed to get stream segment");
      }
    } finally {
      if (carbonLock.unlock()) {
        LOGGER.info(
            "Table unlocked successfully after table status updation" + table.getDatabaseName()
                + "." + table.getTableName());
      } else {
        LOGGER.error("Unable to unlock Table lock for table" + table.getDatabaseName() + "." + table
            .getTableName() + " during table status updation");
      }
    }
  }

  /**
   * change the status of the segment from "streaming" to "streaming finish"
   */
  public static void finishStreaming(CarbonTable carbonTable) throws IOException {
    ICarbonLock statusLock = CarbonLockFactory.getCarbonLockObj(
        carbonTable.getTableInfo().getOrCreateAbsoluteTableIdentifier(),
        LockUsage.TABLE_STATUS_LOCK);
    try {
      if (statusLock.lockWithRetries()) {
        LoadMetadataDetails[] details =
            SegmentStatusManager.readLoadMetadata(carbonTable.getMetadataPath());
        boolean updated = false;
        for (LoadMetadataDetails detail : details) {
          if (SegmentStatus.STREAMING == detail.getSegmentStatus()) {
            detail.setLoadEndTime(System.currentTimeMillis());
            detail.setSegmentStatus(SegmentStatus.STREAMING_FINISH);
            updated = true;
          }
        }
        if (updated) {
          SegmentStatusManager.writeLoadDetailsIntoFile(
              CarbonTablePath.getTableStatusFilePath(carbonTable.getTablePath()),
              details);
        }
      } else {
        String msg = "Failed to acquire table status lock of " + carbonTable.getDatabaseName()
            + "." + carbonTable.getTableName();
        LOGGER.error(msg);
        throw new IOException(msg);
      }
    } finally {
      if (statusLock.unlock()) {
        LOGGER.info("Table unlocked successfully after table status updation"
            + carbonTable.getDatabaseName() + "." + carbonTable.getTableName());
      } else {
        LOGGER.error("Unable to unlock Table lock for table " + carbonTable.getDatabaseName()
            + "." + carbonTable.getTableName() + " during table status updation");
      }
    }
  }

  public static BlockletMinMaxIndex collectMinMaxIndex(SimpleStatsResult[] dimStats,
      SimpleStatsResult[] mrsStats) {
    BlockletMinMaxIndex minMaxIndex = new BlockletMinMaxIndex();
    byte[][] maxIndexes = new byte[dimStats.length + mrsStats.length][];
    for (int index = 0; index < dimStats.length; index++) {
      maxIndexes[index] =
          CarbonUtil.getValueAsBytes(dimStats[index].getDataType(), dimStats[index].getMax());
    }
    for (int index = 0; index < mrsStats.length; index++) {
      maxIndexes[dimStats.length + index] =
          CarbonUtil.getValueAsBytes(mrsStats[index].getDataType(), mrsStats[index].getMax());
    }
    minMaxIndex.setMaxValues(maxIndexes);

    byte[][] minIndexes = new byte[maxIndexes.length][];
    for (int index = 0; index < dimStats.length; index++) {
      minIndexes[index] =
          CarbonUtil.getValueAsBytes(dimStats[index].getDataType(), dimStats[index].getMin());
    }
    for (int index = 0; index < mrsStats.length; index++) {
      minIndexes[dimStats.length + index] =
          CarbonUtil.getValueAsBytes(mrsStats[index].getDataType(), mrsStats[index].getMin());
    }
    minMaxIndex.setMinValues(minIndexes);
    // TODO: handle the min max writing for string type based on character limit for streaming
    boolean[] isMinMaxSet = new boolean[dimStats.length + mrsStats.length];
    Arrays.fill(isMinMaxSet, true);
    minMaxIndex.setIsMinMaxSet(isMinMaxSet);
    return minMaxIndex;
  }

  /**
   * create a StreamBlockIndex from the SimpleStatsResult array
   */
  private static StreamFileIndex createStreamBlockIndex(String fileName,
      BlockletMinMaxIndex minMaxIndex, DataType[] msrDataTypes, int blockletRowCount) {
    StreamFileIndex streamFileIndex =
        new StreamFileIndex(fileName, minMaxIndex, blockletRowCount);
    streamFileIndex.setMsrDataTypes(msrDataTypes);
    return streamFileIndex;
  }

  /**
   * invoke CarbonStreamOutputFormat to append batch data to existing carbondata file
   */
  public static StreamFileIndex appendBatchData(CarbonIterator<Object[]> inputIterators,
      TaskAttemptContext job, CarbonLoadModel carbonLoadModel) throws Exception {
    CarbonStreamRecordWriter writer = null;
    try {
      writer = new CarbonStreamRecordWriter(job, carbonLoadModel);
      // at the begin of each task, should recover file if necessary
      // here can reuse some information of record writer
      recoverFileIfRequired(
          writer.getSegmentDir(),
          writer.getFileName(),
          CarbonTablePath.getCarbonStreamIndexFileName());
      int blockletRowCount = 0;
      while (inputIterators.hasNext()) {
        writer.write(null, inputIterators.next());
        blockletRowCount++;
      }
      inputIterators.close();

      return createStreamBlockIndex(writer.getFileName(), writer.getBatchMinMaxIndex(),
          writer.getMeasureDataTypes(), blockletRowCount);
    } catch (Throwable ex) {
      if (writer != null) {
        LOGGER.error(ex, "Failed to append batch data to stream segment: " +
            writer.getSegmentDir());
        writer.setHasException(true);
      }
      throw ex;
    } finally {
      if (writer != null) {
        writer.close(job);
      }
    }
  }

  /**
   * check the health of stream segment and try to recover segment from job fault
   * this method will be invoked in following scenarios.
   * 1. at the begin of the streaming (StreamSinkFactory.getStreamSegmentId)
   * 2. after job failed (CarbonAppendableStreamSink.writeDataFileJob)
   */
  public static void recoverSegmentIfRequired(String segmentDir) throws IOException {
    FileFactory.FileType fileType = FileFactory.getFileType(segmentDir);
    if (FileFactory.isFileExist(segmentDir, fileType)) {
      String indexName = CarbonTablePath.getCarbonStreamIndexFileName();
      String indexPath = segmentDir + File.separator + indexName;
      CarbonFile index = FileFactory.getCarbonFile(indexPath, fileType);
      CarbonFile[] files = listDataFiles(segmentDir, fileType);
      // TODO better to check backup index at first
      // index file exists
      if (index.exists()) {
        // data file exists
        if (files.length > 0) {
          CarbonIndexFileReader indexReader = new CarbonIndexFileReader();
          try {
            // map block index
            indexReader.openThriftReader(indexPath);
            Map<String, Long> tableSizeMap = new HashMap<>();
            while (indexReader.hasNext()) {
              BlockIndex blockIndex = indexReader.readBlockIndexInfo();
              tableSizeMap.put(blockIndex.getFile_name(), blockIndex.getFile_size());
            }
            // recover each file
            for (CarbonFile file : files) {
              Long size = tableSizeMap.get(file.getName());
              if (null == size || size == 0) {
                file.delete();
              } else if (size < file.getSize()) {
                FileFactory.truncateFile(file.getCanonicalPath(), fileType, size);
              }
            }
          } finally {
            indexReader.closeThriftReader();
          }
        }
      } else {
        if (files.length > 0) {
          for (CarbonFile file : files) {
            file.delete();
          }
        }
      }
    }
  }

  /**
   * check the health of stream data file and try to recover data file from task fault
   *  this method will be invoked in following scenarios.
   *  1. at the begin of writing data file task
   */
  public static void recoverFileIfRequired(
      String segmentDir,
      String fileName,
      String indexName) throws IOException {

    FileFactory.FileType fileType = FileFactory.getFileType(segmentDir);
    String filePath = segmentDir + File.separator + fileName;
    CarbonFile file = FileFactory.getCarbonFile(filePath, fileType);
    String indexPath = segmentDir + File.separator + indexName;
    CarbonFile index = FileFactory.getCarbonFile(indexPath, fileType);
    if (file.exists() && index.exists()) {
      CarbonIndexFileReader indexReader = new CarbonIndexFileReader();
      try {
        indexReader.openThriftReader(indexPath);
        while (indexReader.hasNext()) {
          BlockIndex blockIndex = indexReader.readBlockIndexInfo();
          if (blockIndex.getFile_name().equals(fileName)) {
            if (blockIndex.getFile_size() == 0) {
              file.delete();
            } else if (blockIndex.getFile_size() < file.getSize()) {
              FileFactory.truncateFile(filePath, fileType, blockIndex.getFile_size());
            }
            break;
          }
        }
      } finally {
        indexReader.closeThriftReader();
      }
    }
  }


  /**
   * list all carbondata files of a segment
   */
  public static CarbonFile[] listDataFiles(String segmentDir, FileFactory.FileType fileType) {
    CarbonFile carbonDir = FileFactory.getCarbonFile(segmentDir, fileType);
    if (carbonDir.exists()) {
      return carbonDir.listFiles(new CarbonFileFilter() {
        @Override public boolean accept(CarbonFile file) {
          return CarbonTablePath.isCarbonDataFile(file.getName());
        }
      });
    } else {
      return new CarbonFile[0];
    }
  }

  /**
   * read index file to list BlockIndex
   *
   * @param indexPath path of the index file
   * @param fileType  file type of the index file
   * @return the list of BlockIndex in the index file
   * @throws IOException
   */
  public static List<BlockIndex> readIndexFile(String indexPath, FileFactory.FileType fileType)
      throws IOException {
    List<BlockIndex> blockIndexList = new ArrayList<>();
    CarbonFile index = FileFactory.getCarbonFile(indexPath, fileType);
    if (index.exists()) {
      CarbonIndexFileReader indexReader = new CarbonIndexFileReader();
      try {
        indexReader.openThriftReader(indexPath);
        while (indexReader.hasNext()) {
          blockIndexList.add(indexReader.readBlockIndexInfo());
        }
      } finally {
        indexReader.closeThriftReader();
      }
    }
    return blockIndexList;
  }

  /**
   * combine the index of new blocklet and the BlockletMinMaxIndex index of stream file
   * 1. if file index is null, not require Min/Max index
   * 2. if file index is not null,
   * 2.1 if blocklet index is null, use the BlockletMinMaxIndex index of stream
   * 2.2 if blocklet index is not null, combine these two index
   */
  private static void mergeBatchMinMax(StreamFileIndex blockletIndex, BlockletMinMaxIndex fileIndex)
      throws IOException {
    if (fileIndex == null) {
      // backward compatibility
      // it will not create a min/max index for the old stream file(without min/max index).
      blockletIndex.setMinMaxIndex(null);
      return;
    }

    DataType[] msrDataTypes = blockletIndex.getMsrDataTypes();
    SerializableComparator[] comparators = new SerializableComparator[msrDataTypes.length];
    for (int index = 0; index < comparators.length; index++) {
      comparators[index] = Comparator.getComparatorByDataTypeForMeasure(msrDataTypes[index]);
    }

    // min value
    byte[][] minValues = blockletIndex.getMinMaxIndex().getMinValues();
    byte[][] mergedMinValues = fileIndex.getMinValues();
    if (minValues == null || minValues.length == 0) {
      // use file index
      blockletIndex.getMinMaxIndex().setMinValues(mergedMinValues);
    } else if (mergedMinValues != null && mergedMinValues.length != 0) {
      if (minValues.length != mergedMinValues.length) {
        throw new IOException("the lengths of the min values should be same.");
      }
      int dimCount = minValues.length - msrDataTypes.length;
      for (int index = 0; index < minValues.length; index++) {
        if (index < dimCount) {
          if (ByteUtil.UnsafeComparer.INSTANCE.compareTo(minValues[index], mergedMinValues[index])
              > 0) {
            minValues[index] = mergedMinValues[index];
          }
        } else {
          Object object = DataTypeUtil.getMeasureObjectFromDataType(
              minValues[index], msrDataTypes[index - dimCount]);
          Object mergedObject = DataTypeUtil.getMeasureObjectFromDataType(
              mergedMinValues[index], msrDataTypes[index - dimCount]);
          if (comparators[index - dimCount].compare(object, mergedObject) > 0) {
            minValues[index] = mergedMinValues[index];
          }
        }
      }
    }

    // max value
    byte[][] maxValues = blockletIndex.getMinMaxIndex().getMaxValues();
    byte[][] mergedMaxValues = fileIndex.getMaxValues();
    if (maxValues == null || maxValues.length == 0) {
      blockletIndex.getMinMaxIndex().setMaxValues(mergedMaxValues);
    } else if (mergedMaxValues != null && mergedMaxValues.length != 0) {
      if (maxValues.length != mergedMaxValues.length) {
        throw new IOException("the lengths of the max values should be same.");
      }
      int dimCount = maxValues.length - msrDataTypes.length;
      for (int index = 0; index < maxValues.length; index++) {
        if (index < dimCount) {
          if (ByteUtil.UnsafeComparer.INSTANCE.compareTo(maxValues[index], mergedMaxValues[index])
              < 0) {
            maxValues[index] = mergedMaxValues[index];
          }
        } else {
          Object object = DataTypeUtil.getMeasureObjectFromDataType(
              maxValues[index], msrDataTypes[index - dimCount]);
          Object mergedObject = DataTypeUtil.getMeasureObjectFromDataType(
              mergedMaxValues[index], msrDataTypes[index - dimCount]);
          if (comparators[index - dimCount].compare(object, mergedObject) < 0) {
            maxValues[index] = mergedMaxValues[index];
          }
        }
      }
    }
  }

  /**
   * merge blocklet min/max to generate batch min/max
   */
  public static BlockletMinMaxIndex mergeBlockletMinMax(BlockletMinMaxIndex to,
      BlockletMinMaxIndex from, DataType[] msrDataTypes) {
    if (to == null) {
      return from;
    }
    if (from == null) {
      return to;
    }

    SerializableComparator[] comparators = new SerializableComparator[msrDataTypes.length];
    for (int index = 0; index < comparators.length; index++) {
      comparators[index] = Comparator.getComparatorByDataTypeForMeasure(msrDataTypes[index]);
    }

    // min value
    byte[][] minValues = to.getMinValues();
    byte[][] mergedMinValues = from.getMinValues();
    int dimCount1 = minValues.length - msrDataTypes.length;
    for (int index = 0; index < minValues.length; index++) {
      if (index < dimCount1) {
        if (ByteUtil.UnsafeComparer.INSTANCE.compareTo(minValues[index], mergedMinValues[index])
            > 0) {
          minValues[index] = mergedMinValues[index];
        }
      } else {
        Object object = DataTypeUtil.getMeasureObjectFromDataType(
            minValues[index], msrDataTypes[index - dimCount1]);
        Object mergedObject = DataTypeUtil.getMeasureObjectFromDataType(
            mergedMinValues[index], msrDataTypes[index - dimCount1]);
        if (comparators[index - dimCount1].compare(object, mergedObject) > 0) {
          minValues[index] = mergedMinValues[index];
        }
      }
    }

    // max value
    byte[][] maxValues = to.getMaxValues();
    byte[][] mergedMaxValues = from.getMaxValues();
    int dimCount2 = maxValues.length - msrDataTypes.length;
    for (int index = 0; index < maxValues.length; index++) {
      if (index < dimCount2) {
        if (ByteUtil.UnsafeComparer.INSTANCE.compareTo(maxValues[index], mergedMaxValues[index])
            < 0) {
          maxValues[index] = mergedMaxValues[index];
        }
      } else {
        Object object = DataTypeUtil.getMeasureObjectFromDataType(
            maxValues[index], msrDataTypes[index - dimCount2]);
        Object mergedObject = DataTypeUtil.getMeasureObjectFromDataType(
            mergedMaxValues[index], msrDataTypes[index - dimCount2]);
        if (comparators[index - dimCount2].compare(object, mergedObject) < 0) {
          maxValues[index] = mergedMaxValues[index];
        }
      }
    }
    return to;
  }

  /**
   * merge new blocklet index and old file index to create new file index
   */
  private static void updateStreamFileIndex(Map<String, StreamFileIndex> indexMap,
      String indexPath, FileFactory.FileType fileType) throws IOException {
    List<BlockIndex> blockIndexList = readIndexFile(indexPath, fileType);
    for (BlockIndex blockIndex : blockIndexList) {
      BlockletMinMaxIndex fileIndex = CarbonMetadataUtil
          .convertExternalMinMaxIndex(blockIndex.getBlock_index().getMin_max_index());
      StreamFileIndex blockletIndex = indexMap.get(blockIndex.getFile_name());
      if (blockletIndex == null) {
        // should index all stream file
        indexMap.put(blockIndex.getFile_name(),
            new StreamFileIndex(blockIndex.getFile_name(), fileIndex, blockIndex.getNum_rows()));
      } else {
        // merge minMaxIndex into StreamBlockIndex
        blockletIndex.setRowCount(blockletIndex.getRowCount() + blockIndex.getNum_rows());
        mergeBatchMinMax(blockletIndex, fileIndex);
      }
    }
  }

  /**
   * update carbon index file after a stream batch.
   */
  public static void updateIndexFile(String segmentDir,
      StreamFileIndex[] blockIndexes) throws IOException {
    FileFactory.FileType fileType = FileFactory.getFileType(segmentDir);
    String filePath = CarbonTablePath.getCarbonStreamIndexFilePath(segmentDir);
    // update min/max index
    Map<String, StreamFileIndex> indexMap = new HashMap<>();
    for (StreamFileIndex fileIndex : blockIndexes) {
      indexMap.put(fileIndex.getFileName(), fileIndex);
    }
    updateStreamFileIndex(indexMap, filePath, fileType);

    String tempFilePath = filePath + CarbonCommonConstants.TEMPWRITEFILEEXTENSION;
    CarbonIndexFileWriter writer = new CarbonIndexFileWriter();
    try {
      writer.openThriftWriter(tempFilePath);
      CarbonFile[] files = listDataFiles(segmentDir, fileType);
      BlockIndex blockIndex;
      for (CarbonFile file : files) {
        blockIndex = new BlockIndex();
        blockIndex.setFile_name(file.getName());
        blockIndex.setFile_size(file.getSize());
        blockIndex.setOffset(-1);
        // set min/max index
        BlockletIndex blockletIndex = new BlockletIndex();
        blockIndex.setBlock_index(blockletIndex);
        StreamFileIndex streamFileIndex = indexMap.get(blockIndex.getFile_name());
        if (streamFileIndex != null) {
          blockletIndex.setMin_max_index(
              CarbonMetadataUtil.convertMinMaxIndex(streamFileIndex.getMinMaxIndex()));
          blockIndex.setNum_rows(streamFileIndex.getRowCount());
        } else {
          blockIndex.setNum_rows(-1);
        }
        // write block index
        writer.writeThrift(blockIndex);
      }
      writer.close();
      CarbonFile tempFile = FileFactory.getCarbonFile(tempFilePath, fileType);
      if (!tempFile.renameForce(filePath)) {
        throw new IOException(
            "temporary file renaming failed, src=" + tempFilePath + ", dest=" + filePath);
      }
    } catch (IOException ex) {
      try {
        writer.close();
      } catch (IOException t) {
        LOGGER.error(t);
      }
      throw ex;
    }
  }

  /**
   * calculate the size of the segment by the accumulation of data sizes in index file
   */
  public static long size(String segmentDir) throws IOException {
    long size = 0;
    FileFactory.FileType fileType = FileFactory.getFileType(segmentDir);
    if (FileFactory.isFileExist(segmentDir, fileType)) {
      String indexPath = CarbonTablePath.getCarbonStreamIndexFilePath(segmentDir);
      CarbonFile index = FileFactory.getCarbonFile(indexPath, fileType);
      if (index.exists()) {
        CarbonIndexFileReader indexReader = new CarbonIndexFileReader();
        try {
          indexReader.openThriftReader(indexPath);
          while (indexReader.hasNext()) {
            BlockIndex blockIndex = indexReader.readBlockIndexInfo();
            size += blockIndex.getFile_size();
          }
        } finally {
          indexReader.closeThriftReader();
        }
      }
    }
    return size;
  }
}
