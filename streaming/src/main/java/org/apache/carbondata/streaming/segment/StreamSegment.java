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
import org.apache.carbondata.core.locks.CarbonLockFactory;
import org.apache.carbondata.core.locks.ICarbonLock;
import org.apache.carbondata.core.locks.LockUsage;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.reader.CarbonIndexFileReader;
import org.apache.carbondata.core.statusmanager.FileFormat;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentDetailVO;
import org.apache.carbondata.core.statusmanager.SegmentManager;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.core.writer.CarbonIndexFileWriter;
import org.apache.carbondata.format.BlockIndex;
import org.apache.carbondata.format.BlockletIndex;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.streaming.CarbonStreamRecordWriter;

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
    List<SegmentDetailVO> allSegments =
        new SegmentManager().getAllSegments(table.getAbsoluteTableIdentifier())
            .getAllSegments();
    SegmentDetailVO streamSegment = null;
    for (SegmentDetailVO detail : allSegments) {
      if (FileFormat.ROW_V1.toString().equals(detail.getFileFormat())) {
        if (SegmentStatus.STREAMING.toString().equals(detail.getStatus())) {
          streamSegment = detail;
          break;
        }
      }
    }
    if (null == streamSegment) {
      SegmentDetailVO detailVO = new SegmentDetailVO();
      detailVO.setFileFormat(FileFormat.ROW_V1.toString());
      detailVO.setStatus(SegmentStatus.STREAMING.toString());

      new SegmentManager().createNewSegment(table.getAbsoluteTableIdentifier(), detailVO);
      return detailVO.getSegmentId();
    } else {
      return streamSegment.getSegmentId();
    }
  }

  /**
   * marker old stream segment to finished status and create new stream segment
   */
  public static String close(CarbonTable table, String segmentId)
      throws IOException {
    SegmentDetailVO detailVO =
        new SegmentDetailVO().setSegmentId(segmentId).setLoadEndTime(System.currentTimeMillis())
            .setStatus(SegmentStatus.STREAMING_FINISH.toString());
    new SegmentManager().commitLoadSegment(table.getAbsoluteTableIdentifier(), detailVO);

    SegmentDetailVO newDetail =
        new SegmentDetailVO().setFileFormat(FileFormat.ROW_V1.toString())
            .setStatus(SegmentStatus.STREAMING.toString());
    SegmentDetailVO newSegment =
        new SegmentManager().createNewSegment(table.getAbsoluteTableIdentifier(), newDetail);
    return newSegment.getSegmentId();
  }

  /**
   * change the status of the segment from "streaming" to "streaming finish"
   */
  public static void finishStreaming(CarbonTable carbonTable) throws IOException {
    List<SegmentDetailVO> allSegments =
        new SegmentManager().getAllSegments(carbonTable.getAbsoluteTableIdentifier())
            .getAllSegments();
    boolean updated = false;
    List<SegmentDetailVO> updateSegments = new ArrayList<>();
    for (SegmentDetailVO detail : allSegments) {
      if (SegmentStatus.STREAMING.toString().equals(detail.getStatus())) {
        updateSegments.add(new SegmentDetailVO().setSegmentId(detail.getSegmentId())
            .setLoadEndTime(System.currentTimeMillis())
            .setStatus(SegmentStatus.STREAMING_FINISH.toString()));
        updated = true;
      }
    }
    if (updated) {
      new SegmentManager()
          .updateSegments(carbonTable.getAbsoluteTableIdentifier(), updateSegments);
    }
  }

  /**
   * invoke CarbonStreamOutputFormat to append batch data to existing carbondata file
   */
  public static void appendBatchData(CarbonIterator<Object[]> inputIterators,
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

      while (inputIterators.hasNext()) {
        writer.write(null, inputIterators.next());
      }
      inputIterators.close();
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
   * update carbonindex file after a stream batch.
   */
  public static void updateIndexFile(String segmentDir) throws IOException {
    FileFactory.FileType fileType = FileFactory.getFileType(segmentDir);
    String filePath = CarbonTablePath.getCarbonStreamIndexFilePath(segmentDir);
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
        // TODO need to collect these information
        blockIndex.setNum_rows(-1);
        blockIndex.setOffset(-1);
        blockIndex.setBlock_index(new BlockletIndex());
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
