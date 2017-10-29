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
import java.util.HashMap;
import java.util.Map;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.locks.ICarbonLock;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.reader.CarbonIndexFileReader;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.path.CarbonStorePath;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.core.writer.CarbonIndexFileWriter;
import org.apache.carbondata.format.BlockIndex;
import org.apache.carbondata.format.BlockletIndex;
import org.apache.carbondata.streaming.file.CarbonStreamOutputFormat;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * streaming segment manager
 */
public class StreamSegmentManager {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(StreamSegmentManager.class.getName());

  private static final String FILE_FORMAT = "row-format";
  public static final long STREAM_SEGMENT_MAX_SIZE = 1024L * 1024 * 1024;

  /**
   * get stream segment or create new stream segment if not exists
   */
  public static String getOrCreateStreamSegment(CarbonTable table) throws IOException {
    CarbonTablePath tablePath =
        CarbonStorePath.getCarbonTablePath(table.getAbsoluteTableIdentifier());
    SegmentStatusManager segmentStatusManager =
        new SegmentStatusManager(table.getAbsoluteTableIdentifier());
    ICarbonLock carbonLock = segmentStatusManager.getTableStatusLock();
    try {
      if (carbonLock.lockWithRetries()) {
        LOGGER.info(
            "Acquired lock for table" + table.getDatabaseName() + "." + table.getFactTableName()
                + " for stream table get or create segment");

        LoadMetadataDetails[] details = SegmentStatusManager.readLoadMetadata(tablePath.getPath());
        LoadMetadataDetails streamSegment = null;
        for (LoadMetadataDetails detail : details) {
          if (FILE_FORMAT.equals(detail.getFileFormat())) {
            if (CarbonCommonConstants.STORE_LOADSTATUS_STREAMING.equals(detail.getLoadStatus())) {
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
          newDetail.setFileFormat(FILE_FORMAT);
          newDetail.setLoadStartTime(System.currentTimeMillis());
          newDetail.setLoadStatus(CarbonCommonConstants.STORE_LOADSTATUS_STREAMING);

          LoadMetadataDetails[] newDetails = new LoadMetadataDetails[details.length + 1];
          int i = 0;
          for (; i < details.length; i++) {
            newDetails[i] = details[i];
          }
          newDetails[i] = newDetail;
          SegmentStatusManager
              .writeLoadDetailsIntoFile(tablePath.getTableStatusFilePath(), newDetails);
          return newDetail.getLoadName();
        } else {
          return streamSegment.getLoadName();
        }
      } else {
        LOGGER.error(
            "Not able to acquire the lock for stream table get or create segment for table " + table
                .getDatabaseName() + "." + table.getFactTableName());
        throw new IOException("Failed to get stream segment");
      }
    } finally {
      if (carbonLock.unlock()) {
        LOGGER.info("Table unlocked successfully after stream table get or create segment" + table
            .getDatabaseName() + "." + table.getFactTableName());
      } else {
        LOGGER.error(
            "Unable to unlock table lock for stream table" + table.getDatabaseName() + "." + table
                .getFactTableName() + " during stream table get or create segment");
      }
    }
  }

  /**
   * marker old stream segment to finished status and create new stream segment
   */
  public static String finishAndCreateStreamSegment(CarbonTable table, String segmentId)
      throws IOException {
    CarbonTablePath tablePath =
        CarbonStorePath.getCarbonTablePath(table.getAbsoluteTableIdentifier());
    SegmentStatusManager segmentStatusManager =
        new SegmentStatusManager(table.getAbsoluteTableIdentifier());
    ICarbonLock carbonLock = segmentStatusManager.getTableStatusLock();
    try {
      if (carbonLock.lockWithRetries()) {
        LOGGER.info(
            "Acquired lock for table" + table.getDatabaseName() + "." + table.getFactTableName()
                + " for stream table finish segment");

        LoadMetadataDetails[] details = SegmentStatusManager.readLoadMetadata(tablePath.getPath());
        for (LoadMetadataDetails detail : details) {
          if (segmentId.equals(detail.getLoadName())) {
            detail.setLoadEndTime(System.currentTimeMillis());
            detail.setLoadStatus(CarbonCommonConstants.STORE_LOADSTATUS_STREAMING_FINISH);
            break;
          }
        }

        int newSegmentId = SegmentStatusManager.createNewSegmentId(details);
        LoadMetadataDetails newDetail = new LoadMetadataDetails();
        newDetail.setPartitionCount("0");
        newDetail.setLoadName("" + newSegmentId);
        newDetail.setFileFormat(FILE_FORMAT);
        newDetail.setLoadStartTime(System.currentTimeMillis());
        newDetail.setLoadStatus(CarbonCommonConstants.STORE_LOADSTATUS_STREAMING);

        LoadMetadataDetails[] newDetails = new LoadMetadataDetails[details.length + 1];
        int i = 0;
        for (; i < details.length; i++) {
          newDetails[i] = details[i];
        }
        newDetails[i] = newDetail;
        SegmentStatusManager
            .writeLoadDetailsIntoFile(tablePath.getTableStatusFilePath(), newDetails);
        return newDetail.getLoadName();
      } else {
        LOGGER.error(
            "Not able to acquire the lock for stream table status updation for table " + table
                .getDatabaseName() + "." + table.getFactTableName());
        throw new IOException("Failed to get stream segment");
      }
    } finally {
      if (carbonLock.unlock()) {
        LOGGER.info(
            "Table unlocked successfully after table status updation" + table.getDatabaseName()
                + "." + table.getFactTableName());
      } else {
        LOGGER.error("Unable to unlock Table lock for table" + table.getDatabaseName() + "." + table
            .getFactTableName() + " during table status updation");
      }
    }
  }

  /**
   * invoke CarbonStreamOutputFormat to ouput data to carbondata file
   */
  public static void output(CarbonIterator<Object[]> inputIterators, TaskAttemptContext job)
      throws Exception {
    RecordWriter writer = null;
    try {
      writer = new CarbonStreamOutputFormat().getRecordWriter(job);
      while (inputIterators.hasNext()) {
        writer.write(null, inputIterators.next());
      }
      inputIterators.close();
    } finally {
      if (writer != null) {
        writer.close(job);
      }
    }
  }

  /**
   * check the health of stream segment and try to recover segment from fault
   */
  public static void tryRecoverSegmentFromFault(String segmentDir) throws IOException {
    FileFactory.FileType fileType = FileFactory.getFileType(segmentDir);
    if (FileFactory.isFileExist(segmentDir, fileType)) {
      String indexName = CarbonTablePath.getCarbonStreamIndexFileName();
      String indexPath = segmentDir + File.separator + indexName;
      CarbonFile index = FileFactory.getCarbonFile(indexPath, fileType);
      CarbonFile[] files = listCarbonDataFiles(segmentDir, fileType);
      // TODO better to check backup index at first
      // index file exists
      if (index.exists()) {
        // data file exists
        if (files.length > 0) {
          CarbonIndexFileReader indexReader = new CarbonIndexFileReader();
          try {
            // map block index
            indexReader.openThriftReader(indexPath);
            Map<String, Long> tableSizeMap = new HashMap();
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
   * list all carbondata files of a segment
   */
  public static CarbonFile[] listCarbonDataFiles(String segmentDir, FileFactory.FileType fileType) {
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
   * update carbonindex file after after a stream batch.
   */
  public static void updateCarbonIndexFile(String segmentDir) throws IOException {
    FileFactory.FileType fileType = FileFactory.getFileType(segmentDir);
    String filePath = CarbonTablePath.getCarbonStreamIndexFilePath(segmentDir);
    String tempFilePath = filePath + CarbonCommonConstants.TEMPWRITEFILEEXTENSION;
    CarbonIndexFileWriter writer = new CarbonIndexFileWriter();
    try {
      writer.openThriftWriter(tempFilePath);
      CarbonFile[] files = listCarbonDataFiles(segmentDir, fileType);
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
}
