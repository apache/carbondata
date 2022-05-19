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

package org.apache.carbondata.core.util.path;

import java.io.File;
import java.util.Objects;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.locks.LockUsage;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;

import static org.apache.carbondata.core.constants.CarbonCommonConstants.DASH;
import static org.apache.carbondata.core.constants.CarbonCommonConstants.POINT;

import org.apache.hadoop.conf.Configuration;

/**
 * Helps to get Table content paths.
 */
public class CarbonTablePath {

  private static final String METADATA_DIR = "Metadata";
  private static final String DICTIONARY_EXT = ".dict";
  public static final String SCHEMA_FILE = "schema";
  private static final String FACT_DIR = "Fact";
  public static final String SEGMENT_PREFIX = "Segment_";
  private static final String PARTITION_PREFIX = "Part";
  public static final String DATA_PART_PREFIX = "part-";
  public static final String BATCH_PREFIX = "_batchno";
  public static final String TRASH_DIR = ".Trash";
  private static final String LOCK_DIR = "LockFiles";

  public static final String SEGMENTS_METADATA_DIR = "segments";
  public static final String TABLE_STATUS_FILE = "tablestatus";
  public static final String TABLE_STATUS_HISTORY_FILE = "tablestatus.history";
  public static final String CARBON_DATA_EXT = ".carbondata";
  public static final String INDEX_FILE_EXT = ".carbonindex";
  public static final String MERGE_INDEX_FILE_EXT = ".carbonindexmerge";
  public static final String SEGMENT_EXT = ".segment";

  private static final String STREAMING_DIR = ".streaming";
  private static final String STREAMING_LOG_DIR = "log";
  private static final String STREAMING_CHECKPOINT_DIR = "checkpoint";
  private static final String STAGE_DIR = "stage";
  private static final String STAGE_DATA_DIR = "stage_data";
  public static final String SUCCESS_FILE_SUFFIX = ".success";
  public static final String LOADING_FILE_SUFFIX = ".loading";
  private static final String SNAPSHOT_FILE_NAME = "snapshot";

  public static final String SYSTEM_FOLDER_DIR = "_system";

  /**
   * This class provides static utility only.
   */
  private CarbonTablePath() {
  }

  public static String getStageDir(String tablePath) {
    return getMetadataPath(tablePath) + CarbonCommonConstants.FILE_SEPARATOR + STAGE_DIR;
  }

  public static String getStageDataDir(String tablePath) {
    return tablePath + CarbonCommonConstants.FILE_SEPARATOR + STAGE_DATA_DIR;
  }

  public static String getStageSnapshotFile(String tablePath) {
    return CarbonTablePath.getStageDir(tablePath) + CarbonCommonConstants.FILE_SEPARATOR +
        SNAPSHOT_FILE_NAME;
  }

  /**
   * The method returns the folder path containing the carbon file.
   *
   * @param carbonFilePath
   */
  public static String getFolderContainingFile(String carbonFilePath) {
    int lastIndex = carbonFilePath.lastIndexOf('/');
    // below code for handling windows environment
    if (-1 == lastIndex) {
      lastIndex = carbonFilePath.lastIndexOf(File.separator);
    }
    return carbonFilePath.substring(0, lastIndex);
  }

  /**
   * @param columnId unique column identifier
   * @return name of dictionary file
   */
  public static String getDictionaryFileName(String columnId) {
    return columnId + DICTIONARY_EXT;
  }

  /**
   * whether carbonFile is dictionary file or not
   *
   * @param carbonFile
   * @return
   */
  public static Boolean isDictionaryFile(CarbonFile carbonFile) {
    return (!carbonFile.isDirectory()) && (carbonFile.getName().endsWith(DICTIONARY_EXT));
  }

  /**
   * check if it is carbon data file matching extension
   *
   * @param fileNameWithPath
   * @return boolean
   */
  public static boolean isCarbonDataFile(String fileNameWithPath) {
    int pos = fileNameWithPath.lastIndexOf('.');
    if (pos != -1) {
      return fileNameWithPath.substring(pos).startsWith(CARBON_DATA_EXT);
    }
    return false;
  }

  /**
   * check if it is carbon index file matching extension
   *
   * @param fileNameWithPath
   * @return boolean
   */
  public static boolean isCarbonIndexFile(String fileNameWithPath) {
    int pos = fileNameWithPath.lastIndexOf('.');
    if (pos != -1) {
      return fileNameWithPath.substring(pos).startsWith(INDEX_FILE_EXT);
    }
    return false;
  }

  /**
   * Return absolute path of dictionary file
   */
  public static String getDictionaryFilePath(String tablePath, String columnId) {
    return getMetadataPath(tablePath) + CarbonCommonConstants.FILE_SEPARATOR +
        getDictionaryFileName(columnId);
  }

  /**
   * Return metadata path
   */
  public static String getMetadataPath(String tablePath) {
    return tablePath + CarbonCommonConstants.FILE_SEPARATOR + METADATA_DIR;
  }

  /**
   * return the schema file path
   * @param tablePath path to table files
   * @return schema file path
   */
  public static String getSchemaFilePath(String tablePath) {
    return getActualSchemaFilePath(tablePath, null);
  }

  /**
   * return the schema file path
   * @param tablePath path to table files
   * @param hadoopConf hadoop configuration instance
   * @return schema file path
   */
  public static String getSchemaFilePath(String tablePath, Configuration hadoopConf) {
    return getActualSchemaFilePath(tablePath, hadoopConf);
  }

  private static String getActualSchemaFilePath(String tablePath, Configuration hadoopConf) {
    String metaPath = tablePath + CarbonCommonConstants.FILE_SEPARATOR + METADATA_DIR;
    CarbonFile carbonFile;
    if (hadoopConf != null) {
      carbonFile = FileFactory.getCarbonFile(metaPath, hadoopConf);
    } else {
      carbonFile = FileFactory.getCarbonFile(metaPath);
    }
    CarbonFile[] schemaFile = carbonFile.listFiles(new CarbonFileFilter() {
      @Override
      public boolean accept(CarbonFile file) {
        return file.getName().startsWith(SCHEMA_FILE);
      }
    });
    if (schemaFile != null && schemaFile.length > 0 &&
        FileFactory.getFileType(tablePath) != FileFactory.FileType.ALLUXIO) {
      return schemaFile[0].getAbsolutePath();
    } else {
      return metaPath + CarbonCommonConstants.FILE_SEPARATOR + SCHEMA_FILE;
    }
  }

  /**
   * Return absolute path of table status file
   */
  public static String getTableStatusFilePath(String tablePath) {
    return getMetadataPath(tablePath) + CarbonCommonConstants.FILE_SEPARATOR + TABLE_STATUS_FILE;
  }

  /**
   * Return absolute path of table status file
   */
  public static String getTableStatusFilePath(String tablePath, String tblStatusVersion) {
    String tableStatusPath = getTableStatusFilePath(tablePath);
    if (tblStatusVersion.isEmpty()) {
      return tableStatusPath;
    }
    return tableStatusPath + CarbonCommonConstants.UNDERSCORE + tblStatusVersion;
  }

  public static String getTableStatusFilePathWithUUID(String tablePath, String uuid) {
    if (!uuid.isEmpty()) {
      return getTableStatusFilePath(tablePath) + CarbonCommonConstants.UNDERSCORE + uuid;
    } else {
      return getTableStatusFilePath(tablePath);
    }
  }

  /**
   * Below method will be used to get the index file present in the segment folder
   * based on task id
   *
   * @param taskId      task id of the file
   * @param segmentId   segment number
   * @return full qualified carbon index path
   */
  private static String getCarbonIndexFilePath(final String tablePath, final String taskId,
      final String segmentId, final String bucketNumber) {
    String segmentDir = getSegmentPath(tablePath, segmentId);
    CarbonFile carbonFile =
        FileFactory.getCarbonFile(segmentDir);

    CarbonFile[] files = carbonFile.listFiles(new CarbonFileFilter() {
      @Override
      public boolean accept(CarbonFile file) {
        if (bucketNumber.equals("-1")) {
          return file.getName().startsWith(taskId) && file.getName().endsWith(INDEX_FILE_EXT);
        }
        return file.getName().startsWith(taskId + DASH + bucketNumber) && file.getName()
            .endsWith(INDEX_FILE_EXT);
      }
    });
    if (files.length > 0) {
      return files[0].getAbsolutePath();
    } else {
      throw new RuntimeException("Missing Carbon index file for Segment[" + segmentId + "], "
          + "taskId[" + taskId + "]");
    }
  }

  /**
   * Below method will be used to get the carbon index file path
   * @param taskId
   *        task id
   * @param segmentId
   *        segment id
   * @param bucketNumber
   *        bucket number
   * @param timeStamp
   *        timestamp
   * @return carbon index file path
   */
  public static String getCarbonIndexFilePath(String tablePath, String taskId, String segmentId,
      String bucketNumber, String timeStamp, ColumnarFormatVersion columnarFormatVersion) {
    switch (columnarFormatVersion) {
      case V3:
        String segmentDir = getSegmentPath(tablePath, segmentId);
        return segmentDir + CarbonCommonConstants.FILE_SEPARATOR + getCarbonIndexFileName(taskId,
            Integer.parseInt(bucketNumber), timeStamp, segmentId);
      default:
        throw new UnsupportedOperationException(
            "Unsupported file version: " + columnarFormatVersion);
    }
  }

  private static String getCarbonIndexFileName(String taskNo, int bucketNumber,
      String factUpdatedTimestamp, String segmentNo) {
    if (bucketNumber == -1) {
      return new StringBuilder()
          .append(taskNo).append(DASH)
          .append(segmentNo).append(DASH)
          .append(factUpdatedTimestamp)
          .append(INDEX_FILE_EXT)
          .toString();
    } else {
      return new StringBuilder()
          .append(taskNo).append(DASH)
          .append(bucketNumber).append(DASH)
          .append(segmentNo).append(DASH)
          .append(factUpdatedTimestamp)
          .append(INDEX_FILE_EXT)
          .toString();
    }
  }

  /**
   * Return the segment path from table path and segmentId
   */
  public static String getSegmentPath(String tablePath, String segmentId) {
    return getPartitionDir(tablePath) + CarbonCommonConstants.FILE_SEPARATOR
        + SEGMENT_PREFIX + segmentId;
  }

  /**
   * Gets data file name only, without parent path
   */
  public static String getCarbonDataFileName(Integer filePartNo, String taskNo, int bucketNumber,
      int batchNo, String factUpdateTimeStamp, String segmentNo, String compressor) {
    Objects.requireNonNull(filePartNo);
    Objects.requireNonNull(taskNo);
    Objects.requireNonNull(factUpdateTimeStamp);
    Objects.requireNonNull(compressor);

    // Start from CarbonData 2.0, the data file name patten is:
    // partNo-taskNo-batchNo-bucketNo-segmentNo-timestamp.compressor.carbondata
    // For example:
    // part-0-0_batchno0-0-0-1580982686749.zstd.carbondata
    //
    // If the compressor name is missing, the file is compressed by snappy, which is
    // the default compressor in CarbonData 1.x

    return new StringBuilder()
        .append(DATA_PART_PREFIX)
        .append(filePartNo).append(DASH)
        .append(taskNo).append(BATCH_PREFIX)
        .append(batchNo).append(DASH)
        .append(bucketNumber).append(DASH)
        .append(segmentNo).append(DASH)
        .append(factUpdateTimeStamp).append(POINT)
        .append(compressor).append(CARBON_DATA_EXT)
        .toString();
  }

  public static String getShardName(String taskNo, int bucketNumber, int batchNo,
      String factUpdateTimeStamp, String segmentNo) {
    return taskNo + BATCH_PREFIX + batchNo + DASH + bucketNumber + DASH + segmentNo + DASH
        + factUpdateTimeStamp;
  }

  /**
   * Below method will be used to get the carbon index filename
   *
   * @param taskNo               task number
   * @param factUpdatedTimeStamp time stamp
   * @return filename
   */
  public static String getCarbonIndexFileName(String taskNo, int bucketNumber, int batchNo,
      String factUpdatedTimeStamp, String segmentNo) {
    return getShardName(taskNo, bucketNumber, batchNo, factUpdatedTimeStamp, segmentNo)
        + INDEX_FILE_EXT;
  }

  public static String getCarbonStreamIndexFileName() {
    return getCarbonIndexFileName("0", 0, 0, "0", "0");
  }

  public static String getCarbonStreamIndexFilePath(String segmentDir) {
    return segmentDir + CarbonCommonConstants.FILE_SEPARATOR + getCarbonStreamIndexFileName();
  }

  // This partition is not used in any code logic, just keep backward compatibility
  public static final String DEPRECATED_PARTITION_ID = "0";

  public static String getPartitionDir(String tablePath) {
    return getFactDir(tablePath) + CarbonCommonConstants.FILE_SEPARATOR + PARTITION_PREFIX +
        CarbonTablePath.DEPRECATED_PARTITION_ID;
  }

  public static String getFactDir(String tablePath) {
    return tablePath + CarbonCommonConstants.FILE_SEPARATOR + FACT_DIR;
  }

  public static String getStreamingLogDir(String tablePath) {
    return tablePath + CarbonCommonConstants.FILE_SEPARATOR + STREAMING_DIR +
        CarbonCommonConstants.FILE_SEPARATOR + STREAMING_LOG_DIR;
  }

  public static String getStreamingCheckpointDir(String tablePath) {
    return tablePath + CarbonCommonConstants.FILE_SEPARATOR + STREAMING_DIR +
        CarbonCommonConstants.FILE_SEPARATOR + STREAMING_CHECKPOINT_DIR;
  }

  /**
   * Return store path for index based on the taskNo,if three tasks get launched during loading,
   * then three folders will be created based on the shard name and lucene index file will be
   * written into those folders
   *
   * @return store path based on index shard name
   */
  public static String getIndexStorePathOnShardName(String tablePath, String segmentId,
      String indexName, String shardName) {
    return new StringBuilder()
        .append(getIndexesStorePath(tablePath, segmentId, indexName))
        .append(CarbonCommonConstants.FILE_SEPARATOR)
        .append(shardName)
        .toString();
  }

  /**
   * Return store path for index based on the indexName,
   *
   * @return store path based on index name
   */
  public static String getIndexesStorePath(String tablePath, String segmentId,
      String indexName) {
    return new StringBuilder()
        .append(tablePath)
        .append(CarbonCommonConstants.FILE_SEPARATOR)
        .append(indexName)
        .append(CarbonCommonConstants.FILE_SEPARATOR)
        .append(segmentId)
        .toString();
  }

  /**
   * To manage data file name and composition
   */
  public static class DataFileUtil {

    /**
     * gets updated timestamp information from given carbon data file name
     */
    public static String getTimeStampFromFileName(String carbonDataFileName) {
      // Get the timestamp portion of the file.
      String fileName = getFileName(carbonDataFileName);
      int startIndex;
      if (carbonDataFileName.endsWith(CarbonTablePath.MERGE_INDEX_FILE_EXT)) {
        startIndex = fileName.lastIndexOf(CarbonCommonConstants.UNDERSCORE) + 1;
      } else {
        startIndex = fileName.lastIndexOf(CarbonCommonConstants.HYPHEN) + 1;
      }
      int endIndex = fileName.indexOf(".", startIndex);
      return fileName.substring(startIndex, endIndex);
    }

    /**
     * gets updated timestamp information from given carbon data file name
     * and compares with given timestamp
     *
     * @param fileName
     * @param timestamp
     * @return
     */
    public static Boolean compareCarbonFileTimeStamp(String fileName, Long timestamp) {
      int startIndex = fileName.lastIndexOf(DASH);
      int endIndex = fileName.indexOf(CarbonCommonConstants.POINT, startIndex);
      if (startIndex > 0 && endIndex > 0) {
        return fileName.substring(startIndex + 1, endIndex).equals(timestamp.toString());
      }
      return false;
    }

    /**
     * Return the timestamp present in the delete delta file.
     */
    public static String getTimeStampFromDeleteDeltaFile(String fileName) {
      return DataFileUtil.getTimeStampFromFileName(fileName);
    }

    /**
     * Return the timestamp present in the delete delta file.
     */
    public static String getBlockNameFromDeleteDeltaFile(String fileName) {
      return fileName.substring(0,
          fileName.lastIndexOf(CarbonCommonConstants.HYPHEN));
    }

    /**
     * Return the updated timestamp information from given carbon data file name
     */
    public static String getBucketNo(String carbonFilePath) {
      // Get the file name from path
      String fileName = getFileName(carbonFilePath);
      // + 1 for size of "-"
      int firstDashPos = fileName.indexOf(DASH);
      int secondDash = fileName.indexOf(DASH, firstDashPos + 1);
      int startIndex = fileName.indexOf(DASH, secondDash + 1) + 1;
      int endIndex = fileName.indexOf(DASH, startIndex);
      // to support backward compatibility
      if (endIndex == -1) {
        return "-1";
      }
      return fileName.substring(startIndex, endIndex);
    }

    /**
     * Return the file part number information from given carbon data file name
     */
    public static String getPartNo(String carbonDataFileName) {
      // Get the file name from path
      String fileName = getFileName(carbonDataFileName);
      // + 1 for size of "-"
      int startIndex = fileName.indexOf(DASH) + 1;
      int endIndex = fileName.indexOf(DASH, startIndex);
      return fileName.substring(startIndex, endIndex);
    }

    /**
     * Return the updated timestamp information from given carbon data file name
     */
    public static String getTaskNo(String carbonDataFileName) {
      // Get the file name from path
      String fileName = getFileName(carbonDataFileName);
      // + 1 for size of "-"
      int firstDashPos = fileName.indexOf(DASH);
      int startIndex = fileName.indexOf(DASH, firstDashPos + 1) + 1;
      int endIndex = fileName.indexOf(DASH, startIndex);
      return fileName.substring(startIndex, endIndex);
    }

    /**
     * Return task id in the carbon data file name
     */
    public static long getTaskId(String carbonDataFileName) {
      return Long.parseLong(getTaskNo(carbonDataFileName).split(BATCH_PREFIX)[0]);
    }

    /**
     * Return the updated timestamp information from given carbon data file name
     */
    public static String getSegmentNo(String carbonDataFileName) {
      // Get the file name from path
      String fileName = getFileName(carbonDataFileName);
      // + 1 for size of "-"
      int firstDashPos = fileName.indexOf(DASH);
      int startIndex1 = fileName.indexOf(DASH, firstDashPos + 1) + 1;
      int endIndex1 = fileName.indexOf(DASH, startIndex1);
      int startIndex = fileName.indexOf(DASH, endIndex1 + 1);
      if (startIndex > -1) {
        startIndex += 1;
        int endIndex = fileName.indexOf(DASH, startIndex);
        if (endIndex == -1) {
          return null;
        }
        return fileName.substring(startIndex, endIndex);
      } else {
        return null;
      }
    }

    /**
     * Return the taskId part from taskNo(include taskId + batchNo)
     */
    public static String getTaskIdFromTaskNo(String taskNo) {
      return taskNo.split(BATCH_PREFIX)[0];
    }

    /**
     * Return the batch number from taskNo string
     */
    public static int getBatchNoFromTaskNo(String taskNo) {
      return Integer.parseInt(taskNo.split(BATCH_PREFIX)[1]);
    }

    /**
     * Return the file name from file path
     */
    public static String getFileName(String dataFilePath) {
      int endIndex = dataFilePath.lastIndexOf(CarbonCommonConstants.FILE_SEPARATOR);
      if (endIndex > -1) {
        return dataFilePath.substring(endIndex + 1);
      } else {
        return dataFilePath;
      }
    }

    /**
     * This method returns the segment number from the segment file name
     */
    public static String getSegmentNoFromSegmentFile(String segmentFileName) {
      return segmentFileName.split(CarbonCommonConstants.UNDERSCORE)[0];
    }

    /**
     * gets segment id from given absolute data file path
     */
    public static String getSegmentIdFromPath(String dataFileAbsolutePath) {
      // find segment id from last of data file path
      String tempDataFileAbsolutePath = dataFileAbsolutePath.replace(
          CarbonCommonConstants.WINDOWS_FILE_SEPARATOR, CarbonCommonConstants.FILE_SEPARATOR);
      int endIndex = tempDataFileAbsolutePath.lastIndexOf(CarbonCommonConstants.FILE_SEPARATOR);
      // + 1 for size of "/"
      int startIndex = tempDataFileAbsolutePath.lastIndexOf(
          CarbonCommonConstants.FILE_SEPARATOR, endIndex - 1) + 1;
      String segmentDirStr = dataFileAbsolutePath.substring(startIndex, endIndex);
      //identify id in segment_<id>
      String[] segmentDirSplits = segmentDirStr.split(CarbonCommonConstants.UNDERSCORE);
      try {
        if (segmentDirSplits.length == 2) {
          return segmentDirSplits[1];
        }
      } catch (Exception e) {
        return CarbonCommonConstants.INVALID_SEGMENT_ID;
      }
      return CarbonCommonConstants.INVALID_SEGMENT_ID;
    }
  }

  /**
   * Return the carbondata file name without extension name
   */
  public static String getCarbonDataFileName(String carbonDataFilePath) {
    return carbonDataFilePath.substring(
        carbonDataFilePath.lastIndexOf(CarbonCommonConstants.FILE_SEPARATOR) + 1,
        carbonDataFilePath.lastIndexOf(CARBON_DATA_EXT));
  }

  /**
   * Return prefix of carbon data
   */
  public static String getCarbonDataPrefix() {
    return DATA_PART_PREFIX;
  }

  /**
   *
   * @return carbon data extension
   */
  public static String getCarbonDataExtension() {
    return CARBON_DATA_EXT;
  }

  /**
   *
   * @return carbon index extension
   */
  public static String getCarbonIndexExtension() {
    return INDEX_FILE_EXT;
  }

  /**
   *
   * @return carbon index merge file extension
   */
  public static String getCarbonMergeIndexExtension() {
    return MERGE_INDEX_FILE_EXT;
  }

  /**
   * This method will remove strings in path and return short block id
   *
   * @param blockId
   * @return shortBlockId
   */
  public static String getShortBlockId(String blockId) {
    String blockIdWithCompressorName =
        blockId.replace(PARTITION_PREFIX + "0" + CarbonCommonConstants.FILE_SEPARATOR, "")
            .replace(SEGMENT_PREFIX, "").replace(BATCH_PREFIX, CarbonCommonConstants.UNDERSCORE)
            .replace(DATA_PART_PREFIX, "").replace(CARBON_DATA_EXT, "");
    // to remove compressor name
    if (!blockId.equalsIgnoreCase(blockIdWithCompressorName)) {
      int index = blockIdWithCompressorName.lastIndexOf(POINT);
      int fileSeperatorIndex = blockIdWithCompressorName.lastIndexOf(File.separator);
      if (index != -1) {
        String modifiedBlockId;
        if (index > fileSeperatorIndex) {
          // Default case when path ends with compressor name.
          // Example: 0/0-0_0-0-0-1600789595862.snappy
          modifiedBlockId =
              blockIdWithCompressorName.replace(blockIdWithCompressorName.substring(index), "");
        } else {
          // in case of CACHE_LEVEL = BLOCKLET, blockId path contains both block id and blocklet id
          // so check for next file seperator and remove compressor name.
          // Example: 0/0-0_0-0-0-1600789595862.snappy/0
          modifiedBlockId = blockIdWithCompressorName
              .replace(blockIdWithCompressorName.substring(index, fileSeperatorIndex), "");
        }
        return modifiedBlockId;
      } else {
        return blockIdWithCompressorName;
      }
    } else {
      return blockIdWithCompressorName;
    }
  }

  /**
   * adds data part prefix to given value
   * @return partition prefix
   */
  public static String addDataPartPrefix(String value) {
    return DATA_PART_PREFIX + value;
  }

  /**
   * adds part prefix to given value
   * @return partition prefix
   */
  public static String addPartPrefix(String value) {
    return PARTITION_PREFIX + value;
  }

  /**
   * adds part prefix to given value
   * @return partition prefix
   */
  public static String addSegmentPrefix(String value) {
    return SEGMENT_PREFIX + value;
  }

  /**
   * Unique task name
   *
   * A shard name is composed by `TaskNo-BucketNo-SegmentNo-Timestamp`
   * As for data before version 1.4, shard name was `TaskNo-BucketNo-Timestamp`
   *
   * @param actualBlockName
   * @return
   */
  public static String getShardName(String actualBlockName) {
    String segmentNoStr = DataFileUtil.getSegmentNo(actualBlockName);
    StringBuilder shardName = new StringBuilder();
    if (null != segmentNoStr) {
      shardName.append(DataFileUtil.getTaskNo(actualBlockName)).append(DASH);
      shardName.append(DataFileUtil.getBucketNo(actualBlockName)).append(DASH);
      shardName.append(segmentNoStr).append(DASH);
      shardName.append(DataFileUtil.getTimeStampFromFileName(actualBlockName));
      return shardName.toString();
    } else {
      // data before version 1.4 does not have SegmentNo in carbondata filename
      shardName.append(DataFileUtil.getTaskNo(actualBlockName)).append(DASH);
      shardName.append(DataFileUtil.getBucketNo(actualBlockName)).append(DASH);
      shardName.append(DataFileUtil.getTimeStampFromFileName(actualBlockName));
      return shardName.toString();
    }
  }

  /**
   * Get the segment file locations of table
   */
  public static String getSegmentFilesLocation(String tablePath) {
    return getMetadataPath(tablePath) + CarbonCommonConstants.FILE_SEPARATOR +
        SEGMENTS_METADATA_DIR;
  }

  /**
   * Get the segment file path of table
   */
  public static String getSegmentFilePath(String tablePath, String segmentFileName) {
    return getSegmentFilesLocation(tablePath) + CarbonCommonConstants.FILE_SEPARATOR +
        segmentFileName;
  }

  /**
   * Get the lock files directory
   */
  public static String getLockFilesDirPath(String tablePath) {
    return tablePath + CarbonCommonConstants.FILE_SEPARATOR + LOCK_DIR;
  }

  /**
   * Get the lock file
   */
  public static String getLockFilePath(String tablePath, String lockType) {
    return getLockFilesDirPath(tablePath) + CarbonCommonConstants.FILE_SEPARATOR + lockType;
  }

  /**
   * return true if this lock file is a segment lock file otherwise false.
   */
  public static boolean isSegmentLockFilePath(String lockFileName) {
    return lockFileName.startsWith(SEGMENT_PREFIX) && lockFileName.endsWith(LockUsage.LOCK);
  }

  /**
   * Return table status history file path based on `tablePath`
   */
  public static String getTableStatusHistoryFilePath(String tablePath) {
    return getMetadataPath(tablePath) + CarbonCommonConstants.FILE_SEPARATOR
        + TABLE_STATUS_HISTORY_FILE;
  }

  public static String generateBadRecordsPath(String badLogStoreLocation, String segmentId,
      String taskNo, boolean isTransactionalTable) {
    if (!isTransactionalTable) {
      return badLogStoreLocation + CarbonCommonConstants.FILE_SEPARATOR + "SdkWriterBadRecords"
          + CarbonCommonConstants.FILE_SEPARATOR + taskNo;
    } else {
      return badLogStoreLocation + CarbonCommonConstants.FILE_SEPARATOR + segmentId +
          CarbonCommonConstants.FILE_SEPARATOR + taskNo;
    }
  }

  /**
   * Return the parent path of the input file.
   * For example, if input file path is /user/warehouse/t1/file.carbondata
   * then return will be /user/warehouse/t1
   */
  public static String getParentPath(String dataFilePath) {
    int endIndex = dataFilePath.lastIndexOf(CarbonCommonConstants.FILE_SEPARATOR);
    if (endIndex > -1) {
      return dataFilePath.substring(0, endIndex);
    } else {
      return dataFilePath;
    }
  }

  public static String getTrashFolderPath(String tablePath) {
    return tablePath + CarbonCommonConstants.FILE_SEPARATOR + TRASH_DIR;
  }
}
