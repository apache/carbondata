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

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.locks.LockUsage;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;

import org.apache.hadoop.conf.Configuration;

/**
 * Helps to get Table content paths.
 */
public class CarbonTablePath {

  private static final String METADATA_DIR = "Metadata";
  private static final String DICTIONARY_EXT = ".dict";
  private static final String DICTIONARY_META_EXT = ".dictmeta";
  private static final String SORT_INDEX_EXT = ".sortindex";
  public static final String SCHEMA_FILE = "schema";
  private static final String FACT_DIR = "Fact";
  private static final String SEGMENT_PREFIX = "Segment_";
  private static final String PARTITION_PREFIX = "Part";
  private static final String DATA_PART_PREFIX = "part-";
  private static final String BATCH_PREFIX = "_batchno";
  private static final String LOCK_DIR = "LockFiles";

  public static final String TABLE_STATUS_FILE = "tablestatus";
  public static final String TABLE_STATUS_HISTORY_FILE = "tablestatus.history";
  public static final String CARBON_DATA_EXT = ".carbondata";
  public static final String INDEX_FILE_EXT = ".carbonindex";
  public static final String MERGE_INDEX_FILE_EXT = ".carbonindexmerge";
  public static final String SEGMENT_EXT = ".segment";

  private static final String STREAMING_DIR = ".streaming";
  private static final String STREAMING_LOG_DIR = "log";
  private static final String STREAMING_CHECKPOINT_DIR = "checkpoint";

  /**
   * This class provides static utility only.
   */
  private CarbonTablePath() {
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
    return getMetadataPath(tablePath) + File.separator + getDictionaryFileName(columnId);
  }

  /**
   * Return absolute path of dictionary file
   */
  public static String getExternalDictionaryFilePath(String dictionaryPath, String columnId) {
    return dictionaryPath + File.separator + getDictionaryFileName(columnId);
  }

  /**
   * Return metadata path
   */
  public static String getMetadataPath(String tablePath) {
    return tablePath + File.separator + METADATA_DIR;
  }

  /**
   * Return absolute path of dictionary meta file
   */
  public static String getExternalDictionaryMetaFilePath(String dictionaryPath, String columnId) {
    return dictionaryPath + File.separator + columnId + DICTIONARY_META_EXT;
  }

  /**
   * Return absolute path of dictionary meta file
   */
  public static String getDictionaryMetaFilePath(String tablePath, String columnId) {
    return getMetadataPath(tablePath) + File.separator + columnId + DICTIONARY_META_EXT;
  }

  /**
   * Return sortindex file path based on specified dictionary path
   */
  public static String getExternalSortIndexFilePath(String dictionaryPath, String columnId) {
    return dictionaryPath + File.separator + columnId + SORT_INDEX_EXT;
  }

  /**
   * Return sortindex file path for columnId and offset based on specified dictionary path
   */
  public static String getExternalSortIndexFilePath(String dictionaryPath, String columnId,
      long dictOffset) {
    return dictionaryPath + File.separator + columnId + "_" + dictOffset + SORT_INDEX_EXT;
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
      @Override public boolean accept(CarbonFile file) {
        return file.getName().startsWith(SCHEMA_FILE);
      }
    });
    if (schemaFile != null && schemaFile.length > 0) {
      return schemaFile[0].getAbsolutePath();
    } else {
      return metaPath + CarbonCommonConstants.FILE_SEPARATOR + SCHEMA_FILE;
    }
  }

  /**
   * Return absolute path of table status file
   */
  public static String getTableStatusFilePath(String tablePath) {
    return getMetadataPath(tablePath) + File.separator + TABLE_STATUS_FILE;
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
        FileFactory.getCarbonFile(segmentDir, FileFactory.getFileType(segmentDir));

    CarbonFile[] files = carbonFile.listFiles(new CarbonFileFilter() {
      @Override public boolean accept(CarbonFile file) {
        if (bucketNumber.equals("-1")) {
          return file.getName().startsWith(taskId) && file.getName().endsWith(INDEX_FILE_EXT);
        }
        return file.getName().startsWith(taskId + "-" + bucketNumber) && file.getName()
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
      case V1:
      case V2:
        return getCarbonIndexFilePath(tablePath, taskId, segmentId, bucketNumber);
      default:
        String segmentDir = getSegmentPath(tablePath, segmentId);
        return segmentDir + File.separator + getCarbonIndexFileName(taskId,
            Integer.parseInt(bucketNumber), timeStamp, segmentId);
    }
  }

  private static String getCarbonIndexFileName(String taskNo, int bucketNumber,
      String factUpdatedtimeStamp, String segmentNo) {
    if (bucketNumber == -1) {
      return taskNo + "-" + segmentNo + "-" + factUpdatedtimeStamp + INDEX_FILE_EXT;
    }
    return taskNo + "-" + bucketNumber + "-" + segmentNo + "-" + factUpdatedtimeStamp
        + INDEX_FILE_EXT;
  }

  /**
   * Return the segment path from table path and segmentId
   */
  public static String getSegmentPath(String tablePath, String segmentId) {
    return getPartitionDir(tablePath) + File.separator + SEGMENT_PREFIX + segmentId;
  }

  /**
   * Gets data file name only with out path
   *
   * @param filePartNo          data file part number
   * @param taskNo              task identifier
   * @param factUpdateTimeStamp unique identifier to identify an update
   * @return gets data file name only with out path
   */
  public static String getCarbonDataFileName(Integer filePartNo, Long taskNo, int bucketNumber,
      int batchNo, String factUpdateTimeStamp, String segmentNo) {
    return DATA_PART_PREFIX + filePartNo + "-" + taskNo + BATCH_PREFIX + batchNo + "-"
        + bucketNumber + "-" + segmentNo + "-" + factUpdateTimeStamp + CARBON_DATA_EXT;
  }

  public static String getShardName(Long taskNo, int bucketNumber, int batchNo,
      String factUpdateTimeStamp, String segmentNo) {
    return taskNo + BATCH_PREFIX + batchNo + "-" + bucketNumber + "-" + segmentNo + "-"
        + factUpdateTimeStamp;
  }

  /**
   * Below method will be used to get the carbon index filename
   *
   * @param taskNo               task number
   * @param factUpdatedTimeStamp time stamp
   * @return filename
   */
  public static String getCarbonIndexFileName(long taskNo, int bucketNumber, int batchNo,
      String factUpdatedTimeStamp, String segmentNo) {
    return getShardName(taskNo, bucketNumber, batchNo, factUpdatedTimeStamp, segmentNo)
        + INDEX_FILE_EXT;
  }

  public static String getCarbonStreamIndexFileName() {
    return getCarbonIndexFileName(0, 0, 0, "0", "0");
  }

  public static String getCarbonStreamIndexFilePath(String segmentDir) {
    return segmentDir + File.separator + getCarbonStreamIndexFileName();
  }

  // This partition is not used in any code logic, just keep backward compatibility
  public static final String DEPRECATED_PARTITION_ID = "0";

  public static String getPartitionDir(String tablePath) {
    return getFactDir(tablePath) + File.separator + PARTITION_PREFIX +
        CarbonTablePath.DEPRECATED_PARTITION_ID;
  }

  public static String getFactDir(String tablePath) {
    return tablePath + File.separator + FACT_DIR;
  }

  public static String getStreamingLogDir(String tablePath) {
    return tablePath + File.separator + STREAMING_DIR + File.separator + STREAMING_LOG_DIR;
  }

  public static String getStreamingCheckpointDir(String tablePath) {
    return tablePath + File.separator + STREAMING_DIR + File.separator + STREAMING_CHECKPOINT_DIR;
  }

  /**
   * Return store path for datamap based on the taskNo,if three tasks get launched during loading,
   * then three folders will be created based on the shard name and lucene index file will be
   * written into those folders
   *
   * @return store path based on index shard name
   */
  public static String getDataMapStorePathOnShardName(String tablePath, String segmentId,
      String dataMapName, String shardName) {
    return new StringBuilder()
        .append(getDataMapStorePath(tablePath, segmentId, dataMapName))
        .append(File.separator)
        .append(shardName)
        .toString();
  }

  /**
   * Return store path for datamap based on the dataMapName,
   *
   * @return store path based on datamapname
   */
  public static String getDataMapStorePath(String tablePath, String segmentId,
      String dataMapName) {
    return new StringBuilder()
        .append(tablePath)
        .append(File.separator)
        .append(dataMapName)
        .append(File.separator)
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
      int startIndex = fileName.lastIndexOf(CarbonCommonConstants.HYPHEN) + 1;
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
      int lastIndexOfHyphen = fileName.lastIndexOf("-");
      int lastIndexOfDot = fileName.lastIndexOf(".");
      if (lastIndexOfHyphen > 0 && lastIndexOfDot > 0) {
        return fileName.substring(fileName.lastIndexOf("-") + 1, fileName.lastIndexOf("."))
            .equals(timestamp.toString());
      }
      return false;
    }

    /**
     * Return the timestamp present in the delete delta file.
     */
    public static String getTimeStampFromDeleteDeltaFile(String fileName) {
      return fileName.substring(fileName.lastIndexOf(CarbonCommonConstants.HYPHEN) + 1,
          fileName.lastIndexOf("."));
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
      int firstDashPos = fileName.indexOf("-");
      int secondDash = fileName.indexOf("-", firstDashPos + 1);
      int startIndex = fileName.indexOf("-", secondDash + 1) + 1;
      int endIndex = fileName.indexOf("-", startIndex);
      // to support backward compatibility
      if (startIndex == -1 || endIndex == -1) {
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
      int startIndex = fileName.indexOf("-") + 1;
      int endIndex = fileName.indexOf("-", startIndex);
      return fileName.substring(startIndex, endIndex);
    }

    /**
     * Return the updated timestamp information from given carbon data file name
     */
    public static String getTaskNo(String carbonDataFileName) {
      // Get the file name from path
      String fileName = getFileName(carbonDataFileName);
      // + 1 for size of "-"
      int firstDashPos = fileName.indexOf("-");
      int startIndex = fileName.indexOf("-", firstDashPos + 1) + 1;
      int endIndex = fileName.indexOf("-", startIndex);
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
      int firstDashPos = fileName.indexOf("-");
      int startIndex1 = fileName.indexOf("-", firstDashPos + 1) + 1;
      int endIndex1 = fileName.indexOf("-", startIndex1);
      int startIndex = fileName.indexOf("-", endIndex1 + 1);
      if (startIndex > -1) {
        startIndex += 1;
        int endIndex = fileName.indexOf("-", startIndex);
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
    public static long getTaskIdFromTaskNo(String taskNo) {
      return Long.parseLong(taskNo.split(BATCH_PREFIX)[0]);
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
        return dataFilePath.substring(endIndex + 1, dataFilePath.length());
      } else {
        return dataFilePath;
      }
    }

    /**
     * gets segement id from given absolute data file path
     */
    public static String getSegmentIdFromPath(String dataFileAbsolutePath) {
      // find segment id from last of data file path
      String tempdataFileAbsolutePath = dataFileAbsolutePath.replace(
          CarbonCommonConstants.WINDOWS_FILE_SEPARATOR, CarbonCommonConstants.FILE_SEPARATOR);
      int endIndex = tempdataFileAbsolutePath.lastIndexOf(CarbonCommonConstants.FILE_SEPARATOR);
      // + 1 for size of "/"
      int startIndex = tempdataFileAbsolutePath.lastIndexOf(
          CarbonCommonConstants.FILE_SEPARATOR, endIndex - 1) + 1;
      String segmentDirStr = dataFileAbsolutePath.substring(startIndex, endIndex);
      //identify id in segment_<id>
      String[] segmentDirSplits = segmentDirStr.split("_");
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
   * Below method will be used to get sort index file present in mentioned folder
   *
   * @param sortIndexDir directory where sort index file resides
   * @param columnUniqueId   columnunique id
   * @return sort index carbon files
   */
  public static CarbonFile[] getSortIndexFiles(CarbonFile sortIndexDir,
      final String columnUniqueId) {
    if (null != sortIndexDir) {
      return sortIndexDir.listFiles(new CarbonFileFilter() {
        @Override public boolean accept(CarbonFile file) {
          return file.getName().startsWith(columnUniqueId) && file.getName()
              .endsWith(SORT_INDEX_EXT);
        }
      });
    }
    return null;
  }

  /**
   * Return the carbondata file name
   */
  public static String getCarbonDataFileName(String carbonDataFilePath) {
    return carbonDataFilePath.substring(
        carbonDataFilePath.lastIndexOf(CarbonCommonConstants.FILE_SEPARATOR) + 1,
        carbonDataFilePath.indexOf(CARBON_DATA_EXT));
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
    return blockId.replace(PARTITION_PREFIX, "")
            .replace(SEGMENT_PREFIX, "")
            .replace(DATA_PART_PREFIX, "")
            .replace(CARBON_DATA_EXT, "");
  }

  /**
   * This method will remove strings in path and return short block id
   *
   * @param blockId
   * @return shortBlockId
   */
  public static String getShortBlockIdForPartitionTable(String blockId) {
    return blockId.replace(DATA_PART_PREFIX, "")
        .replace(CARBON_DATA_EXT, "");
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

  public static String getCarbonIndexFileName(String actualBlockName) {
    return getShardName(actualBlockName) + INDEX_FILE_EXT;
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
      shardName.append(DataFileUtil.getTaskNo(actualBlockName)).append("-");
      shardName.append(DataFileUtil.getBucketNo(actualBlockName)).append("-");
      shardName.append(segmentNoStr).append("-");
      shardName.append(DataFileUtil.getTimeStampFromFileName(actualBlockName));
      return shardName.toString();
    } else {
      // data before version 1.4 does not have SegmentNo in carbondata filename
      shardName.append(DataFileUtil.getTaskNo(actualBlockName)).append("-");
      shardName.append(DataFileUtil.getBucketNo(actualBlockName)).append("-");
      shardName.append(DataFileUtil.getTimeStampFromFileName(actualBlockName));
      return shardName.toString();
    }
  }

  /**
   * Get the segment file locations of table
   */
  public static String getSegmentFilesLocation(String tablePath) {
    return getMetadataPath(tablePath) + CarbonCommonConstants.FILE_SEPARATOR + "segments";
  }

  /**
   * Get the segment file path of table
   */
  public static String getSegmentFilePath(String tablePath, String segmentFileName) {
    return getMetadataPath(tablePath) + CarbonCommonConstants.FILE_SEPARATOR + "segments"
        + CarbonCommonConstants.FILE_SEPARATOR + segmentFileName;
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
      return badLogStoreLocation + File.separator + "SdkWriterBadRecords"
          + CarbonCommonConstants.FILE_SEPARATOR + taskNo;
    } else {
      return badLogStoreLocation + File.separator + segmentId + CarbonCommonConstants.FILE_SEPARATOR
          + taskNo;
    }
  }
}
