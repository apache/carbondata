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
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;

import org.apache.hadoop.fs.Path;


/**
 * Helps to get Table content paths.
 */
public class CarbonTablePath extends Path {

  private static final String METADATA_DIR = "Metadata";
  private static final String DICTIONARY_EXT = ".dict";
  private static final String DICTIONARY_META_EXT = ".dictmeta";
  private static final String SORT_INDEX_EXT = ".sortindex";
  private static final String SCHEMA_FILE = "schema";
  private static final String TABLE_STATUS_FILE = "tablestatus";
  private static final String FACT_DIR = "Fact";
  private static final String SEGMENT_PREFIX = "Segment_";
  private static final String PARTITION_PREFIX = "Part";
  private static final String DATA_PART_PREFIX = "part-";
  private static final String BATCH_PREFIX = "_batchno";

  public static final String CARBON_DATA_EXT = ".carbondata";
  public static final String INDEX_FILE_EXT = ".carbonindex";
  public static final String MERGE_INDEX_FILE_EXT = ".carbonindexmerge";
  public static final String PARTITION_MAP_EXT = ".partitionmap";

  private static final String STREAMING_DIR = ".streaming";
  private static final String STREAMING_LOG_DIR = "log";
  private static final String STREAMING_CHECKPOINT_DIR = "checkpoint";

  private String tablePath;
  private CarbonTableIdentifier carbonTableIdentifier;

  /**
   * structure CarbonTablePath object to manage table paths
   *
   * @param carbonTableIdentifier identifier of carbon table that the segment belong to
   * @param tablePathString the store path of the segment
   */
  public CarbonTablePath(CarbonTableIdentifier carbonTableIdentifier, String tablePathString) {
    super(tablePathString);
    this.carbonTableIdentifier = carbonTableIdentifier;
    this.tablePath = tablePathString;
  }

  /**
   * The method returns the folder path containing the carbon file.
   *
   * @param carbonFilePath
   */
  public static String getFolderContainingFile(String carbonFilePath) {
    return carbonFilePath.substring(0, carbonFilePath.lastIndexOf(File.separator));
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
   * gets table path
   */
  public String getPath() {
    return tablePath;
  }

  /**
   * @param columnId unique column identifier
   * @return absolute path of dictionary file
   */
  public String getDictionaryFilePath(String columnId) {
    return getMetaDataDir() + File.separator + getDictionaryFileName(columnId);
  }

  /**
   * @param dictionaryPath
   * @param columnId unique column identifier
   * @return absolute path of dictionary file
   */
  public String getDictionaryFilePath(String dictionaryPath, String columnId) {
    return dictionaryPath + File.separator + getDictionaryFileName(columnId);
  }

  /**
   * This method will return the metadata directory location for a table
   *
   * @return
   */
  public String getMetadataDirectoryPath() {
    return getMetaDataDir();
  }

  /**
   * Return metadata path based on `tablePath`
   */
  public static String getMetadataPath(String tablePath) {
    return tablePath + File.separator + METADATA_DIR;
  }

  /**
   * @param columnId unique column identifier
   * @return absolute path of dictionary meta file
   */
  public String getDictionaryMetaFilePath(String columnId) {
    return getMetaDataDir() + File.separator + columnId + DICTIONARY_META_EXT;
  }

  /**
   * @param dictionaryPath
   * @param columnId unique column identifier
   * @return absolute path of dictionary file
   */
  public String getDictionaryMetaFilePath(String dictionaryPath, String columnId) {
    return dictionaryPath + File.separator + columnId + DICTIONARY_META_EXT;
  }

  /**
   * @param columnId unique column identifier
   * @return absolute path of sort index file
   */
  public String getSortIndexFilePath(String columnId) {
    return getMetaDataDir() + File.separator + columnId + SORT_INDEX_EXT;
  }

  /**
   * @param dictionaryPath
   * @param columnId unique column identifier
   * @return absolute path of dictionary file
   */
  public String getSortIndexFilePath(String dictionaryPath, String columnId) {
    return dictionaryPath + File.separator + columnId + SORT_INDEX_EXT;
  }

  /**
   *
   * @param columnId
   * @param dictOffset
   * @return absolute path of sortindex with appeneded dictionary offset
   */
  public String getSortIndexFilePath(String columnId, long dictOffset) {
    return getMetaDataDir() + File.separator + columnId + "_" + dictOffset + SORT_INDEX_EXT;
  }

  /**
   * @param dictionaryPath
   * @param columnId unique column identifier
   * @param dictOffset
   * @return absolute path of dictionary file
   */
  public String getSortIndexFilePath(String dictionaryPath, String columnId, long dictOffset) {
    return dictionaryPath + File.separator + columnId + "_" + dictOffset + SORT_INDEX_EXT;
  }

  /**
   * @return absolute path of schema file
   */
  public String getSchemaFilePath() {
    return getMetaDataDir() + File.separator + SCHEMA_FILE;
  }

  /**
   * return the schema file path
   * @param tablePath path to table files
   * @return schema file path
   */
  public static String getSchemaFilePath(String tablePath) {
    return tablePath + File.separator + METADATA_DIR + File.separator + SCHEMA_FILE;
  }

  /**
   * @return absolute path of table status file
   */
  public String getTableStatusFilePath() {
    return getMetaDataDir() + File.separator + TABLE_STATUS_FILE;
  }

  /**
   * Gets absolute path of data file
   *
   * @param partitionId         unique partition identifier
   * @param segmentId           unique partition identifier
   * @param filePartNo          data file part number
   * @param factUpdateTimeStamp unique identifier to identify an update
   * @return absolute path of data file stored in carbon data format
   */
  public String getCarbonDataFilePath(String partitionId, String segmentId, Integer filePartNo,
      Long taskNo, int batchNo, int bucketNumber, String factUpdateTimeStamp) {
    return getSegmentDir(partitionId, segmentId) + File.separator + getCarbonDataFileName(
        filePartNo, taskNo, bucketNumber, batchNo, factUpdateTimeStamp);
  }

  /**
   * Below method will be used to get the index file present in the segment folder
   * based on task id
   *
   * @param taskId      task id of the file
   * @param partitionId partition number
   * @param segmentId   segment number
   * @return full qualified carbon index path
   */
  public String getCarbonIndexFilePath(final String taskId, final String partitionId,
      final String segmentId, final String bucketNumber) {
    String segmentDir = getSegmentDir(partitionId, segmentId);
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
      throw new RuntimeException("Missing Carbon index file for partition["
          + partitionId + "] Segment[" + segmentId + "], taskId[" + taskId
          + "]");
    }
  }

  /**
   * Below method will be used to get the carbon index file path
   * @param taskId
   *        task id
   * @param partitionId
   *        partition id
   * @param segmentId
   *        segment id
   * @param bucketNumber
   *        bucket number
   * @param timeStamp
   *        timestamp
   * @return carbon index file path
   */
  public String getCarbonIndexFilePath(String taskId, String partitionId, String segmentId,
      String bucketNumber, String timeStamp, ColumnarFormatVersion columnarFormatVersion) {
    switch (columnarFormatVersion) {
      case V1:
      case V2:
        return getCarbonIndexFilePath(taskId, partitionId, segmentId, bucketNumber);
      default:
        String segmentDir = getSegmentDir(partitionId, segmentId);
        return segmentDir + File.separator + getCarbonIndexFileName(taskId,
            Integer.parseInt(bucketNumber), timeStamp);
    }
  }

  public String getCarbonIndexFilePath(String taskId, String partitionId, String segmentId,
      int batchNo, String bucketNumber, String timeStamp,
      ColumnarFormatVersion columnarFormatVersion) {
    switch (columnarFormatVersion) {
      case V1:
      case V2:
        return getCarbonIndexFilePath(taskId, partitionId, segmentId, bucketNumber);
      default:
        String segmentDir = getSegmentDir(partitionId, segmentId);
        return segmentDir + File.separator + getCarbonIndexFileName(Long.parseLong(taskId),
            Integer.parseInt(bucketNumber), batchNo, timeStamp);
    }
  }

  private static String getCarbonIndexFileName(String taskNo, int bucketNumber,
      String factUpdatedtimeStamp) {
    return taskNo + "-" + bucketNumber + "-" + factUpdatedtimeStamp + INDEX_FILE_EXT;
  }

  /**
   * Gets absolute path of data file
   *
   * @param partitionId unique partition identifier
   * @param segmentId   unique partition identifier
   * @return absolute path of data file stored in carbon data format
   */
  public String getCarbonDataDirectoryPath(String partitionId, String segmentId) {
    return getSegmentDir(partitionId, segmentId);
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
      int batchNo, String factUpdateTimeStamp) {
    return DATA_PART_PREFIX + filePartNo + "-" + taskNo + BATCH_PREFIX + batchNo + "-"
        + bucketNumber + "-" + factUpdateTimeStamp + CARBON_DATA_EXT;
  }

  /**
   * Below method will be used to get the carbon index filename
   *
   * @param taskNo               task number
   * @param factUpdatedTimeStamp time stamp
   * @return filename
   */
  public static String getCarbonIndexFileName(long taskNo, int bucketNumber, int batchNo,
      String factUpdatedTimeStamp) {
    return taskNo + BATCH_PREFIX + batchNo + "-" + bucketNumber + "-" + factUpdatedTimeStamp
        + INDEX_FILE_EXT;
  }

  public static String getCarbonStreamIndexFileName() {
    return getCarbonIndexFileName(0, 0, 0, "0");
  }

  public static String getCarbonStreamIndexFilePath(String segmentDir) {
    return segmentDir + File.separator + getCarbonStreamIndexFileName();
  }

  public String getSegmentDir(String partitionId, String segmentId) {
    return getPartitionDir(partitionId) + File.separator + SEGMENT_PREFIX + segmentId;
  }

  public String getPartitionDir(String partitionId) {
    return getFactDir() + File.separator + PARTITION_PREFIX + partitionId;
  }

  private String getMetaDataDir() {
    return tablePath + File.separator + METADATA_DIR;
  }

  public String getFactDir() {
    return tablePath + File.separator + FACT_DIR;
  }

  public String getStreamingLogDir() {
    return tablePath + File.separator + STREAMING_DIR + File.separator + STREAMING_LOG_DIR;
  }

  public String getStreamingCheckpointDir() {
    return tablePath + File.separator + STREAMING_DIR + File.separator + STREAMING_CHECKPOINT_DIR;
  }

  public CarbonTableIdentifier getCarbonTableIdentifier() {
    return carbonTableIdentifier;
  }

  @Override public boolean equals(Object o) {
    if (!(o instanceof CarbonTablePath)) {
      return false;
    }
    CarbonTablePath path = (CarbonTablePath) o;
    return tablePath.equals(path.tablePath) && super.equals(o);
  }

  @Override public int hashCode() {
    return super.hashCode() + tablePath.hashCode();
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
     * This will return the timestamp present in the delete delta file.
     * @param fileName
     * @return
     */
    public static String getTimeStampFromDeleteDeltaFile(String fileName) {
      return fileName.substring(fileName.lastIndexOf(CarbonCommonConstants.HYPHEN) + 1,
          fileName.lastIndexOf("."));
    }

    /**
     * This will return the timestamp present in the delete delta file.
     * @param fileName
     * @return
     */
    public static String getBlockNameFromDeleteDeltaFile(String fileName) {
      return fileName.substring(0,
          fileName.lastIndexOf(CarbonCommonConstants.HYPHEN));
    }

    /**
     * gets updated timestamp information from given carbon data file name
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
     * gets file part number information from given carbon data file name
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
     * gets updated timestamp information from given carbon data file name
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
     * get the taskId part from taskNo(include taskId + batchNo)
     * @param taskNo
     * @return
     */
    public static long getTaskIdFromTaskNo(String taskNo) {
      return Long.parseLong(taskNo.split(BATCH_PREFIX)[0]);
    }

    public static int getBatchNoFromTaskNo(String taskNo) {
      return Integer.parseInt(taskNo.split(BATCH_PREFIX)[1]);
    }

    /**
     * Gets the file name from file path
     */
    private static String getFileName(String carbonDataFileName) {
      int endIndex = carbonDataFileName.lastIndexOf(CarbonCommonConstants.FILE_SEPARATOR);
      if (endIndex > -1) {
        return carbonDataFileName.substring(endIndex + 1, carbonDataFileName.length());
      } else {
        return carbonDataFileName;
      }
    }
  }

  /**
   * To manage data path and composition
   */
  public static class DataPathUtil {

    /**
     * gets segement id from given absolute data file path
     */
    public static String getSegmentId(String dataFileAbsolutePath) {
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
    return sortIndexDir.listFiles(new CarbonFileFilter() {
      @Override public boolean accept(CarbonFile file) {
        return file.getName().startsWith(columnUniqueId) && file.getName().endsWith(SORT_INDEX_EXT);
      }
    });
  }

  /**
   * returns the carbondata file name
   *
   * @param carbonDataFilePath carbondata file path
   * @return
   */
  public static String getCarbonDataFileName(String carbonDataFilePath) {
    return carbonDataFilePath
        .substring(carbonDataFilePath.lastIndexOf(CarbonCommonConstants.FILE_SEPARATOR) + 1,
            carbonDataFilePath.indexOf(CARBON_DATA_EXT));
  }

  /**
   * @return prefix of carbon data
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
   * This method will append strings in path and return block id
   *
   * @param shortBlockId
   * @return blockId
   */
  public static String getBlockId(String shortBlockId) {
    String[] splitRecords = shortBlockId.split(CarbonCommonConstants.FILE_SEPARATOR);
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < splitRecords.length; i++) {
      if (i == 0) {
        sb.append(PARTITION_PREFIX);
        sb.append(splitRecords[i]);
      } else if (i == 1) {
        sb.append(CarbonCommonConstants.FILE_SEPARATOR);
        sb.append(SEGMENT_PREFIX);
        sb.append(splitRecords[i]);
      } else if (i == 2) {
        sb.append(CarbonCommonConstants.FILE_SEPARATOR);
        sb.append(DATA_PART_PREFIX);
        sb.append(splitRecords[i]);
      } else if (i == 3) {
        sb.append(CarbonCommonConstants.FILE_SEPARATOR);
        sb.append(splitRecords[i]);
        sb.append(CARBON_DATA_EXT);
      } else {
        sb.append(CarbonCommonConstants.FILE_SEPARATOR);
        sb.append(splitRecords[i]);
      }
    }
    return sb.toString();
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
    return DataFileUtil.getTaskNo(actualBlockName) + "-" + DataFileUtil.getBucketNo(actualBlockName)
        + "-" + DataFileUtil.getTimeStampFromFileName(actualBlockName) + INDEX_FILE_EXT;
  }

  /**
   * Get the segment path from table path and segmentid
   */
  public static String getSegmentPath(String tablePath, String segmentId) {
    return tablePath + "/Fact/Part0/Segment_" + segmentId;
  }
}