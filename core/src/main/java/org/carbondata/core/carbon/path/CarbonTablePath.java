/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.carbondata.core.carbon.path;

import java.io.File;

import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.carbondata.core.datastorage.store.filesystem.CarbonFileFilter;
import org.carbondata.core.datastorage.store.impl.FileFactory;

import static org.carbondata.core.constants.CarbonCommonConstants.INVALID_SEGMENT_ID;

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
  private static final String AGGREGATE_TABLE_PREFIX = "Agg";
  private static final String SEGMENT_PREFIX = "Segment_";
  private static final String PARTITION_PREFIX = "Part";
  private static final String CARBON_DATA_EXT = ".carbondata";
  private static final String DATA_PART_PREFIX = "part";
  private static final String INDEX_FILE_EXT = ".carbonindex";

  private String tablePath;

  public CarbonTablePath(String tablePathString) {
    super(tablePathString);
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
   * This method will return the metadata directory location for a table
   *
   * @return
   */
  public String getMetadataDirectoryPath() {
    return getMetaDataDir();
  }

  /**
   * @param columnId unique column identifier
   * @return absolute path of dictionary meta file
   */
  public String getDictionaryMetaFilePath(String columnId) {
    return getMetaDataDir() + File.separator + columnId + DICTIONARY_META_EXT;
  }

  /**
   * @param columnId unique column identifier
   * @return absolute path of sort index file
   */
  public String getSortIndexFilePath(String columnId) {
    return getMetaDataDir() + File.separator + columnId + SORT_INDEX_EXT;
  }

  /**
   * @return absolute path of schema file
   */
  public String getSchemaFilePath() {
    return getMetaDataDir() + File.separator + SCHEMA_FILE;
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
      Integer taskNo, String factUpdateTimeStamp) {
    return getSegmentDir(partitionId, segmentId) + File.separator + getCarbonDataFileName(
        filePartNo, taskNo, factUpdateTimeStamp);
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
      final String segmentId) {
    String segmentDir = getSegmentDir(partitionId, segmentId);
    CarbonFile carbonFile =
        FileFactory.getCarbonFile(segmentDir, FileFactory.getFileType(segmentDir));

    CarbonFile[] files = carbonFile.listFiles(new CarbonFileFilter() {
      @Override public boolean accept(CarbonFile file) {
        return file.getName().startsWith(taskId) && file.getName().endsWith(INDEX_FILE_EXT);
      }
    });
    return files[0].getAbsolutePath();
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
   * Gets absolute path of data file of given aggregate table
   *
   * @param aggTableID          unique aggregate table identifier
   * @param partitionId         unique partition identifier
   * @param segmentId           unique partition identifier
   * @param filePartNo          data file part number
   * @param factUpdateTimeStamp unique identifier to identify an update
   * @return absolute path of data file stored in carbon data format
   */
  public String getCarbonAggDataFilePath(String aggTableID, String partitionId, String segmentId,
      Integer filePartNo, Integer taskNo, String factUpdateTimeStamp) {
    return getAggSegmentDir(aggTableID, partitionId, segmentId) + File.separator
        + getCarbonDataFileName(filePartNo, taskNo, factUpdateTimeStamp);
  }

  /**
   * Gets data file name only with out path
   *
   * @param filePartNo          data file part number
   * @param taskNo              task identifier
   * @param factUpdateTimeStamp unique identifier to identify an update
   * @return gets data file name only with out path
   */
  public String getCarbonDataFileName(Integer filePartNo, Integer taskNo,
      String factUpdateTimeStamp) {
    return DATA_PART_PREFIX + "-" + filePartNo + "-" + taskNo + "-" + factUpdateTimeStamp
        + CARBON_DATA_EXT;
  }

  /**
   * Below method will be used to get the carbon index filename
   *
   * @param taskNo               task number
   * @param factUpdatedTimeStamp time stamp
   * @return filename
   */
  public String getCarbonIndexFileName(int taskNo, String factUpdatedTimeStamp) {
    return taskNo + "-" + factUpdatedTimeStamp + INDEX_FILE_EXT;
  }

  private String getSegmentDir(String partitionId, String segmentId) {
    return getPartitionDir(partitionId) + File.separator + SEGMENT_PREFIX + segmentId;
  }

  public String getPartitionDir(String partitionId) {
    return getFactDir() + File.separator + PARTITION_PREFIX + partitionId;
  }

  private String getAggSegmentDir(String aggTableID, String partitionId, String segmentId) {
    return getAggPartitionDir(aggTableID, partitionId) + File.separator + SEGMENT_PREFIX
        + segmentId;
  }

  private String getAggPartitionDir(String aggTableID, String partitionId) {
    return getAggregateTableDir(aggTableID) + File.separator + PARTITION_PREFIX + partitionId;
  }

  private String getMetaDataDir() {
    return tablePath + File.separator + METADATA_DIR;
  }

  public String getFactDir() {
    return tablePath + File.separator + FACT_DIR;
  }

  private String getAggregateTableDir(String aggTableId) {
    return tablePath + File.separator + AGGREGATE_TABLE_PREFIX + aggTableId;
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
    public static String getUpdateTimeStamp(String carbonDataFileName) {
      // + 1 for size of "-"
      int firstDashPos = carbonDataFileName.indexOf("-");
      int secondDashPos = carbonDataFileName.indexOf("-", firstDashPos + 1);
      int startIndex = carbonDataFileName.indexOf("-", secondDashPos + 1) + 1;
      int endIndex = carbonDataFileName.indexOf(".");
      return carbonDataFileName.substring(startIndex, endIndex);
    }

    /**
     * gets file part number information from given carbon data file name
     */
    public static String getPartNo(String carbonDataFileName) {
      // + 1 for size of "-"
      int startIndex = carbonDataFileName.indexOf("-") + 1;
      int endIndex = carbonDataFileName.indexOf("-", startIndex);
      return carbonDataFileName.substring(startIndex, endIndex);
    }

    /**
     * gets updated timestamp information from given carbon data file name
     */
    public static String getTaskNo(String carbonDataFileName) {
      // + 1 for size of "-"
      int firstDashPos = carbonDataFileName.indexOf("-");
      int startIndex = carbonDataFileName.indexOf("-", firstDashPos + 1) + 1;
      int endIndex = carbonDataFileName.indexOf("-", startIndex);
      return carbonDataFileName.substring(startIndex, endIndex);
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
      int endIndex = dataFileAbsolutePath.lastIndexOf(CarbonCommonConstants.FILE_SEPARATOR);
      // + 1 for size of "/"
      int startIndex =
          dataFileAbsolutePath.lastIndexOf(CarbonCommonConstants.FILE_SEPARATOR, endIndex - 1) + 1;
      String segmentDirStr = dataFileAbsolutePath.substring(startIndex, endIndex);
      //identify id in segment_<id>
      String[] segmentDirSplits = segmentDirStr.split("_");
      try {
        if (segmentDirSplits.length == 2) {
          return segmentDirSplits[1];
        }
      } catch (Exception e) {
        return INVALID_SEGMENT_ID;
      }
      return INVALID_SEGMENT_ID;
    }
  }
}