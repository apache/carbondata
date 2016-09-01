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
package org.apache.carbondata.integration.spark.merger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.carbon.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.carbon.datastore.block.TaskBlockInfo;
import org.apache.carbondata.core.carbon.datastore.exception.IndexBuilderException;
import org.apache.carbondata.core.carbon.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.carbon.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.carbon.path.CarbonTablePath;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastorage.store.impl.FileFactory;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.CarbonUtilException;

import org.apache.spark.sql.hive.TableMeta;

/**
 * Utility Class for the Compaction Flow.
 */
public class CarbonCompactionUtil {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonCompactionExecutor.class.getName());

  /**
   * To create a mapping of Segment Id and TableBlockInfo.
   *
   * @param tableBlockInfoList
   * @return
   */
  public static Map<String, TaskBlockInfo> createMappingForSegments(
      List<TableBlockInfo> tableBlockInfoList) {

    // stores taskBlockInfo of each segment
    Map<String, TaskBlockInfo> segmentBlockInfoMapping =
        new HashMap<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);


    for (TableBlockInfo info : tableBlockInfoList) {
      String segId = info.getSegmentId();
      // check if segId is already present in map
      TaskBlockInfo taskBlockInfoMapping = segmentBlockInfoMapping.get(segId);
      // extract task ID from file Path.
      String taskNo = CarbonTablePath.DataFileUtil.getTaskNo(info.getFilePath());
      // if taskBlockInfo is not there, then create and add
      if (null == taskBlockInfoMapping) {
        taskBlockInfoMapping = new TaskBlockInfo();
        groupCorrespodingInfoBasedOnTask(info, taskBlockInfoMapping, taskNo);
        // put the taskBlockInfo with respective segment id
        segmentBlockInfoMapping.put(segId, taskBlockInfoMapping);
      } else
      {
        groupCorrespodingInfoBasedOnTask(info, taskBlockInfoMapping, taskNo);
      }
    }
    return segmentBlockInfoMapping;

  }

  /**
   * Grouping the taskNumber and list of TableBlockInfo.
   * @param info
   * @param taskBlockMapping
   * @param taskNo
   */
  private static void groupCorrespodingInfoBasedOnTask(TableBlockInfo info,
      TaskBlockInfo taskBlockMapping, String taskNo) {
    // get the corresponding list from task mapping.
    List<TableBlockInfo> blockLists = taskBlockMapping.getTableBlockInfoList(taskNo);
    if (null != blockLists) {
      blockLists.add(info);
    } else {
      blockLists = new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
      blockLists.add(info);
      taskBlockMapping.addTableBlockInfoList(taskNo, blockLists);
    }
  }

  /**
   * To create a mapping of Segment Id and DataFileFooter.
   *
   * @param tableBlockInfoList
   * @return
   */
  public static Map<String, List<DataFileFooter>> createDataFileFooterMappingForSegments(
      List<TableBlockInfo> tableBlockInfoList) throws IndexBuilderException {

    Map<String, List<DataFileFooter>> segmentBlockInfoMapping = new HashMap<>();
    for (TableBlockInfo blockInfo : tableBlockInfoList) {
      List<DataFileFooter> eachSegmentBlocks = new ArrayList<>();
      String segId = blockInfo.getSegmentId();

      DataFileFooter dataFileMatadata = null;
      // check if segId is already present in map
      List<DataFileFooter> metadataList = segmentBlockInfoMapping.get(segId);
      try {
        dataFileMatadata = CarbonUtil
            .readMetadatFile(blockInfo.getFilePath(), blockInfo.getBlockOffset(),
                blockInfo.getBlockLength());
      } catch (CarbonUtilException e) {
        throw new IndexBuilderException(e);
      }
      if (null == metadataList) {
        // if it is not present
        eachSegmentBlocks.add(dataFileMatadata);
        segmentBlockInfoMapping.put(segId, eachSegmentBlocks);
      } else {

        // if its already present then update the list.
        metadataList.add(dataFileMatadata);
      }
    }
    return segmentBlockInfoMapping;

  }

  /**
   * Check whether the file to indicate the compaction is present or not.
   * @param metaFolderPath
   * @return
   */
  public static boolean isCompactionRequiredForTable(String metaFolderPath) {
    String minorCompactionStatusFile = metaFolderPath + CarbonCommonConstants.FILE_SEPARATOR
        + CarbonCommonConstants.minorCompactionRequiredFile;

    String majorCompactionStatusFile = metaFolderPath + CarbonCommonConstants.FILE_SEPARATOR
        + CarbonCommonConstants.majorCompactionRequiredFile;
    try {
      if (FileFactory.isFileExist(minorCompactionStatusFile,
          FileFactory.getFileType(minorCompactionStatusFile)) || FileFactory
          .isFileExist(majorCompactionStatusFile,
              FileFactory.getFileType(majorCompactionStatusFile))) {
        return true;
      }
    } catch (IOException e) {
      LOGGER.error("Exception in isFileExist compaction request file " + e.getMessage() );
    }
    return false;
  }

  /**
   * Determine the type of the compaction received.
   * @param metaFolderPath
   * @return
   */
  public static CompactionType determineCompactionType(String metaFolderPath) {
    String minorCompactionStatusFile = metaFolderPath + CarbonCommonConstants.FILE_SEPARATOR
        + CarbonCommonConstants.minorCompactionRequiredFile;

    String majorCompactionStatusFile = metaFolderPath + CarbonCommonConstants.FILE_SEPARATOR
        + CarbonCommonConstants.majorCompactionRequiredFile;
    try {
      if (FileFactory.isFileExist(minorCompactionStatusFile,
          FileFactory.getFileType(minorCompactionStatusFile))) {
        return CompactionType.MINOR_COMPACTION;
      }
      if (FileFactory.isFileExist(majorCompactionStatusFile,
          FileFactory.getFileType(majorCompactionStatusFile))) {
        return CompactionType.MAJOR_COMPACTION;
      }

    } catch (IOException e) {
      LOGGER.error("Exception in determining the compaction request file " + e.getMessage() );
    }
    return CompactionType.MINOR_COMPACTION;
  }

  /**
   * Delete the compation request file once the compaction is done.
   * @param metaFolderPath
   * @param compactionType
   * @return
   */
  public static boolean deleteCompactionRequiredFile(String metaFolderPath,
      CompactionType compactionType) {
    String statusFile;
    if (compactionType.equals(CompactionType.MINOR_COMPACTION)) {
      statusFile = metaFolderPath + CarbonCommonConstants.FILE_SEPARATOR
          + CarbonCommonConstants.minorCompactionRequiredFile;
    } else {
      statusFile = metaFolderPath + CarbonCommonConstants.FILE_SEPARATOR
          + CarbonCommonConstants.majorCompactionRequiredFile;
    }
    try {
      if (FileFactory.isFileExist(statusFile, FileFactory.getFileType(statusFile))) {
        if (FileFactory.getCarbonFile(statusFile, FileFactory.getFileType(statusFile)).delete()) {
          LOGGER.info("Deleted the compaction request file " + statusFile);
        } else {
          LOGGER.error("Unable to delete the compaction request file " + statusFile);
        }
      }
    } catch (IOException e) {
      LOGGER.error("Exception in deleting the compaction request file " + e.getMessage() );
    }
    return false;
  }

  /**
   * Creation of the compaction request if someother compaction is in progress.
   * @param metaFolderPath
   * @param compactionType
   * @return
   */
  public static boolean createCompactionRequiredFile(String metaFolderPath,
      CompactionType compactionType) {
    String statusFile;
    if (compactionType.equals(CompactionType.MINOR_COMPACTION)) {
      statusFile = metaFolderPath + CarbonCommonConstants.FILE_SEPARATOR
          + CarbonCommonConstants.minorCompactionRequiredFile;
    } else {
      statusFile = metaFolderPath + CarbonCommonConstants.FILE_SEPARATOR
          + CarbonCommonConstants.majorCompactionRequiredFile;
    }
    try {
      if (!FileFactory.isFileExist(statusFile, FileFactory.getFileType(statusFile))) {
        if (FileFactory.createNewFile(statusFile, FileFactory.getFileType(statusFile))) {
          LOGGER.info("successfully created a compaction required file - " + statusFile);
          return true;
        } else {
          LOGGER.error("Not able to create a compaction required file - " + statusFile);
          return false;
        }
      }
    } catch (IOException e) {
      LOGGER.error("Exception in creating the compaction request file " + e.getMessage() );
    }
    return false;
  }

  /**
   * This will check if any compaction request has been received for any table.
   * @param tableMetas
   * @return
   */
  public static TableMeta getNextTableToCompact(TableMeta[] tableMetas) {
    for (TableMeta table : tableMetas) {
      CarbonTable ctable = table.carbonTable();
      String metadataPath = ctable.getMetaDataFilepath();
      if (CarbonCompactionUtil.isCompactionRequiredForTable(metadataPath)) {
        return table;
      }
    }
    return null;
  }
}
