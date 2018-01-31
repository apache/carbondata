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
package org.apache.carbondata.processing.merger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.datastore.block.TaskBlockInfo;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import org.apache.commons.lang3.ArrayUtils;

/**
 * Utility Class for the Compaction Flow.
 */
public class CarbonCompactionUtil {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonCompactionUtil.class.getName());

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
      List<TableBlockInfo> tableBlockInfoList) throws IOException {

    Map<String, List<DataFileFooter>> segmentBlockInfoMapping = new HashMap<>();
    for (TableBlockInfo blockInfo : tableBlockInfoList) {
      List<DataFileFooter> eachSegmentBlocks = new ArrayList<>();
      String segId = blockInfo.getSegmentId();
      DataFileFooter dataFileMatadata = null;
      // check if segId is already present in map
      List<DataFileFooter> metadataList = segmentBlockInfoMapping.get(segId);
      // check to decide whether to read file footer of carbondata file forcefully. This will help
      // in getting the schema last updated time based on which compaction flow is decided that
      // whether it will go to restructure compaction flow or normal compaction flow.
      // This decision will impact the compaction performance so it needs to be decided carefully
      if (null != blockInfo.getDetailInfo()
          && blockInfo.getDetailInfo().getSchemaUpdatedTimeStamp() == 0L) {
        dataFileMatadata = CarbonUtil.readMetadatFile(blockInfo, true);
      } else {
        dataFileMatadata = CarbonUtil.readMetadatFile(blockInfo);
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
      LOGGER.error("Exception in isFileExist compaction request file " + e.getMessage());
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
        return CompactionType.MINOR;
      }
      if (FileFactory.isFileExist(majorCompactionStatusFile,
          FileFactory.getFileType(majorCompactionStatusFile))) {
        return CompactionType.MAJOR;
      }

    } catch (IOException e) {
      LOGGER.error("Exception in determining the compaction request file " + e.getMessage());
    }
    return CompactionType.MINOR;
  }

  /**
   * Delete the compation request file once the compaction is done.
   * @param metaFolderPath
   * @param compactionType
   * @return
   */
  public static boolean deleteCompactionRequiredFile(String metaFolderPath,
      CompactionType compactionType) {
    String compactionRequiredFile;
    if (compactionType.equals(CompactionType.MINOR)) {
      compactionRequiredFile = metaFolderPath + CarbonCommonConstants.FILE_SEPARATOR
          + CarbonCommonConstants.minorCompactionRequiredFile;
    } else {
      compactionRequiredFile = metaFolderPath + CarbonCommonConstants.FILE_SEPARATOR
          + CarbonCommonConstants.majorCompactionRequiredFile;
    }
    try {
      if (FileFactory
          .isFileExist(compactionRequiredFile, FileFactory.getFileType(compactionRequiredFile))) {
        if (FileFactory
            .getCarbonFile(compactionRequiredFile, FileFactory.getFileType(compactionRequiredFile))
            .delete()) {
          LOGGER.info("Deleted the compaction request file " + compactionRequiredFile);
          return true;
        } else {
          LOGGER.error("Unable to delete the compaction request file " + compactionRequiredFile);
        }
      } else {
        LOGGER.info("Compaction request file is not present. file is : " + compactionRequiredFile);
      }
    } catch (IOException e) {
      LOGGER.error("Exception in deleting the compaction request file " + e.getMessage());
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
    if (CompactionType.MINOR == compactionType) {
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
      } else {
        LOGGER.info("Compaction request file : " + statusFile + " already exist.");
      }
    } catch (IOException e) {
      LOGGER.error("Exception in creating the compaction request file " + e.getMessage());
    }
    return false;
  }

  /**
   * This will check if any compaction request has been received for any table.
   *
   * @param carbonTables
   * @return
   */
  public static CarbonTable getNextTableToCompact(CarbonTable[] carbonTables,
      List<CarbonTableIdentifier> skipList) {
    for (CarbonTable ctable : carbonTables) {
      String metadataPath = ctable.getMetadataPath();
      // check for the compaction required file and at the same time exclude the tables which are
      // present in the skip list.
      if (CarbonCompactionUtil.isCompactionRequiredForTable(metadataPath) && !skipList
          .contains(ctable.getCarbonTableIdentifier())) {
        return ctable;
      }
    }
    return null;
  }

  /**
   * This method will add the prepare the max column cardinality map
   *
   * @param columnCardinalityMap
   * @param currentBlockSchema
   * @param currentBlockCardinality
   */
  public static void addColumnCardinalityToMap(Map<String, Integer> columnCardinalityMap,
      List<ColumnSchema> currentBlockSchema, int[] currentBlockCardinality) {
    for (int i = 0; i < currentBlockCardinality.length; i++) {
      // add value to map only if does not exist or new cardinality is > existing value
      String columnUniqueId = currentBlockSchema.get(i).getColumnUniqueId();
      Integer value = columnCardinalityMap.get(columnUniqueId);
      if (null == value) {
        columnCardinalityMap.put(columnUniqueId, currentBlockCardinality[i]);
      } else {
        if (currentBlockCardinality[i] > value) {
          columnCardinalityMap.put(columnUniqueId, currentBlockCardinality[i]);
        }
      }
    }
  }

  /**
   * This method will return the updated cardinality according to the master schema
   *
   * @param columnCardinalityMap
   * @param carbonTable
   * @param updatedColumnSchemaList
   * @return
   */
  public static int[] updateColumnSchemaAndGetCardinality(Map<String, Integer> columnCardinalityMap,
      CarbonTable carbonTable, List<ColumnSchema> updatedColumnSchemaList) {
    List<CarbonDimension> masterDimensions =
        carbonTable.getDimensionByTableName(carbonTable.getTableName());
    List<Integer> updatedCardinalityList = new ArrayList<>(columnCardinalityMap.size());
    for (CarbonDimension dimension : masterDimensions) {
      Integer value = columnCardinalityMap.get(dimension.getColumnId());
      if (null == value) {
        updatedCardinalityList.add(getDimensionDefaultCardinality(dimension));
      } else {
        updatedCardinalityList.add(value);
      }
      updatedColumnSchemaList.add(dimension.getColumnSchema());
    }
    // add measures to the column schema list
    List<CarbonMeasure> masterSchemaMeasures =
        carbonTable.getMeasureByTableName(carbonTable.getTableName());
    for (CarbonMeasure measure : masterSchemaMeasures) {
      updatedColumnSchemaList.add(measure.getColumnSchema());
    }
    return ArrayUtils
        .toPrimitive(updatedCardinalityList.toArray(new Integer[updatedCardinalityList.size()]));
  }

  /**
   * This method will return the default cardinality based on dimension type
   *
   * @param dimension
   * @return
   */
  private static int getDimensionDefaultCardinality(CarbonDimension dimension) {
    int cardinality = 0;
    if (dimension.hasEncoding(Encoding.DIRECT_DICTIONARY)) {
      cardinality = Integer.MAX_VALUE;
    } else if (dimension.hasEncoding(Encoding.DICTIONARY)) {
      if (null != dimension.getDefaultValue()) {
        cardinality = CarbonCommonConstants.DICTIONARY_DEFAULT_CARDINALITY + 1;
      } else {
        cardinality = CarbonCommonConstants.DICTIONARY_DEFAULT_CARDINALITY;
      }
    } else {
      cardinality = -1;
    }
    return cardinality;
  }

  /**
   * This method will check for any restructured block in the blocks selected for compaction
   *
   * @param segmentMapping
   * @param dataFileMetadataSegMapping
   * @param tableLastUpdatedTime
   * @return
   */
  public static boolean checkIfAnyRestructuredBlockExists(Map<String, TaskBlockInfo> segmentMapping,
      Map<String, List<DataFileFooter>> dataFileMetadataSegMapping, long tableLastUpdatedTime) {
    boolean restructuredBlockExists = false;
    for (Map.Entry<String, TaskBlockInfo> taskMap : segmentMapping.entrySet()) {
      String segmentId = taskMap.getKey();
      List<DataFileFooter> listMetadata = dataFileMetadataSegMapping.get(segmentId);
      for (DataFileFooter dataFileFooter : listMetadata) {
        // if schema modified timestamp is greater than footer stored schema timestamp,
        // it indicates it is a restructured block
        if (tableLastUpdatedTime > dataFileFooter.getSchemaUpdatedTimeStamp()) {
          restructuredBlockExists = true;
          break;
        }
      }
      if (restructuredBlockExists) {
        break;
      }
    }
    return restructuredBlockExists;
  }

  /**
   * This method will check for any restructured block in the blocks selected for compaction
   *
   * @param segmentMapping
   * @param tableLastUpdatedTime
   * @return
   */
  public static boolean checkIfAnyRestructuredBlockExists(Map<String, TaskBlockInfo> segmentMapping,
      long tableLastUpdatedTime) {
    boolean restructuredBlockExists = false;
    for (Map.Entry<String, TaskBlockInfo> taskMap : segmentMapping.entrySet()) {
      String segmentId = taskMap.getKey();
      TaskBlockInfo taskBlockInfo = taskMap.getValue();
      Collection<List<TableBlockInfo>> infoList = taskBlockInfo.getAllTableBlockInfoList();
      for (List<TableBlockInfo> listMetadata : infoList) {
        for (TableBlockInfo blockInfo : listMetadata) {
          // if schema modified timestamp is greater than footer stored schema timestamp,
          // it indicates it is a restructured block
          if (tableLastUpdatedTime > blockInfo.getDetailInfo().getSchemaUpdatedTimeStamp()) {
            restructuredBlockExists = true;
            break;
          }
        }
      }
      if (restructuredBlockExists) {
        break;
      }
    }
    return restructuredBlockExists;
  }
}
