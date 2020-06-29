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

package org.apache.carbondata.core.indexstore.blockletindex;

import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.block.SegmentPropertiesAndSchemaHolder;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.index.IndexFilter;
import org.apache.carbondata.core.index.Segment;
import org.apache.carbondata.core.index.dev.IndexModel;
import org.apache.carbondata.core.index.dev.cgindex.CoarseGrainIndex;
import org.apache.carbondata.core.indexstore.AbstractMemoryDMStore;
import org.apache.carbondata.core.indexstore.BlockMetaInfo;
import org.apache.carbondata.core.indexstore.Blocklet;
import org.apache.carbondata.core.indexstore.ExtendedBlocklet;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.indexstore.SafeMemoryDMStore;
import org.apache.carbondata.core.indexstore.UnsafeMemoryDMStore;
import org.apache.carbondata.core.indexstore.row.IndexRow;
import org.apache.carbondata.core.indexstore.row.IndexRowImpl;
import org.apache.carbondata.core.indexstore.schema.CarbonRowSchema;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.metadata.blocklet.index.BlockletMinMaxIndex;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.profiler.ExplainCollector;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.filter.FilterExpressionProcessor;
import org.apache.carbondata.core.scan.filter.FilterUtil;
import org.apache.carbondata.core.scan.filter.executer.FilterExecuter;
import org.apache.carbondata.core.scan.filter.executer.ImplicitColumnFilterExecutor;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.util.BlockletIndexUtil;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataFileFooterConverter;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * Index implementation for block.
 */
public class BlockIndex extends CoarseGrainIndex
    implements BlockletIndexRowIndexes, Serializable {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3704

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(BlockIndex.class.getName());

  protected static final long serialVersionUID = -2170289352240810993L;
  /**
   * for CACHE_LEVEL=BLOCK and legacy store default blocklet id will be -1
   */
  private static final short BLOCK_DEFAULT_BLOCKLET_ID = -1;
  /**
   * store which will hold all the block or blocklet entries in one task
   */
  protected AbstractMemoryDMStore memoryDMStore;
  /**
   * task summary holder store
   */
  protected AbstractMemoryDMStore taskSummaryDMStore;
  /**
   * index of segmentProperties in the segmentProperties holder
   */
  protected transient SegmentPropertiesAndSchemaHolder.SegmentPropertiesWrapper
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3482
      segmentPropertiesWrapper;
  /**
   * flag to be used for forming the complete file path from file name. It will be true in case of
   * partition table and non transactional table
   */
  protected boolean isFilePathStored;
  /**
   * flag to be used for partition table
   */
  protected boolean isPartitionTable;

  @Override
  public void init(IndexModel indexModel) throws IOException {
    long startTime = System.currentTimeMillis();
    assert (indexModel instanceof BlockletIndexModel);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765
    BlockletIndexModel blockletIndexModel = (BlockletIndexModel) indexModel;
    DataFileFooterConverter fileFooterConverter =
        new DataFileFooterConverter(indexModel.getConfiguration());
    List<DataFileFooter> indexInfo = null;
    if (blockletIndexModel.getIndexInfos() == null || blockletIndexModel.getIndexInfos()
        .isEmpty()) {
      indexInfo = fileFooterConverter
          .getIndexInfo(blockletIndexModel.getFilePath(), blockletIndexModel.getFileData(),
              blockletIndexModel.getCarbonTable().isTransactionalTable());
    } else {
      // when index info is already read and converted to data file footer object
      indexInfo = blockletIndexModel.getIndexInfos();
    }
    String path = blockletIndexModel.getFilePath();
    // store file path only in case of partition table, non transactional table and flat folder
    // structure
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3557
    byte[] filePath;
    this.isPartitionTable = blockletIndexModel.getCarbonTable().isHivePartitionTable();
    if (this.isPartitionTable || !blockletIndexModel.getCarbonTable().isTransactionalTable() ||
        blockletIndexModel.getCarbonTable().isSupportFlatFolder() ||
        // if the segment data is written in tablepath then no need to store whole path of file.
        !blockletIndexModel.getFilePath().startsWith(
            blockletIndexModel.getCarbonTable().getTablePath())) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3659
      filePath = FilenameUtils.getFullPathNoEndSeparator(path)
              .getBytes(CarbonCommonConstants.DEFAULT_CHARSET);
      isFilePathStored = true;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3557
    } else {
      filePath = new byte[0];
    }
    byte[] fileName = path.substring(path.lastIndexOf("/") + 1, path.length())
        .getBytes(CarbonCommonConstants.DEFAULT_CHARSET);
    byte[] segmentId =
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765
        blockletIndexModel.getSegmentId().getBytes(CarbonCommonConstants.DEFAULT_CHARSET);
    if (!indexInfo.isEmpty()) {
      DataFileFooter fileFooter = indexInfo.get(0);
      // init segment properties and create schema
      SegmentProperties segmentProperties = initSegmentProperties(blockletIndexModel, fileFooter);
      createMemorySchema(blockletIndexModel);
      createSummaryDMStore(blockletIndexModel);
      CarbonRowSchema[] taskSummarySchema = getTaskSummarySchema();
      // check for legacy store and load the metadata
      IndexRowImpl summaryRow =
          loadMetadata(taskSummarySchema, segmentProperties, blockletIndexModel, indexInfo);
      finishWriting(taskSummarySchema, filePath, fileName, segmentId, summaryRow);
      if (((BlockletIndexModel) indexModel).isSerializeDmStore()) {
        serializeDmStore();
      }
    }
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765
          "Time taken to load blocklet index from file : " + indexModel.getFilePath() + " is "
              + (System.currentTimeMillis() - startTime));
    }
  }

  private void finishWriting(CarbonRowSchema[] taskSummarySchema, byte[] filePath, byte[] fileName,
      byte[] segmentId, IndexRowImpl summaryRow) {
    if (memoryDMStore != null) {
      memoryDMStore.finishWriting();
    }
    if (null != taskSummaryDMStore) {
      addTaskSummaryRowToUnsafeMemoryStore(taskSummarySchema, summaryRow, filePath, fileName,
          segmentId);
      taskSummaryDMStore.finishWriting();
    }
  }

  private void serializeDmStore() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3680
    if (memoryDMStore != null) {
      memoryDMStore.serializeMemoryBlock();
    }
    if (null != taskSummaryDMStore) {
      taskSummaryDMStore.serializeMemoryBlock();
    }
  }

  /**
   * Method to check the cache level and load metadata based on that information
   *
   * @param segmentProperties
   * @param blockletIndexModel
   * @param indexInfo
   */
  protected IndexRowImpl loadMetadata(CarbonRowSchema[] taskSummarySchema,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765
      SegmentProperties segmentProperties, BlockletIndexModel blockletIndexModel,
      List<DataFileFooter> indexInfo) {
    return loadBlockMetaInfo(taskSummarySchema, segmentProperties, blockletIndexModel, indexInfo);
  }

  /**
   * initialise segment properties
   *
   * @param fileFooter
   * @throws IOException
   */
  private SegmentProperties initSegmentProperties(BlockletIndexModel blockletIndexModel,
      DataFileFooter fileFooter) {
    List<ColumnSchema> columnInTable = fileFooter.getColumnInTable();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3482
    segmentPropertiesWrapper = SegmentPropertiesAndSchemaHolder.getInstance()
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765
        .addSegmentProperties(blockletIndexModel.getCarbonTable(),
            columnInTable, blockletIndexModel.getSegmentId());
    return segmentPropertiesWrapper.getSegmentProperties();
  }

  protected void setMinMaxFlagForTaskSummary(IndexRow summaryRow,
      CarbonRowSchema[] taskSummarySchema, SegmentProperties segmentProperties,
      boolean[] minMaxFlag) {
    // add min max flag for all the dimension columns
    boolean[] minMaxFlagValuesForColumnsToBeCached = BlockletIndexUtil
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3036
        .getMinMaxFlagValuesForColumnsToBeCached(segmentProperties, getMinMaxCacheColumns(),
            minMaxFlag);
    addMinMaxFlagValues(summaryRow, taskSummarySchema[TASK_MIN_MAX_FLAG],
        minMaxFlagValuesForColumnsToBeCached, TASK_MIN_MAX_FLAG);
  }

  /**
   * Method to load block metadata information
   *
   * @param blockletIndexModel
   * @param indexInfo
   */
  private IndexRowImpl loadBlockMetaInfo(CarbonRowSchema[] taskSummarySchema,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765
      SegmentProperties segmentProperties, BlockletIndexModel blockletIndexModel,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3575
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3575
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3575
      List<DataFileFooter> indexInfo) {
    String tempFilePath = null;
    DataFileFooter previousDataFileFooter = null;
    int footerCounter = 0;
    byte[][] blockMinValues = null;
    byte[][] blockMaxValues = null;
    IndexRowImpl summaryRow = null;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2788
    List<Short> blockletCountInEachBlock = new ArrayList<>(indexInfo.size());
    short totalBlockletsInOneBlock = 0;
    boolean isLastFileFooterEntryNeedToBeAdded = false;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2649
    CarbonRowSchema[] schema = getFileFooterEntrySchema();
    // flag for each block entry
    boolean[] minMaxFlag = new boolean[segmentProperties.getNumberOfColumns()];
    Arrays.fill(minMaxFlag, true);
    // min max flag for task summary
    boolean[] taskSummaryMinMaxFlag = new boolean[segmentProperties.getNumberOfColumns()];
    Arrays.fill(taskSummaryMinMaxFlag, true);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3293
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3293
    long totalRowCount = 0;
    for (DataFileFooter fileFooter : indexInfo) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3684
      TableBlockInfo blockInfo = fileFooter.getBlockInfo();
      BlockMetaInfo blockMetaInfo =
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765
          blockletIndexModel.getBlockMetaInfoMap().get(blockInfo.getFilePath());
      footerCounter++;
      if (blockMetaInfo != null) {
        // this variable will be used for adding the IndexRow entry every time a unique block
        // path is encountered
        if (null == tempFilePath) {
          tempFilePath = blockInfo.getFilePath();
          // 1st time assign the min and max values from the current file footer
          blockMinValues = fileFooter.getBlockletIndex().getMinMaxIndex().getMinValues();
          blockMaxValues = fileFooter.getBlockletIndex().getMinMaxIndex().getMaxValues();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2942
          updateMinMaxFlag(fileFooter, minMaxFlag);
          updateMinMaxFlag(fileFooter, taskSummaryMinMaxFlag);
          previousDataFileFooter = fileFooter;
          totalBlockletsInOneBlock++;
        } else if (blockInfo.getFilePath().equals(tempFilePath)) {
          // After iterating over all the blocklets that belong to one block we need to compute the
          // min and max at block level. So compare min and max values and update if required
          BlockletMinMaxIndex currentFooterMinMaxIndex =
              fileFooter.getBlockletIndex().getMinMaxIndex();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2649
          blockMinValues =
              compareAndUpdateMinMax(currentFooterMinMaxIndex.getMinValues(), blockMinValues, true);
          blockMaxValues =
              compareAndUpdateMinMax(currentFooterMinMaxIndex.getMaxValues(), blockMaxValues,
                  false);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2942
          updateMinMaxFlag(fileFooter, minMaxFlag);
          updateMinMaxFlag(fileFooter, taskSummaryMinMaxFlag);
          totalBlockletsInOneBlock++;
        }
        // as one task contains entries for all the blocklets we need iterate and load only the
        // with unique file path because each unique path will correspond to one
        // block in the task. OR condition is to handle the loading of last file footer
        if (!blockInfo.getFilePath().equals(tempFilePath) || footerCounter == indexInfo.size()) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3684
          TableBlockInfo previousBlockInfo = previousDataFileFooter.getBlockInfo();
          summaryRow = loadToUnsafeBlock(schema, taskSummarySchema, previousDataFileFooter,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2649
              segmentProperties, getMinMaxCacheColumns(), previousBlockInfo.getFilePath(),
              summaryRow,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765
              blockletIndexModel.getBlockMetaInfoMap().get(previousBlockInfo.getFilePath()),
              blockMinValues, blockMaxValues, minMaxFlag);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3293
          totalRowCount += previousDataFileFooter.getNumberOfRows();
          minMaxFlag = new boolean[segmentProperties.getNumberOfColumns()];
          Arrays.fill(minMaxFlag, true);
          // flag to check whether last file footer entry is different from previous entry.
          // If yes then it need to be added at last
          isLastFileFooterEntryNeedToBeAdded =
              (footerCounter == indexInfo.size()) && (!blockInfo.getFilePath()
                  .equals(tempFilePath));
          // assign local variables values using the current file footer
          tempFilePath = blockInfo.getFilePath();
          blockMinValues = fileFooter.getBlockletIndex().getMinMaxIndex().getMinValues();
          blockMaxValues = fileFooter.getBlockletIndex().getMinMaxIndex().getMaxValues();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2942
          updateMinMaxFlag(fileFooter, minMaxFlag);
          updateMinMaxFlag(fileFooter, taskSummaryMinMaxFlag);
          previousDataFileFooter = fileFooter;
          blockletCountInEachBlock.add(totalBlockletsInOneBlock);
          // for next block count will start from 1 because a row is created whenever a new file
          // path comes. Here already a new file path has come so the count should start from 1
          totalBlockletsInOneBlock = 1;
        }
      }
    }
    // add the last file footer entry
    if (isLastFileFooterEntryNeedToBeAdded) {
      summaryRow =
          loadToUnsafeBlock(schema, taskSummarySchema, previousDataFileFooter, segmentProperties,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2649
              getMinMaxCacheColumns(),
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3684
              previousDataFileFooter.getBlockInfo().getFilePath(), summaryRow,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765
              blockletIndexModel.getBlockMetaInfoMap()
                  .get(previousDataFileFooter.getBlockInfo().getFilePath()),
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2942
              blockMinValues, blockMaxValues, minMaxFlag);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3293
      totalRowCount += previousDataFileFooter.getNumberOfRows();
      blockletCountInEachBlock.add(totalBlockletsInOneBlock);
    }
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2788
    byte[] blockletCount = convertRowCountFromShortToByteArray(blockletCountInEachBlock);
    // set the total row count
    summaryRow.setLong(totalRowCount, TASK_ROW_COUNT);
    // blocklet count index is the last index
    summaryRow.setByteArray(blockletCount, taskSummarySchema.length - 1);
    setMinMaxFlagForTaskSummary(summaryRow, taskSummarySchema, segmentProperties,
        taskSummaryMinMaxFlag);
    return summaryRow;
  }

  protected void updateMinMaxFlag(DataFileFooter fileFooter, boolean[] minMaxFlag) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3704
    BlockletIndexUtil
        .updateMinMaxFlag(fileFooter.getBlockletIndex().getMinMaxIndex(), minMaxFlag);
  }

  private byte[] convertRowCountFromShortToByteArray(List<Short> blockletCountInEachBlock) {
    int bufferSize = blockletCountInEachBlock.size() * 2;
    ByteBuffer byteBuffer = ByteBuffer.allocate(bufferSize);
    for (Short blockletCount : blockletCountInEachBlock) {
      byteBuffer.putShort(blockletCount);
    }
    byteBuffer.rewind();
    return byteBuffer.array();
  }

  protected void setLocations(String[] locations, IndexRow row, int ordinal)
      throws UnsupportedEncodingException {
    // Add location info
    String locationStr = StringUtils.join(locations, ',');
    row.setByteArray(locationStr.getBytes(CarbonCommonConstants.DEFAULT_CHARSET), ordinal);
  }

  /**
   * Load information for the block.It is the case can happen only for old stores
   * where blocklet information is not available in index file. So load only block information
   * and read blocklet information in executor.
   */
  protected IndexRowImpl loadToUnsafeBlock(CarbonRowSchema[] schema,
      CarbonRowSchema[] taskSummarySchema, DataFileFooter fileFooter,
      SegmentProperties segmentProperties, List<CarbonColumn> minMaxCacheColumns, String filePath,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3704
      IndexRowImpl summaryRow, BlockMetaInfo blockMetaInfo, byte[][] minValues,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2942
      byte[][] maxValues, boolean[] minMaxFlag) {
    // Add one row to maintain task level min max for segment pruning
    if (summaryRow == null) {
      summaryRow = new IndexRowImpl(taskSummarySchema);
    }
    IndexRow row = new IndexRowImpl(schema);
    int ordinal = 0;
    int taskMinMaxOrdinal = 1;
    // get min max values for columns to be cached
    byte[][] minValuesForColumnsToBeCached = BlockletIndexUtil
        .getMinMaxForColumnsToBeCached(segmentProperties, minMaxCacheColumns, minValues);
    byte[][] maxValuesForColumnsToBeCached = BlockletIndexUtil
        .getMinMaxForColumnsToBeCached(segmentProperties, minMaxCacheColumns, maxValues);
    boolean[] minMaxFlagValuesForColumnsToBeCached = BlockletIndexUtil
        .getMinMaxFlagValuesForColumnsToBeCached(segmentProperties, minMaxCacheColumns, minMaxFlag);
    IndexRow indexRow = addMinMax(schema[ordinal], minValuesForColumnsToBeCached);
    row.setRow(indexRow, ordinal);
    // compute and set task level min values
    addTaskMinMaxValues(summaryRow, taskSummarySchema, taskMinMaxOrdinal,
        minValuesForColumnsToBeCached, TASK_MIN_VALUES_INDEX, true);
    ordinal++;
    taskMinMaxOrdinal++;
    row.setRow(addMinMax(schema[ordinal], maxValuesForColumnsToBeCached), ordinal);
    // compute and set task level max values
    addTaskMinMaxValues(summaryRow, taskSummarySchema, taskMinMaxOrdinal,
        maxValuesForColumnsToBeCached, TASK_MAX_VALUES_INDEX, false);
    ordinal++;
    // add total rows in one carbondata file
    row.setInt((int) fileFooter.getNumberOfRows(), ordinal++);
    // add file name
    byte[] filePathBytes =
        CarbonTablePath.getCarbonDataFileName(filePath)
            .getBytes(CarbonCommonConstants.DEFAULT_CHARSET_CLASS);
    row.setByteArray(filePathBytes, ordinal++);
    // add version number
    row.setShort(fileFooter.getVersionId().number(), ordinal++);
    // add schema updated time
    row.setLong(fileFooter.getSchemaUpdatedTimeStamp(), ordinal++);
    // add block offset
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3684
    row.setLong(fileFooter.getBlockInfo().getBlockOffset(), ordinal++);
    try {
      setLocations(blockMetaInfo.getLocationInfo(), row, ordinal++);
      // store block size
      row.setLong(blockMetaInfo.getSize(), ordinal++);
      // add min max flag for all the dimension columns
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3036
      addMinMaxFlagValues(row, schema[ordinal], minMaxFlagValuesForColumnsToBeCached, ordinal);
      memoryDMStore.addIndexRow(schema, row);
    } catch (Exception e) {
      String message = "Load to unsafe failed for block: " + filePath;
      LOGGER.error(message, e);
      throw new RuntimeException(message, e);
    }
    return summaryRow;
  }

  protected void addMinMaxFlagValues(IndexRow row, CarbonRowSchema carbonRowSchema,
      boolean[] minMaxFlag, int ordinal) {
    CarbonRowSchema[] minMaxFlagSchema =
        ((CarbonRowSchema.StructCarbonRowSchema) carbonRowSchema).getChildSchemas();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3704
    IndexRow minMaxFlagRow = new IndexRowImpl(minMaxFlagSchema);
    int flagOrdinal = 0;
    // min value adding
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3036
    for (int i = 0; i < minMaxFlag.length; i++) {
      minMaxFlagRow.setBoolean(minMaxFlag[i], flagOrdinal++);
    }
    row.setRow(minMaxFlagRow, ordinal);
  }

  protected String getFilePath() {
    if (isFilePathStored) {
      return getTableTaskInfo(SUMMARY_INDEX_PATH);
    }
    // create the segment directory path
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3482
    String tablePath = segmentPropertiesWrapper.getTableIdentifier().getTablePath();
    String segmentId = getTableTaskInfo(SUMMARY_SEGMENTID);
    return CarbonTablePath.getSegmentPath(tablePath, segmentId);
  }

  protected String getFileNameWithFilePath(IndexRow indexRow, String filePath) {
    String fileName = filePath + CarbonCommonConstants.FILE_SEPARATOR + new String(
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3704
        indexRow.getByteArray(FILE_PATH_INDEX), CarbonCommonConstants.DEFAULT_CHARSET_CLASS)
        + CarbonTablePath.getCarbonDataExtension();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3217
    return FileFactory.getUpdatedFilePath(fileName);
  }

  private void addTaskSummaryRowToUnsafeMemoryStore(CarbonRowSchema[] taskSummarySchema,
      IndexRow summaryRow, byte[] filePath, byte[] fileName, byte[] segmentId) {
    // write the task summary info to unsafe memory store
    if (null != summaryRow) {
      summaryRow.setByteArray(fileName, SUMMARY_INDEX_FILE_NAME);
      summaryRow.setByteArray(segmentId, SUMMARY_SEGMENTID);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3557
      if (filePath.length > 0) {
        summaryRow.setByteArray(filePath, SUMMARY_INDEX_PATH);
      }
      try {
        taskSummaryDMStore.addIndexRow(taskSummarySchema, summaryRow);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  protected IndexRow addMinMax(CarbonRowSchema carbonRowSchema,
      byte[][] minValues) {
    CarbonRowSchema[] minSchemas =
        ((CarbonRowSchema.StructCarbonRowSchema) carbonRowSchema).getChildSchemas();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3704
    IndexRow minRow = new IndexRowImpl(minSchemas);
    int minOrdinal = 0;
    // min value adding
    for (int i = 0; i < minValues.length; i++) {
      minRow.setByteArray(minValues[i], minOrdinal++);
    }
    return minRow;
  }

  /**
   * This method will compute min/max values at task level
   *
   * @param taskMinMaxRow
   * @param carbonRowSchema
   * @param taskMinMaxOrdinal
   * @param minMaxValue
   * @param ordinal
   * @param isMinValueComparison
   */
  protected void addTaskMinMaxValues(IndexRow taskMinMaxRow, CarbonRowSchema[] carbonRowSchema,
      int taskMinMaxOrdinal, byte[][] minMaxValue, int ordinal, boolean isMinValueComparison) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3704
    IndexRow row = taskMinMaxRow.getRow(ordinal);
    byte[][] updatedMinMaxValues = null;
    if (null == row) {
      CarbonRowSchema[] minSchemas =
          ((CarbonRowSchema.StructCarbonRowSchema) carbonRowSchema[taskMinMaxOrdinal])
              .getChildSchemas();
      row = new IndexRowImpl(minSchemas);
      updatedMinMaxValues = minMaxValue;
    } else {
      byte[][] existingMinMaxValues = getMinMaxValue(taskMinMaxRow, ordinal);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2649
      updatedMinMaxValues =
          compareAndUpdateMinMax(minMaxValue, existingMinMaxValues, isMinValueComparison);
    }
    int minMaxOrdinal = 0;
    // min/max value adding
    for (int i = 0; i < updatedMinMaxValues.length; i++) {
      row.setByteArray(updatedMinMaxValues[i], minMaxOrdinal++);
    }
    taskMinMaxRow.setRow(row, ordinal);
  }

  /**
   * This method will do min/max comparison of values and update if required
   *
   * @param minMaxValueCompare1
   * @param minMaxValueCompare2
   * @param isMinValueComparison
   */
  private byte[][] compareAndUpdateMinMax(byte[][] minMaxValueCompare1,
      byte[][] minMaxValueCompare2, boolean isMinValueComparison) {
    // Compare and update min max values
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2649
    byte[][] updatedMinMaxValues = new byte[minMaxValueCompare1.length][];
    System.arraycopy(minMaxValueCompare1, 0, updatedMinMaxValues, 0, minMaxValueCompare1.length);
    for (int i = 0; i < minMaxValueCompare1.length; i++) {
      int compare = ByteUtil.UnsafeComparer.INSTANCE
          .compareTo(minMaxValueCompare2[i], minMaxValueCompare1[i]);
      if (isMinValueComparison) {
        if (compare < 0) {
          updatedMinMaxValues[i] = minMaxValueCompare2[i];
        }
      } else if (compare > 0) {
        updatedMinMaxValues[i] = minMaxValueCompare2[i];
      }
    }
    return updatedMinMaxValues;
  }

  protected void createMemorySchema(BlockletIndexModel blockletIndexModel) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765
    memoryDMStore = getMemoryDMStore(blockletIndexModel.isAddToUnsafe());
  }

  /**
   * Creates the schema to store summary information or the information which can be stored only
   * once per index. It stores index level max/min of each column and partition information of
   * index
   *
   */
  protected void createSummaryDMStore(BlockletIndexModel blockletIndexModel) {
    taskSummaryDMStore = getMemoryDMStore(blockletIndexModel.isAddToUnsafe());
  }

  @Override
  public boolean isScanRequired(FilterResolverIntf filterExp) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3611
    FilterExecuter filterExecuter = FilterUtil.getFilterExecuterTree(
        filterExp, getSegmentProperties(), null, getMinMaxCacheColumns(), false);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3704
    IndexRow unsafeRow = taskSummaryDMStore
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765
        .getIndexRow(getTaskSummarySchema(), taskSummaryDMStore.getRowCount() - 1);
    boolean isScanRequired = FilterExpressionProcessor
        .isScanRequired(filterExecuter, getMinMaxValue(unsafeRow, TASK_MAX_VALUES_INDEX),
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2942
            getMinMaxValue(unsafeRow, TASK_MIN_VALUES_INDEX),
            getMinMaxFlag(unsafeRow, TASK_MIN_MAX_FLAG));
    if (isScanRequired) {
      return true;
    }
    return false;
  }

  protected List<CarbonColumn> getMinMaxCacheColumns() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3482
    return segmentPropertiesWrapper.getMinMaxCacheColumns();
  }

  /**
   * for CACHE_LEVEL=BLOCK, each entry in memoryDMStore is for a block
   * if data is not legacy store, we can get blocklet count from taskSummaryDMStore
   */
  protected short getBlockletNumOfEntry(int index) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3557
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3684
    final byte[] bytes = getBlockletRowCountForEachBlock();
    // if the segment data is written in tablepath
    // then the reuslt of getBlockletRowCountForEachBlock will be empty.
    if (bytes.length == 0) {
      return 0;
    } else {
      return ByteBuffer.wrap(bytes).getShort(index * CarbonCommonConstants.SHORT_SIZE_IN_BYTE);
    }
  }

  // get total block number in this index
  public int getTotalBlocks() {
    return memoryDMStore.getRowCount();
  }

  // get total blocklet number in this index
  protected int getTotalBlocklets() {
    ByteBuffer byteBuffer = ByteBuffer.wrap(getBlockletRowCountForEachBlock());
    int sum = 0;
    while (byteBuffer.hasRemaining()) {
      sum += byteBuffer.getShort();
    }
    return sum;
  }

  @Override
  public long getRowCount(Segment segment, List<PartitionSpec> partitions) {
    long totalRowCount =
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765
        taskSummaryDMStore.getIndexRow(getTaskSummarySchema(), 0).getLong(TASK_ROW_COUNT);
    if (totalRowCount == 0) {
      Map<String, Long> blockletToRowCountMap = new HashMap<>();
      // if it has partitioned index but there is no partitioned information stored, it means
      // partitions are dropped so return empty list.
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3773
      if (partitions != null) {
        if (validatePartitionInfo(partitions)) {
          return totalRowCount;
        }
      }
      getRowCountForEachBlock(segment, partitions, blockletToRowCountMap);
      for (long blockletRowCount : blockletToRowCountMap.values()) {
        totalRowCount += blockletRowCount;
      }
    } else {
      if (taskSummaryDMStore.getRowCount() == 0) {
        return 0L;
      }
    }
    return totalRowCount;
  }

  public Map<String, Long> getRowCountForEachBlock(Segment segment, List<PartitionSpec> partitions,
      Map<String, Long> blockletToRowCountMap) {
    if (memoryDMStore.getRowCount() == 0) {
      return new HashMap<>();
    }
    CarbonRowSchema[] schema = getFileFooterEntrySchema();
    int numEntries = memoryDMStore.getRowCount();
    for (int i = 0; i < numEntries; i++) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765
      IndexRow indexRow = memoryDMStore.getIndexRow(schema, i);
      String fileName = new String(indexRow.getByteArray(FILE_PATH_INDEX),
          CarbonCommonConstants.DEFAULT_CHARSET_CLASS) + CarbonTablePath.getCarbonDataExtension();
      int rowCount = indexRow.getInt(ROW_COUNT_INDEX);
      // prepend segment number with the blocklet file path
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3391
      String blockletMapKey = segment.getSegmentNo() + "," + fileName;
      Long existingCount = blockletToRowCountMap.get(blockletMapKey);
      if (null != existingCount) {
        blockletToRowCountMap.put(blockletMapKey, (long) rowCount + existingCount);
      } else {
        blockletToRowCountMap.put(blockletMapKey, (long) rowCount);
      }
    }
    return blockletToRowCountMap;
  }

  private List<Blocklet> prune(FilterResolverIntf filterExp, FilterExecuter filterExecuter,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3700
      SegmentProperties segmentProperties) {
    if (memoryDMStore.getRowCount() == 0) {
      return new ArrayList<>();
    }
    List<Blocklet> blocklets = new ArrayList<>();
    CarbonRowSchema[] schema = getFileFooterEntrySchema();
    String filePath = getFilePath();
    int numEntries = memoryDMStore.getRowCount();
    int totalBlocklets = 0;
    if (ExplainCollector.enabled()) {
      totalBlocklets = getTotalBlocklets();
    }
    int hitBlocklets = 0;
    if (filterExp == null) {
      for (int i = 0; i < numEntries; i++) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765
        IndexRow indexRow = memoryDMStore.getIndexRow(schema, i);
        blocklets.add(createBlocklet(indexRow, getFileNameWithFilePath(indexRow, filePath),
            getBlockletId(indexRow), false));
      }
      hitBlocklets = totalBlocklets;
    } else {
      // Remove B-tree jump logic as start and end key prepared is not
      // correct for old store scenarios
      int entryIndex = 0;
      // flag to be used for deciding whether use min/max in executor pruning for BlockletIndex
      boolean useMinMaxForPruning = useMinMaxForExecutorPruning(filterExp);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3700
      if (!validateSegmentProperties(segmentProperties)) {
        filterExecuter = FilterUtil
                .getFilterExecuterTree(filterExp, getSegmentProperties(),
                        null, getMinMaxCacheColumns(), false);
      }
      // min and max for executor pruning
      while (entryIndex < numEntries) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765
        IndexRow row = memoryDMStore.getIndexRow(schema, entryIndex);
        boolean[] minMaxFlag = getMinMaxFlag(row, BLOCK_MIN_MAX_FLAG);
        String fileName = getFileNameWithFilePath(row, filePath);
        short blockletId = getBlockletId(row);
        boolean isValid =
            addBlockBasedOnMinMaxValue(filterExecuter, getMinMaxValue(row, MAX_VALUES_INDEX),
                getMinMaxValue(row, MIN_VALUES_INDEX), minMaxFlag, fileName, blockletId);
        if (isValid) {
          blocklets.add(createBlocklet(row, fileName, blockletId, useMinMaxForPruning));
          if (ExplainCollector.enabled()) {
            hitBlocklets += getBlockletNumOfEntry(entryIndex);
          }
        }
        entryIndex++;
      }
    }
    if (ExplainCollector.enabled()) {
      ExplainCollector.setShowPruningInfo(true);
      ExplainCollector.addTotalBlocklets(totalBlocklets);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2929
      ExplainCollector.addTotalBlocks(getTotalBlocks());
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765
      ExplainCollector.addDefaultIndexPruningHit(hitBlocklets);
    }
    return blocklets;
  }

  protected boolean useMinMaxForExecutorPruning(FilterResolverIntf filterResolverIntf) {
    return false;
  }

  @Override
  public List<Blocklet> prune(Expression expression, SegmentProperties properties,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3781
      CarbonTable carbonTable, FilterExecuter filterExecuter) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3704
    return prune(new IndexFilter(properties, carbonTable, expression).getResolver(), properties,
        filterExecuter, carbonTable);
  }

  @Override
  public List<Blocklet> prune(FilterResolverIntf filterExp, SegmentProperties segmentProperties,
      FilterExecuter filterExecuter, CarbonTable table) {
    if (memoryDMStore.getRowCount() == 0) {
      return new ArrayList<>();
    }
    // Prune with filters if the partitions are existed in this index
    // changed segmentProperties to this.segmentProperties to make sure the pruning with its own
    // segmentProperties.
    // Its a temporary fix. The Interface Index.prune(FilterResolverIntf filterExp,
    // SegmentProperties segmentProperties, List<PartitionSpec> partitions) should be corrected
    return prune(filterExp, filterExecuter, segmentProperties);
  }

  public boolean validatePartitionInfo(List<PartitionSpec> partitions) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3773
    if (memoryDMStore.getRowCount() == 0) {
      return true;
    }
    // First get the partitions which are stored inside index.
    String[] fileDetails = getFileDetails();
    // Check the exact match of partition information inside the stored partitions.
    boolean found = false;
    Path folderPath = new Path(fileDetails[0]);
    for (PartitionSpec spec : partitions) {
      if (folderPath.equals(spec.getLocation()) && isCorrectUUID(fileDetails, spec)) {
        found = true;
        break;
      }
    }
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3773
    return !found;
  }

  @Override
  public void finish() {

  }

  private boolean isCorrectUUID(String[] fileDetails, PartitionSpec spec) {
    boolean needToScan = false;
    if (spec.getUuid() != null) {
      String[] split = spec.getUuid().split("_");
      if (split[0].equals(fileDetails[2]) && CarbonTablePath.DataFileUtil
          .getTimeStampFromFileName(fileDetails[1]).equals(split[1])) {
        needToScan = true;
      }
    } else {
      needToScan = true;
    }
    return needToScan;
  }

  /**
   * select the blocks based on column min and max value
   *
   * @param filterExecuter
   * @param maxValue
   * @param minValue
   * @param minMaxFlag
   * @param filePath
   * @param blockletId
   * @return
   */
  private boolean addBlockBasedOnMinMaxValue(FilterExecuter filterExecuter, byte[][] maxValue,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2942
      byte[][] minValue, boolean[] minMaxFlag, String filePath, int blockletId) {
    BitSet bitSet = null;
    if (filterExecuter instanceof ImplicitColumnFilterExecutor) {
      String uniqueBlockPath;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3801
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3805
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3809
      CarbonTable carbonTable = segmentPropertiesWrapper.getCarbonTable();
      if (carbonTable.isHivePartitionTable()) {
        // While data loading to SI created on Partition table, on partition directory, '/' will be
        // replaced with '#', to support multi level partitioning. For example, BlockId will be
        // look like `part1=1#part2=2/xxxxxxxxx`. During query also, blockId should be
        // replaced by '#' in place of '/', to match and prune data on SI table.
        uniqueBlockPath = CarbonUtil
            .getBlockId(carbonTable.getAbsoluteTableIdentifier(), filePath, "", true, false, true);
      } else {
        uniqueBlockPath = filePath.substring(filePath.lastIndexOf("/Part") + 1);
      }
      // this case will come in case of old store where index file does not contain the
      // blocklet information
      if (blockletId != -1) {
        uniqueBlockPath = uniqueBlockPath + CarbonCommonConstants.FILE_SEPARATOR + blockletId;
      }
      bitSet = ((ImplicitColumnFilterExecutor) filterExecuter)
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2942
          .isFilterValuesPresentInBlockOrBlocklet(maxValue, minValue, uniqueBlockPath, minMaxFlag);
    } else {
      bitSet = filterExecuter.isScanRequired(maxValue, minValue, minMaxFlag);
    }
    if (!bitSet.isEmpty()) {
      return true;
    } else {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2942
      return false;
    }
  }

  public ExtendedBlocklet getDetailedBlocklet(String blockletId) {
    int absoluteBlockletId = Integer.parseInt(blockletId);
    return createBlockletFromRelativeBlockletId(absoluteBlockletId);
  }

  /**
   * Method to get the relative blocklet ID. Absolute blocklet ID is the blocklet Id as per
   * task level but relative blocklet ID is id as per carbondata file/block level
   *
   * @param absoluteBlockletId
   * @return
   */
  private ExtendedBlocklet createBlockletFromRelativeBlockletId(int absoluteBlockletId) {
    short relativeBlockletId = -1;
    int rowIndex = 0;
    // return 0 if absoluteBlockletId is 0
    if (absoluteBlockletId == 0) {
      relativeBlockletId = (short) absoluteBlockletId;
    } else {
      int diff = absoluteBlockletId;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2788
      ByteBuffer byteBuffer = ByteBuffer.wrap(getBlockletRowCountForEachBlock());
      // Example: absoluteBlockletID = 17, blockletRowCountForEachBlock = {4,3,2,5,7}
      // step1: diff = 17-4, diff = 13
      // step2: diff = 13-3, diff = 10
      // step3: diff = 10-2, diff = 8
      // step4: diff = 8-5, diff = 3
      // step5: diff = 3-7, diff = -4 (satisfies <= 0)
      // step6: relativeBlockletId = -4+7, relativeBlockletId = 3 (4th index starting from 0)
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2788
      while (byteBuffer.hasRemaining()) {
        short blockletCount = byteBuffer.getShort();
        diff = diff - blockletCount;
        if (diff < 0) {
          relativeBlockletId = (short) (diff + blockletCount);
          break;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3592
        } else if (diff == 0) {
          relativeBlockletId++;
          break;
        }
        rowIndex++;
      }
    }
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3704
    IndexRow row =
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765
        memoryDMStore.getIndexRow(getFileFooterEntrySchema(), rowIndex);
    String filePath = getFilePath();
    return createBlocklet(row, getFileNameWithFilePath(row, filePath), relativeBlockletId,
        false);
  }

  private byte[] getBlockletRowCountForEachBlock() {
    // taskSummary DM store will  have only one row
    CarbonRowSchema[] taskSummarySchema = getTaskSummarySchema();
    return taskSummaryDMStore
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765
        .getIndexRow(taskSummarySchema, taskSummaryDMStore.getRowCount() - 1)
        .getByteArray(taskSummarySchema.length - 1);
  }

  /**
   * Get the index file name of the blocklet index
   *
   * @return
   */
  public String getTableTaskInfo(int index) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765
    IndexRow unsafeRow = taskSummaryDMStore.getIndexRow(getTaskSummarySchema(), 0);
    try {
      return new String(unsafeRow.getByteArray(index), CarbonCommonConstants.DEFAULT_CHARSET);
    } catch (UnsupportedEncodingException e) {
      // should never happen!
      throw new IllegalArgumentException("UTF8 encoding is not supported", e);
    }
  }

  private byte[][] getMinMaxValue(IndexRow row, int index) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3704
    IndexRow minMaxRow = row.getRow(index);
    byte[][] minMax = new byte[minMaxRow.getColumnCount()][];
    for (int i = 0; i < minMax.length; i++) {
      minMax[i] = minMaxRow.getByteArray(i);
    }
    return minMax;
  }

  private boolean[] getMinMaxFlag(IndexRow row, int index) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3704
    IndexRow minMaxFlagRow = row.getRow(index);
    boolean[] minMaxFlag = new boolean[minMaxFlagRow.getColumnCount()];
    for (int i = 0; i < minMaxFlag.length; i++) {
      minMaxFlag[i] = minMaxFlagRow.getBoolean(i);
    }
    return minMaxFlag;
  }

  protected short getBlockletId(IndexRow indexRow) {
    return BLOCK_DEFAULT_BLOCKLET_ID;
  }

  protected ExtendedBlocklet createBlocklet(IndexRow row, String fileName, short blockletId,
      boolean useMinMaxForPruning) {
    short versionNumber = row.getShort(VERSION_INDEX);
    ExtendedBlocklet blocklet = new ExtendedBlocklet(fileName, blockletId + "", false,
        ColumnarFormatVersion.valueOf(versionNumber));
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765
    blocklet.setIndexRow(row);
    blocklet.setUseMinMaxForPruning(useMinMaxForPruning);
    return blocklet;
  }

  private String[] getFileDetails() {
    try {
      String[] fileDetails = new String[3];
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765
      IndexRow unsafeRow = taskSummaryDMStore.getIndexRow(getTaskSummarySchema(), 0);
      fileDetails[0] = new String(unsafeRow.getByteArray(SUMMARY_INDEX_PATH),
          CarbonCommonConstants.DEFAULT_CHARSET);
      fileDetails[1] = new String(unsafeRow.getByteArray(SUMMARY_INDEX_FILE_NAME),
          CarbonCommonConstants.DEFAULT_CHARSET);
      fileDetails[2] = new String(unsafeRow.getByteArray(SUMMARY_SEGMENTID),
          CarbonCommonConstants.DEFAULT_CHARSET);
      return fileDetails;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void clear() {
    if (memoryDMStore != null) {
      memoryDMStore.freeMemory();
    }
    // clear task min/max unsafe memory
    if (null != taskSummaryDMStore) {
      taskSummaryDMStore.freeMemory();
    }
  }

  public long getMemorySize() {
    long memoryUsed = 0L;
    if (memoryDMStore != null) {
      memoryUsed += memoryDMStore.getMemoryUsed();
    }
    if (null != taskSummaryDMStore) {
      memoryUsed += taskSummaryDMStore.getMemoryUsed();
    }
    return memoryUsed;
  }

  protected boolean validateSegmentProperties(SegmentProperties tableSegmentProperties) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3700
    return tableSegmentProperties.equals(getSegmentProperties());
  }

  protected SegmentProperties getSegmentProperties() {
    return segmentPropertiesWrapper.getSegmentProperties();
  }

  public List<ColumnSchema> getColumnSchema() {
    return segmentPropertiesWrapper.getColumnsInTable();
  }

  protected AbstractMemoryDMStore getMemoryDMStore(boolean addToUnsafe) {
    AbstractMemoryDMStore memoryDMStore;
    if (addToUnsafe) {
      memoryDMStore = new UnsafeMemoryDMStore();
    } else {
      memoryDMStore = new SafeMemoryDMStore();
    }
    return memoryDMStore;
  }

  protected CarbonRowSchema[] getFileFooterEntrySchema() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3482
    return segmentPropertiesWrapper.getBlockFileFooterEntrySchema();
  }

  protected CarbonRowSchema[] getTaskSummarySchema() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3062
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3575
    return segmentPropertiesWrapper.getTaskSummarySchemaForBlock(true, isFilePathStored);
  }

  /**
   * This method will ocnvert safe to unsafe memory DM store
   *
   */
  public void convertToUnsafeDMStore() {
    if (memoryDMStore instanceof SafeMemoryDMStore) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2649
      UnsafeMemoryDMStore unsafeMemoryDMStore = memoryDMStore.convertToUnsafeDMStore(
          getFileFooterEntrySchema());
      memoryDMStore.freeMemory();
      memoryDMStore = unsafeMemoryDMStore;
    }
    if (taskSummaryDMStore instanceof SafeMemoryDMStore) {
      UnsafeMemoryDMStore unsafeSummaryMemoryDMStore =
          taskSummaryDMStore.convertToUnsafeDMStore(getTaskSummarySchema());
      taskSummaryDMStore.freeMemory();
      taskSummaryDMStore = unsafeSummaryMemoryDMStore;
    }
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3680
    if (memoryDMStore instanceof UnsafeMemoryDMStore) {
      if (memoryDMStore.isSerialized()) {
        memoryDMStore.copyToMemoryBlock();
      }
    }
    if (taskSummaryDMStore instanceof UnsafeMemoryDMStore) {
      if (taskSummaryDMStore.isSerialized()) {
        taskSummaryDMStore.copyToMemoryBlock();
      }
    }
  }

  public void setSegmentPropertiesWrapper(
      SegmentPropertiesAndSchemaHolder.SegmentPropertiesWrapper segmentPropertiesWrapper) {
    this.segmentPropertiesWrapper = segmentPropertiesWrapper;
  }

  public SegmentPropertiesAndSchemaHolder.SegmentPropertiesWrapper getSegmentPropertiesWrapper() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3482
    return segmentPropertiesWrapper;
  }

  @Override
  public int getNumberOfEntries() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3118
    if (memoryDMStore != null) {
      if (memoryDMStore.getRowCount() == 0) {
        // so that one index considered as one record
        return 1;
      } else {
        return memoryDMStore.getRowCount();
      }
    } else {
      // legacy store
      return 1;
    }
  }
}
