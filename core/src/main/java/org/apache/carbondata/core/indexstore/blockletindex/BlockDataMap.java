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
import java.util.List;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.dev.DataMapModel;
import org.apache.carbondata.core.datamap.dev.cgdatamap.CoarseGrainDataMap;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.block.SegmentPropertiesAndSchemaHolder;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.indexstore.AbstractMemoryDMStore;
import org.apache.carbondata.core.indexstore.BlockMetaInfo;
import org.apache.carbondata.core.indexstore.Blocklet;
import org.apache.carbondata.core.indexstore.BlockletDetailInfo;
import org.apache.carbondata.core.indexstore.ExtendedBlocklet;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.indexstore.SafeMemoryDMStore;
import org.apache.carbondata.core.indexstore.UnsafeMemoryDMStore;
import org.apache.carbondata.core.indexstore.row.DataMapRow;
import org.apache.carbondata.core.indexstore.row.DataMapRowImpl;
import org.apache.carbondata.core.indexstore.schema.CarbonRowSchema;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.metadata.blocklet.index.BlockletIndex;
import org.apache.carbondata.core.metadata.blocklet.index.BlockletMinMaxIndex;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.profiler.ExplainCollector;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.filter.FilterExpressionProcessor;
import org.apache.carbondata.core.scan.filter.FilterUtil;
import org.apache.carbondata.core.scan.filter.executer.FilterExecuter;
import org.apache.carbondata.core.scan.filter.executer.ImplicitColumnFilterExecutor;
import org.apache.carbondata.core.scan.filter.intf.FilterOptimizer;
import org.apache.carbondata.core.scan.filter.optimizer.RangeFilterOptmizer;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.util.BlockletDataMapUtil;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataFileFooterConverter;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * Datamap implementation for block.
 */
public class BlockDataMap extends CoarseGrainDataMap
    implements BlockletDataMapRowIndexes, Serializable {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(BlockDataMap.class.getName());

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
  protected int segmentPropertiesIndex;
  /**
   * flag to check for store from 1.1 or any prior version
   */
  protected boolean isLegacyStore;
  /**
   * flag to be used for forming the complete file path from file name. It will be true in case of
   * partition table and non transactional table
   */
  protected boolean isFilePathStored;

  @Override public void init(DataMapModel dataMapModel)
      throws IOException, MemoryException {
    long startTime = System.currentTimeMillis();
    assert (dataMapModel instanceof BlockletDataMapModel);
    BlockletDataMapModel blockletDataMapInfo = (BlockletDataMapModel) dataMapModel;
    DataFileFooterConverter fileFooterConverter =
        new DataFileFooterConverter(dataMapModel.getConfiguration());
    List<DataFileFooter> indexInfo = fileFooterConverter
        .getIndexInfo(blockletDataMapInfo.getFilePath(), blockletDataMapInfo.getFileData(),
            blockletDataMapInfo.getCarbonTable().isTransactionalTable());
    Path path = new Path(blockletDataMapInfo.getFilePath());
    // store file path only in case of partition table, non transactional table and flat folder
    // structure
    byte[] filePath = null;
    boolean isPartitionTable = blockletDataMapInfo.getCarbonTable().isHivePartitionTable();
    if (isPartitionTable || !blockletDataMapInfo.getCarbonTable().isTransactionalTable()
        || blockletDataMapInfo.getCarbonTable().isSupportFlatFolder()) {
      filePath = path.getParent().toString().getBytes(CarbonCommonConstants.DEFAULT_CHARSET);
      isFilePathStored = true;
    }
    byte[] fileName = path.getName().getBytes(CarbonCommonConstants.DEFAULT_CHARSET);
    byte[] segmentId =
        blockletDataMapInfo.getSegmentId().getBytes(CarbonCommonConstants.DEFAULT_CHARSET);
    if (!indexInfo.isEmpty()) {
      DataFileFooter fileFooter = indexInfo.get(0);
      // store for 1.1 or any prior version will not have any blocklet information in file footer
      isLegacyStore = fileFooter.getBlockletList() == null;
      // init segment properties and create schema
      SegmentProperties segmentProperties = initSegmentProperties(blockletDataMapInfo, fileFooter);
      createMemorySchema(blockletDataMapInfo);
      createSummaryDMStore(blockletDataMapInfo);
      CarbonRowSchema[] taskSummarySchema = getTaskSummarySchema();
      // check for legacy store and load the metadata
      DataMapRowImpl summaryRow =
          loadMetadata(taskSummarySchema, segmentProperties, blockletDataMapInfo, indexInfo);
      finishWriting(taskSummarySchema, filePath, fileName, segmentId, summaryRow);
    }
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "Time taken to load blocklet datamap from file : " + dataMapModel.getFilePath() + " is "
              + (System.currentTimeMillis() - startTime));
    }
  }

  private void finishWriting(CarbonRowSchema[] taskSummarySchema, byte[] filePath, byte[] fileName,
      byte[] segmentId, DataMapRowImpl summaryRow) throws MemoryException {
    if (memoryDMStore != null) {
      memoryDMStore.finishWriting();
    }
    if (null != taskSummaryDMStore) {
      addTaskSummaryRowToUnsafeMemoryStore(taskSummarySchema, summaryRow, filePath, fileName,
          segmentId);
      taskSummaryDMStore.finishWriting();
    }
  }

  /**
   * Method to check the cache level and load metadata based on that information
   *
   * @param segmentProperties
   * @param blockletDataMapInfo
   * @param indexInfo
   * @throws IOException
   * @throws MemoryException
   */
  protected DataMapRowImpl loadMetadata(CarbonRowSchema[] taskSummarySchema,
      SegmentProperties segmentProperties, BlockletDataMapModel blockletDataMapInfo,
      List<DataFileFooter> indexInfo) throws IOException, MemoryException {
    if (isLegacyStore) {
      return loadBlockInfoForOldStore(taskSummarySchema, segmentProperties, blockletDataMapInfo,
          indexInfo);
    } else {
      return loadBlockMetaInfo(taskSummarySchema, segmentProperties, blockletDataMapInfo,
          indexInfo);
    }
  }

  /**
   * initialise segment properties
   *
   * @param fileFooter
   * @throws IOException
   */
  private SegmentProperties initSegmentProperties(BlockletDataMapModel blockletDataMapInfo,
      DataFileFooter fileFooter) throws IOException {
    List<ColumnSchema> columnInTable = fileFooter.getColumnInTable();
    int[] columnCardinality = fileFooter.getSegmentInfo().getColumnCardinality();
    segmentPropertiesIndex = SegmentPropertiesAndSchemaHolder.getInstance()
        .addSegmentProperties(blockletDataMapInfo.getCarbonTable(),
            columnInTable, columnCardinality, blockletDataMapInfo.getSegmentId());
    return getSegmentProperties();
  }

  /**
   * This is old store scenario, here blocklet information is not available in index
   * file so load only block info. Old store refers to store in 1.1 or prior to 1.1 version
   *
   * @param blockletDataMapInfo
   * @param indexInfo
   * @throws IOException
   * @throws MemoryException
   */
  protected DataMapRowImpl loadBlockInfoForOldStore(CarbonRowSchema[] taskSummarySchema,
      SegmentProperties segmentProperties, BlockletDataMapModel blockletDataMapInfo,
      List<DataFileFooter> indexInfo) throws IOException, MemoryException {
    DataMapRowImpl summaryRow = null;
    CarbonRowSchema[] schema = getFileFooterEntrySchema();
    boolean[] minMaxFlag = new boolean[segmentProperties.getColumnsValueSize().length];
    Arrays.fill(minMaxFlag, true);
    for (DataFileFooter fileFooter : indexInfo) {
      TableBlockInfo blockInfo = fileFooter.getBlockInfo().getTableBlockInfo();
      BlockMetaInfo blockMetaInfo =
          blockletDataMapInfo.getBlockMetaInfoMap().get(blockInfo.getFilePath());
      // Here it loads info about all blocklets of index
      // Only add if the file exists physically. There are scenarios which index file exists inside
      // merge index but related carbondata files are deleted. In that case we first check whether
      // the file exists physically or not
      if (null != blockMetaInfo) {
        BlockletIndex blockletIndex = fileFooter.getBlockletIndex();
        BlockletMinMaxIndex minMaxIndex = blockletIndex.getMinMaxIndex();
        byte[][] minValues =
            BlockletDataMapUtil.updateMinValues(segmentProperties, minMaxIndex.getMinValues());
        byte[][] maxValues =
            BlockletDataMapUtil.updateMaxValues(segmentProperties, minMaxIndex.getMaxValues());
        // update min max values in case of old store for measures as measure min/max in
        // old stores in written opposite
        byte[][] updatedMinValues =
            CarbonUtil.updateMinMaxValues(fileFooter, maxValues, minValues, true);
        byte[][] updatedMaxValues =
            CarbonUtil.updateMinMaxValues(fileFooter, maxValues, minValues, false);
        summaryRow = loadToUnsafeBlock(schema, taskSummarySchema, fileFooter, segmentProperties,
            getMinMaxCacheColumns(), blockInfo.getFilePath(), summaryRow,
            blockMetaInfo, updatedMinValues, updatedMaxValues, minMaxFlag);
      }
    }
    List<Short> blockletCountList = new ArrayList<>();
    blockletCountList.add((short) 0);
    byte[] blockletCount = convertRowCountFromShortToByteArray(blockletCountList);
    summaryRow.setByteArray(blockletCount, taskSummarySchema.length - 1);
    return summaryRow;
  }

  /**
   * Method to load block metadata information
   *
   * @param blockletDataMapInfo
   * @param indexInfo
   * @throws IOException
   * @throws MemoryException
   */
  private DataMapRowImpl loadBlockMetaInfo(CarbonRowSchema[] taskSummarySchema,
      SegmentProperties segmentProperties, BlockletDataMapModel blockletDataMapInfo,
      List<DataFileFooter> indexInfo) throws IOException, MemoryException {
    String tempFilePath = null;
    DataFileFooter previousDataFileFooter = null;
    int footerCounter = 0;
    byte[][] blockMinValues = null;
    byte[][] blockMaxValues = null;
    DataMapRowImpl summaryRow = null;
    List<Short> blockletCountInEachBlock = new ArrayList<>(indexInfo.size());
    short totalBlockletsInOneBlock = 0;
    boolean isLastFileFooterEntryNeedToBeAdded = false;
    CarbonRowSchema[] schema = getFileFooterEntrySchema();
    // flag for each block entry
    boolean[] minMaxFlag = new boolean[segmentProperties.getColumnsValueSize().length];
    Arrays.fill(minMaxFlag, true);
    // min max flag for task summary
    boolean[] taskSummaryMinMaxFlag = new boolean[segmentProperties.getColumnsValueSize().length];
    Arrays.fill(taskSummaryMinMaxFlag, true);
    for (DataFileFooter fileFooter : indexInfo) {
      TableBlockInfo blockInfo = fileFooter.getBlockInfo().getTableBlockInfo();
      BlockMetaInfo blockMetaInfo =
          blockletDataMapInfo.getBlockMetaInfoMap().get(blockInfo.getFilePath());
      footerCounter++;
      if (blockMetaInfo != null) {
        // this variable will be used for adding the DataMapRow entry every time a unique block
        // path is encountered
        if (null == tempFilePath) {
          tempFilePath = blockInfo.getFilePath();
          // 1st time assign the min and max values from the current file footer
          blockMinValues = fileFooter.getBlockletIndex().getMinMaxIndex().getMinValues();
          blockMaxValues = fileFooter.getBlockletIndex().getMinMaxIndex().getMaxValues();
          updateMinMaxFlag(fileFooter, minMaxFlag);
          updateMinMaxFlag(fileFooter, taskSummaryMinMaxFlag);
          previousDataFileFooter = fileFooter;
          totalBlockletsInOneBlock++;
        } else if (blockInfo.getFilePath().equals(tempFilePath)) {
          // After iterating over all the blocklets that belong to one block we need to compute the
          // min and max at block level. So compare min and max values and update if required
          BlockletMinMaxIndex currentFooterMinMaxIndex =
              fileFooter.getBlockletIndex().getMinMaxIndex();
          blockMinValues =
              compareAndUpdateMinMax(currentFooterMinMaxIndex.getMinValues(), blockMinValues, true);
          blockMaxValues =
              compareAndUpdateMinMax(currentFooterMinMaxIndex.getMaxValues(), blockMaxValues,
                  false);
          updateMinMaxFlag(fileFooter, minMaxFlag);
          updateMinMaxFlag(fileFooter, taskSummaryMinMaxFlag);
          totalBlockletsInOneBlock++;
        }
        // as one task contains entries for all the blocklets we need iterate and load only the
        // with unique file path because each unique path will correspond to one
        // block in the task. OR condition is to handle the loading of last file footer
        if (!blockInfo.getFilePath().equals(tempFilePath) || footerCounter == indexInfo.size()) {
          TableBlockInfo previousBlockInfo =
              previousDataFileFooter.getBlockInfo().getTableBlockInfo();
          summaryRow = loadToUnsafeBlock(schema, taskSummarySchema, previousDataFileFooter,
              segmentProperties, getMinMaxCacheColumns(), previousBlockInfo.getFilePath(),
              summaryRow,
              blockletDataMapInfo.getBlockMetaInfoMap().get(previousBlockInfo.getFilePath()),
              blockMinValues, blockMaxValues, minMaxFlag);
          minMaxFlag = new boolean[segmentProperties.getColumnsValueSize().length];
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
              getMinMaxCacheColumns(),
              previousDataFileFooter.getBlockInfo().getTableBlockInfo().getFilePath(), summaryRow,
              blockletDataMapInfo.getBlockMetaInfoMap()
                  .get(previousDataFileFooter.getBlockInfo().getTableBlockInfo().getFilePath()),
              blockMinValues, blockMaxValues, minMaxFlag);
      blockletCountInEachBlock.add(totalBlockletsInOneBlock);
    }
    byte[] blockletCount = convertRowCountFromShortToByteArray(blockletCountInEachBlock);
    // blocklet count index is the last index
    summaryRow.setByteArray(blockletCount, taskSummarySchema.length - 1);
    return summaryRow;
  }

  protected void updateMinMaxFlag(DataFileFooter fileFooter, boolean[] minMaxFlag) {
    BlockletDataMapUtil
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

  protected void setLocations(String[] locations, DataMapRow row, int ordinal)
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
  protected DataMapRowImpl loadToUnsafeBlock(CarbonRowSchema[] schema,
      CarbonRowSchema[] taskSummarySchema, DataFileFooter fileFooter,
      SegmentProperties segmentProperties, List<CarbonColumn> minMaxCacheColumns, String filePath,
      DataMapRowImpl summaryRow, BlockMetaInfo blockMetaInfo, byte[][] minValues,
      byte[][] maxValues, boolean[] minMaxFlag) {
    // Add one row to maintain task level min max for segment pruning
    if (summaryRow == null) {
      summaryRow = new DataMapRowImpl(taskSummarySchema);
    }
    DataMapRow row = new DataMapRowImpl(schema);
    int ordinal = 0;
    int taskMinMaxOrdinal = 0;
    // add total rows in one carbondata file
    row.setInt((int) fileFooter.getNumberOfRows(), ordinal++);
    // get min max values for columns to be cached
    byte[][] minValuesForColumnsToBeCached = BlockletDataMapUtil
        .getMinMaxForColumnsToBeCached(segmentProperties, minMaxCacheColumns, minValues);
    byte[][] maxValuesForColumnsToBeCached = BlockletDataMapUtil
        .getMinMaxForColumnsToBeCached(segmentProperties, minMaxCacheColumns, maxValues);
    byte[] minMaxFlagValuesForColumnsToBeCached = BlockletDataMapUtil
        .getMinMaxFlagValuesForColumnsToBeCached(segmentProperties, minMaxCacheColumns, minMaxFlag);
    row.setRow(
        addMinMax(schema[ordinal], minValuesForColumnsToBeCached, maxValuesForColumnsToBeCached,
            minMaxFlagValuesForColumnsToBeCached), ordinal);
    ordinal++;
    // compute and set task level min values
    addTaskMinMaxValues(summaryRow, taskSummarySchema, taskMinMaxOrdinal,
        minValuesForColumnsToBeCached, maxValuesForColumnsToBeCached,
        minMaxFlagValuesForColumnsToBeCached, TASK_MIN_VALUES_INDEX);
    // add file name
    byte[] filePathBytes =
        getFileNameFromPath(filePath).getBytes(CarbonCommonConstants.DEFAULT_CHARSET_CLASS);
    row.setByteArray(filePathBytes, ordinal++);
    try {
      setLocations(blockMetaInfo.getLocationInfo(), row, ordinal++);
      // add version number
      row.setShort(fileFooter.getVersionId().number(), ordinal++);
      // add schema updated time
      row.setLong(fileFooter.getSchemaUpdatedTimeStamp(), ordinal++);
      // add block offset
      row.setLong(fileFooter.getBlockInfo().getTableBlockInfo().getBlockOffset(), ordinal++);

      // store block size
      row.setLong(blockMetaInfo.getSize(), ordinal);

      memoryDMStore.addIndexRow(schema, row);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return summaryRow;
  }


  protected String getFileNameFromPath(String filePath) {
    return CarbonTablePath.getCarbonDataFileName(filePath);
  }

  protected String getFilePath() {
    if (isFilePathStored) {
      return getTableTaskInfo(SUMMARY_INDEX_PATH);
    }
    // create the segment directory path
    String tablePath = SegmentPropertiesAndSchemaHolder.getInstance()
        .getSegmentPropertiesWrapper(segmentPropertiesIndex).getTableIdentifier().getTablePath();
    String segmentId = getTableTaskInfo(SUMMARY_SEGMENTID);
    return CarbonTablePath.getSegmentPath(tablePath, segmentId);
  }

  protected String getFileNameWithFilePath(DataMapRow dataMapRow, String filePath, int position) {
    String fileName = new StringBuffer(filePath).append(CarbonCommonConstants.FILE_SEPARATOR)
        .append(new String(dataMapRow.getByteArray(FILE_PATH_INDEX, position),
            CarbonCommonConstants.DEFAULT_CHARSET_CLASS))
        .append(CarbonTablePath.getCarbonDataExtension()).toString();
    return FileFactory.getUpdatedFilePath(fileName);
  }

  private void addTaskSummaryRowToUnsafeMemoryStore(CarbonRowSchema[] taskSummarySchema,
      DataMapRow summaryRow, byte[] filePath, byte[] fileName, byte[] segmentId) {
    // write the task summary info to unsafe memory store
    if (null != summaryRow) {
      summaryRow.setByteArray(fileName, SUMMARY_INDEX_FILE_NAME);
      summaryRow.setByteArray(segmentId, SUMMARY_SEGMENTID);
      if (null != filePath) {
        summaryRow.setByteArray(filePath, SUMMARY_INDEX_PATH);
      }
      try {
        taskSummaryDMStore.addIndexRow(taskSummarySchema, summaryRow);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  protected DataMapRow addMinMax(CarbonRowSchema carbonRowSchema,
      byte[][] minValues, byte[][] maxValues, byte[] minMaxFlag) {
    CarbonRowSchema[] minSchemas =
        ((CarbonRowSchema.StructCarbonRowSchema) carbonRowSchema).getChildSchemas();
    DataMapRow row = new DataMapRowImpl(minSchemas);
    if (minValues.length != 0) {
      int ordinal = 0;
      // min value adding
      for (int i = 0; i < minValues.length; i++) {
        row.setByteArray(minValues[i], ordinal++);
      }
      // max value adding
      for (int i = 0; i < maxValues.length; i++) {
        row.setByteArray(maxValues[i], ordinal++);
      }
      row.setByteArray(minMaxFlag, ordinal);
    }
    return row;
  }

  /**
   * This method will compute min/max values at task level
   *
   * @param taskMinMaxRow
   * @param carbonRowSchema
   * @param taskMinMaxOrdinal
   * @param ordinal
   */
  protected void addTaskMinMaxValues(DataMapRow taskMinMaxRow, CarbonRowSchema[] carbonRowSchema,
      int taskMinMaxOrdinal, byte[][] minValue, byte[][] maxValue, byte[] flags, int ordinal) {
    DataMapRow row = taskMinMaxRow.getRow(ordinal);
    byte[][] updatedMinValues = null;
    byte[][] updatedMaxValues = null;
    if (null == row) {
      CarbonRowSchema[] minSchemas =
          ((CarbonRowSchema.StructCarbonRowSchema) carbonRowSchema[taskMinMaxOrdinal])
              .getChildSchemas();
      row = new DataMapRowImpl(minSchemas);
      updatedMinValues = minValue;
      updatedMaxValues = maxValue;
    } else {
      DataMapRowImpl minMaxRow = getRow(taskMinMaxRow, ordinal, -1);
      DataMapMinMaxDetails details = getMinMaxRow(minMaxRow);
      updatedMinValues =
          compareAndUpdateMinMax(minValue, details.minVals, true);
      updatedMaxValues =
          compareAndUpdateMinMax(maxValue, details.maxVals, false);
    }
    if (minValue.length != 0) {
      int minMaxOrdinal = 0;
      // min/max value adding
      for (int i = 0; i < updatedMinValues.length; i++) {
        row.setByteArray(updatedMinValues[i], minMaxOrdinal++);
      }
      for (int i = 0; i < updatedMaxValues.length; i++) {
        row.setByteArray(updatedMaxValues[i], minMaxOrdinal++);
      }
      row.setByteArray(flags, minMaxOrdinal);
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

  protected void createMemorySchema(BlockletDataMapModel blockletDataMapModel)
      throws MemoryException {
    memoryDMStore = getMemoryDMStore(blockletDataMapModel.isAddToUnsafe());
  }

  /**
   * Creates the schema to store summary information or the information which can be stored only
   * once per datamap. It stores datamap level max/min of each column and partition information of
   * datamap
   *
   * @throws MemoryException
   */
  protected void createSummaryDMStore(BlockletDataMapModel blockletDataMapModel)
      throws MemoryException {
    taskSummaryDMStore = getMemoryDMStore(blockletDataMapModel.isAddToUnsafe());
  }

  @Override public boolean isScanRequired(FilterResolverIntf filterExp) {
    FilterExecuter filterExecuter = FilterUtil
        .getFilterExecuterTree(filterExp, getSegmentProperties(), null, getMinMaxCacheColumns());
    DataMapRow unsafeRow = taskSummaryDMStore
        .getDataMapRow(getTaskSummarySchema(), taskSummaryDMStore.getRowCount() - 1);
    DataMapRowImpl minRow = getRow(unsafeRow, TASK_MIN_VALUES_INDEX, -1);
    DataMapMinMaxDetails minMaxRow = getMinMaxRow(minRow);
    boolean isScanRequired = FilterExpressionProcessor
        .isScanRequired(filterExecuter, minMaxRow.maxVals, minMaxRow.minVals, minMaxRow.flags);
    if (isScanRequired) {
      return true;
    }
    return false;
  }

  protected List<CarbonColumn> getMinMaxCacheColumns() {
    return SegmentPropertiesAndSchemaHolder.getInstance()
        .getSegmentPropertiesWrapper(segmentPropertiesIndex).getMinMaxCacheColumns();
  }

  /**
   * for CACHE_LEVEL=BLOCK, each entry in memoryDMStore is for a block
   * if data is not legacy store, we can get blocklet count from taskSummaryDMStore
   */
  protected short getBlockletNumOfEntry(int index) {
    if (isLegacyStore) {
      // dummy value
      return 0;
    } else {
      return ByteBuffer.wrap(getBlockletRowCountForEachBlock()).getShort(
          index * CarbonCommonConstants.SHORT_SIZE_IN_BYTE);
    }
  }

  // get total block number in this datamap
  public int getTotalBlocks() {
    if (isLegacyStore) {
      // dummy value
      return 0;
    } else {
      return memoryDMStore.getRowCount();
    }
  }

  // get total blocklet number in this datamap
  protected int getTotalBlocklets() {
    ByteBuffer byteBuffer = ByteBuffer.wrap(getBlockletRowCountForEachBlock());
    int sum = 0;
    while (byteBuffer.hasRemaining()) {
      sum += byteBuffer.getShort();
    }
    return sum;
  }

  private List<Blocklet> prune(FilterResolverIntf filterExp) {
    if (memoryDMStore.getRowCount() == 0) {
      return new ArrayList<>();
    }
    List<Blocklet> blocklets = new ArrayList<>();
    CarbonRowSchema[] schema = getFileFooterEntrySchema();
    String filePath = getFilePath();
    int numEntries = memoryDMStore.getRowCount();
    int totalBlocklets = getTotalBlocklets();
    int hitBlocklets = 0;
    if (filterExp == null) {
      for (int i = 0; i < numEntries; i++) {
        DataMapRow row = memoryDMStore.getDataMapRow(schema, i);
        DataMapRow safeRow = row.convertToSafeRow(FILE_PATH_INDEX, 0);
        safeRow.setInt(row.getInt(ROW_COUNT_INDEX), ROW_COUNT_INDEX);
        blocklets.add(createBlocklet(safeRow, getFileNameWithFilePath(safeRow, filePath, -1),
            getBlockletId(safeRow), false));
      }
      hitBlocklets = totalBlocklets;
    } else {
      // Remove B-tree jump logic as start and end key prepared is not
      // correct for old store scenarios
      int entryIndex = 0;
      FilterExecuter filterExecuter = FilterUtil
          .getFilterExecuterTree(filterExp, getSegmentProperties(), null, getMinMaxCacheColumns());
      // flag to be used for deciding whether use min/max in executor pruning for BlockletDataMap
      boolean useMinMaxForPruning = useMinMaxForExecutorPruning(filterExp);
      // min and max for executor pruning
      while (entryIndex < numEntries) {
        DataMapRow unSafeRow = memoryDMStore.getDataMapRow(schema, entryIndex);
        short blockletId = getBlockletId(unSafeRow);
        DataMapRowImpl minValueRow = getRow(unSafeRow, MIN_VALUES_INDEX, -1);
        DataMapMinMaxDetails minMaxRow = getMinMaxRow(minValueRow);
        String fileName =
            getFileNameWithFilePath(unSafeRow, filePath, minValueRow.getTotalSizeInBytes());
        boolean isValid =
            addBlockBasedOnMinMaxValue(filterExecuter, minMaxRow.maxVals, minMaxRow.minVals,
                minMaxRow.flags, fileName, blockletId);
        if (isValid) {
          DataMapRow row = unSafeRow.convertToSafeRow(LOCATIONS, unSafeRow.getCurrentPointer());
          row.setInt(unSafeRow.getInt(ROW_COUNT_INDEX), ROW_COUNT_INDEX);
          blocklets.add(createBlocklet(row, fileName, blockletId,
              useMinMaxForPruning));
          hitBlocklets += getBlockletNumOfEntry(entryIndex);
        }
        entryIndex++;
      }
    }

    if (isLegacyStore) {
      ExplainCollector.setShowPruningInfo(false);
    } else {
      ExplainCollector.setShowPruningInfo(true);
      ExplainCollector.addTotalBlocklets(totalBlocklets);
      ExplainCollector.addTotalBlocks(getTotalBlocks());
      ExplainCollector.addDefaultDataMapPruningHit(hitBlocklets);
    }
    return blocklets;
  }

  protected boolean useMinMaxForExecutorPruning(FilterResolverIntf filterResolverIntf) {
    return false;
  }

  @Override
  public List<Blocklet> prune(Expression expression, SegmentProperties properties,
      List<PartitionSpec> partitions, CarbonTable carbonTable) throws IOException {
    FilterResolverIntf filterResolverIntf = null;
    if (expression != null) {
      QueryModel.FilterProcessVO processVO =
          new QueryModel.FilterProcessVO(properties.getDimensions(), properties.getMeasures(),
              new ArrayList<CarbonDimension>());
      QueryModel.processFilterExpression(processVO, expression, null, null, carbonTable);
      // Optimize Filter Expression and fit RANGE filters is conditions apply.
      FilterOptimizer rangeFilterOptimizer = new RangeFilterOptmizer(expression);
      rangeFilterOptimizer.optimizeFilter();
      filterResolverIntf =
          CarbonTable.resolveFilter(expression, carbonTable.getAbsoluteTableIdentifier());
    }
    return prune(filterResolverIntf, properties, partitions);
  }

  @Override
  public List<Blocklet> prune(FilterResolverIntf filterExp, SegmentProperties segmentProperties,
      List<PartitionSpec> partitions) {
    if (memoryDMStore.getRowCount() == 0) {
      return new ArrayList<>();
    }
    // if it has partitioned datamap but there is no partitioned information stored, it means
    // partitions are dropped so return empty list.
    if (partitions != null) {
      if (!validatePartitionInfo(partitions)) {
        return new ArrayList<>();
      }
    }
    // Prune with filters if the partitions are existed in this datamap
    // changed segmentProperties to this.segmentProperties to make sure the pruning with its own
    // segmentProperties.
    // Its a temporary fix. The Interface DataMap.prune(FilterResolverIntf filterExp,
    // SegmentProperties segmentProperties, List<PartitionSpec> partitions) should be corrected
    return prune(filterExp);
  }

  private boolean validatePartitionInfo(List<PartitionSpec> partitions) {
    // First get the partitions which are stored inside datamap.
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
    return found;
  }

  @Override public void finish() {

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
      byte[][] minValue, boolean[] minMaxFlag, String filePath, int blockletId) {
    BitSet bitSet = null;
    if (filterExecuter instanceof ImplicitColumnFilterExecutor) {
      String uniqueBlockPath = filePath.substring(filePath.lastIndexOf("/Part") + 1);
      // this case will come in case of old store where index file does not contain the
      // blocklet information
      if (blockletId != -1) {
        uniqueBlockPath = uniqueBlockPath + CarbonCommonConstants.FILE_SEPARATOR + blockletId;
      }
      bitSet = ((ImplicitColumnFilterExecutor) filterExecuter)
          .isFilterValuesPresentInBlockOrBlocklet(maxValue, minValue, uniqueBlockPath, minMaxFlag);
    } else {
      bitSet = filterExecuter.isScanRequired(maxValue, minValue, minMaxFlag);
    }
    if (!bitSet.isEmpty()) {
      return true;
    } else {
      return false;
    }
  }

  public ExtendedBlocklet getDetailedBlocklet(String blockletId) {
    if (isLegacyStore) {
      throw new UnsupportedOperationException("With legacy store only BlockletDataMap is allowed."
          + " In order to use other dataMaps upgrade to new store.");
    }
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
      ByteBuffer byteBuffer = ByteBuffer.wrap(getBlockletRowCountForEachBlock());
      // Example: absoluteBlockletID = 17, blockletRowCountForEachBlock = {4,3,2,5,7}
      // step1: diff = 17-4, diff = 13
      // step2: diff = 13-3, diff = 10
      // step3: diff = 10-2, diff = 8
      // step4: diff = 8-5, diff = 3
      // step5: diff = 3-7, diff = -4 (satisfies <= 0)
      // step6: relativeBlockletId = -4+7, relativeBlockletId = 3 (4th index starting from 0)
      while (byteBuffer.hasRemaining()) {
        short blockletCount = byteBuffer.getShort();
        diff = diff - blockletCount;
        if (diff < 0) {
          relativeBlockletId = (short) (diff + blockletCount);
          break;
        }
        rowIndex++;
      }
    }
    DataMapRow unsafeRow = memoryDMStore.getDataMapRow(getFileFooterEntrySchema(), rowIndex);
    DataMapRow safeRow = unsafeRow.convertToSafeRow(FILE_PATH_INDEX, 0);
    safeRow.setInt(unsafeRow.getInt(ROW_COUNT_INDEX), ROW_COUNT_INDEX);
    String filePath = getFilePath();
    return createBlocklet(safeRow, getFileNameWithFilePath(safeRow, filePath, -1),
        relativeBlockletId,
        false);
  }

  private byte[] getBlockletRowCountForEachBlock() {
    // taskSummary DM store will  have only one row
    CarbonRowSchema[] taskSummarySchema = getTaskSummarySchema();
    return taskSummaryDMStore
        .getDataMapRow(taskSummarySchema, taskSummaryDMStore.getRowCount() - 1)
        .getByteArray(taskSummarySchema.length - 1);
  }

  /**
   * Get the index file name of the blocklet data map
   *
   * @return
   */
  public String getTableTaskInfo(int index) {
    DataMapRow unsafeRow = taskSummaryDMStore.getDataMapRow(getTaskSummarySchema(), 0);
    try {
      return new String(unsafeRow.getByteArray(index), CarbonCommonConstants.DEFAULT_CHARSET);
    } catch (UnsupportedEncodingException e) {
      // should never happen!
      throw new IllegalArgumentException("UTF8 encoding is not supported", e);
    }
  }

  private DataMapRowImpl getRow(DataMapRow row, int index, int position) {
    DataMapRow minMaxRow = row.getRow(index, position);
    DataMapRowImpl safeRow = (DataMapRowImpl)minMaxRow.convertToSafeRow(0, 0);
    safeRow.setTotalLengthInBytes(minMaxRow.getPosition() + safeRow.getTotalSizeInBytes());
    return safeRow;
  }

  private DataMapMinMaxDetails getMinMaxRow(DataMapRowImpl safeRow) {
    int size = (safeRow.getColumnCount() - 1) / 2;
    byte[][] min = new byte[size][];
    byte[][] max = new byte[size][];
    boolean[] flags = new boolean[size];
    if (size > 0) {
      int l = 0;
      for (int i = 0; i < size; i++) {
        min[i] = safeRow.getByteArray(l++);
      }
      for (int i = 0; i < size; i++) {
        max[i] = safeRow.getByteArray(l++);
      }
      byte[] byteArray = safeRow.getByteArray(l);

      for (int i = 0; i < byteArray.length; i++) {
        flags[i] = byteArray[i] == 1;
      }
    }
    return new DataMapMinMaxDetails(min, max, flags);
  }

  protected short getBlockletId(DataMapRow dataMapRow) {
    return BLOCK_DEFAULT_BLOCKLET_ID;
  }

  protected ExtendedBlocklet createBlocklet(DataMapRow row, String fileName, short blockletId,
      boolean useMinMaxForPruning) {
    ExtendedBlocklet blocklet = new ExtendedBlocklet(fileName, blockletId + "", false);
    BlockletDetailInfo detailInfo = getBlockletDetailInfo(row, blockletId, blocklet);
    detailInfo.setBlockletInfoBinary(new byte[0]);
    blocklet.setDetailInfo(detailInfo);
    return blocklet;
  }

  protected BlockletDetailInfo getBlockletDetailInfo(DataMapRow row, short blockletId,
      ExtendedBlocklet blocklet) {
    BlockletDetailInfo detailInfo = new BlockletDetailInfo();
    detailInfo.setRowCount(row.getInt(ROW_COUNT_INDEX));
    detailInfo.setVersionNumber(row.getShort(VERSION_INDEX));
    detailInfo.setBlockletId(blockletId);
    detailInfo.setDimLens(getColumnCardinality());
    detailInfo.setSchemaUpdatedTimeStamp(row.getLong(SCHEMA_UPADATED_TIME_INDEX));
    try {
      blocklet.setLocation(
          new String(row.getByteArray(LOCATIONS), CarbonCommonConstants.DEFAULT_CHARSET)
              .split(","));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    detailInfo.setBlockFooterOffset(row.getLong(BLOCK_FOOTER_OFFSET));
    detailInfo.setBlockSize(row.getLong(BLOCK_LENGTH));
    detailInfo.setLegacyStore(isLegacyStore);
    return detailInfo;
  }

  private String[] getFileDetails() {
    try {
      String[] fileDetails = new String[3];
      DataMapRow unsafeRow = taskSummaryDMStore.getDataMapRow(getTaskSummarySchema(), 0);
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

  @Override public void clear() {
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

  protected SegmentProperties getSegmentProperties() {
    return SegmentPropertiesAndSchemaHolder.getInstance()
        .getSegmentProperties(segmentPropertiesIndex);
  }

  public int[] getColumnCardinality() {
    return SegmentPropertiesAndSchemaHolder.getInstance()
        .getSegmentPropertiesWrapper(segmentPropertiesIndex).getColumnCardinality();
  }

  public List<ColumnSchema> getColumnSchema() {
    return SegmentPropertiesAndSchemaHolder.getInstance()
        .getSegmentPropertiesWrapper(segmentPropertiesIndex).getColumnsInTable();
  }

  protected AbstractMemoryDMStore getMemoryDMStore(boolean addToUnsafe)
      throws MemoryException {
    AbstractMemoryDMStore memoryDMStore;
    if (addToUnsafe) {
      memoryDMStore = new UnsafeMemoryDMStore();
    } else {
      memoryDMStore = new SafeMemoryDMStore();
    }
    return memoryDMStore;
  }

  protected CarbonRowSchema[] getFileFooterEntrySchema() {
    return SegmentPropertiesAndSchemaHolder.getInstance()
        .getSegmentPropertiesWrapper(segmentPropertiesIndex).getBlockFileFooterEntrySchema();
  }

  protected CarbonRowSchema[] getTaskSummarySchema() {
    SegmentPropertiesAndSchemaHolder.SegmentPropertiesWrapper segmentPropertiesWrapper =
        SegmentPropertiesAndSchemaHolder.getInstance()
            .getSegmentPropertiesWrapper(segmentPropertiesIndex);
    try {
      return segmentPropertiesWrapper.getTaskSummarySchemaForBlock(true, isFilePathStored);
    } catch (MemoryException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * This method will ocnvert safe to unsafe memory DM store
   *
   * @throws MemoryException
   */
  public void convertToUnsafeDMStore() throws MemoryException {
    if (memoryDMStore instanceof SafeMemoryDMStore) {
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
  }

  public void setSegmentPropertiesIndex(int segmentPropertiesIndex) {
    this.segmentPropertiesIndex = segmentPropertiesIndex;
  }

  public int getSegmentPropertiesIndex() {
    return segmentPropertiesIndex;
  }

  @Override public int getNumberOfEntries() {
    if (memoryDMStore != null) {
      if (memoryDMStore.getRowCount() == 0) {
        // so that one datamap considered as one record
        return 1;
      } else {
        return memoryDMStore.getRowCount();
      }
    } else {
      // legacy store
      return 1;
    }
  }

  static class DataMapMinMaxDetails {

    byte[][] minVals;
    byte[][] maxVals;
    boolean[] flags;

    public DataMapMinMaxDetails(byte[][] minVals, byte[][] maxVals, boolean[] flags) {
      this.minVals = minVals;
      this.maxVals = maxVals;
      this.flags = flags;
    }
  }
}
