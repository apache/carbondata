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

import java.io.*;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.dev.DataMapModel;
import org.apache.carbondata.core.datamap.dev.cgdatamap.CoarseGrainDataMap;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
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
import org.apache.carbondata.core.indexstore.schema.SchemaGenerator;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.metadata.blocklet.index.BlockletIndex;
import org.apache.carbondata.core.metadata.blocklet.index.BlockletMinMaxIndex;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.profiler.ExplainCollector;
import org.apache.carbondata.core.scan.filter.FilterExpressionProcessor;
import org.apache.carbondata.core.scan.filter.FilterUtil;
import org.apache.carbondata.core.scan.filter.executer.FilterExecuter;
import org.apache.carbondata.core.scan.filter.executer.ImplicitColumnFilterExecutor;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.util.BlockletDataMapUtil;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataFileFooterConverter;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.xerial.snappy.Snappy;

/**
 * Datamap implementation for block.
 */
public class BlockDataMap extends CoarseGrainDataMap
    implements BlockletDataMapRowIndexes, Serializable {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(BlockDataMap.class.getName());

  protected static final long serialVersionUID = -2170289352240810993L;
  /**
   * for CACHE_LEVEL=BLOCK and legacy store default blocklet id will be -1
   */
  private static final short BLOCK_DEFAULT_BLOCKLET_ID = -1;

  protected AbstractMemoryDMStore memoryDMStore;

  protected AbstractMemoryDMStore taskSummaryDMStore;

  // As it is a heavy object it is not recommended to serialize this object
  protected transient SegmentProperties segmentProperties;

  protected int[] columnCardinality;

  protected long blockletSchemaTime;
  /**
   * flag to check for store from 1.1 or any prior version
   */
  protected boolean isLegacyStore;

  @Override public void init(DataMapModel dataMapModel) throws IOException, MemoryException {
    long startTime = System.currentTimeMillis();
    assert (dataMapModel instanceof BlockletDataMapModel);
    BlockletDataMapModel blockletDataMapInfo = (BlockletDataMapModel) dataMapModel;
    DataFileFooterConverter fileFooterConverter = new DataFileFooterConverter();
    List<DataFileFooter> indexInfo = fileFooterConverter
        .getIndexInfo(blockletDataMapInfo.getFilePath(), blockletDataMapInfo.getFileData());
    Path path = new Path(blockletDataMapInfo.getFilePath());
    byte[] filePath = path.getParent().toString().getBytes(CarbonCommonConstants.DEFAULT_CHARSET);
    byte[] fileName = path.getName().toString().getBytes(CarbonCommonConstants.DEFAULT_CHARSET);
    byte[] segmentId =
        blockletDataMapInfo.getSegmentId().getBytes(CarbonCommonConstants.DEFAULT_CHARSET);
    byte[] schemaBinary = null;
    if (!indexInfo.isEmpty()) {
      DataFileFooter fileFooter = indexInfo.get(0);
      // store for 1.1 or any prior version will not have any blocklet information in file footer
      isLegacyStore = fileFooter.getBlockletList() == null;
      // init segment properties and create schema
      initSegmentProperties(fileFooter);
      schemaBinary = convertSchemaToBinary(fileFooter.getColumnInTable());
      createSchema(segmentProperties, blockletDataMapInfo.isAddToUnsafe());
      createSummarySchema(segmentProperties, schemaBinary, filePath, fileName, segmentId,
          blockletDataMapInfo.isAddToUnsafe());
    }
    // check for legacy store and load the metadata
    DataMapRowImpl summaryRow = loadMetadata(blockletDataMapInfo, indexInfo);
    finishWriting(filePath, fileName, segmentId, schemaBinary, summaryRow);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "Time taken to load blocklet datamap from file : " + dataMapModel.getFilePath() + " is "
              + (System.currentTimeMillis() - startTime));
    }
  }

  private void finishWriting(byte[] filePath, byte[] fileName, byte[] segmentId,
      byte[] schemaBinary, DataMapRowImpl summaryRow) throws MemoryException {
    if (memoryDMStore != null) {
      memoryDMStore.finishWriting();
    }
    // TODO: schema binary not required. Instead maintain only the segmentProperties index and
    // get the cardinality and columnSchema from there
    if (null != taskSummaryDMStore) {
      addTaskSummaryRowToUnsafeMemoryStore(summaryRow, schemaBinary, filePath, fileName, segmentId);
      taskSummaryDMStore.finishWriting();
    }
  }

  /**
   * Method to check the cache level and load metadata based on that information
   *
   * @param blockletDataMapInfo
   * @param indexInfo
   * @throws IOException
   * @throws MemoryException
   */
  protected DataMapRowImpl loadMetadata(BlockletDataMapModel blockletDataMapInfo,
      List<DataFileFooter> indexInfo) throws IOException, MemoryException {
    if (isLegacyStore) {
      return loadBlockInfoForOldStore(blockletDataMapInfo, indexInfo);
    } else {
      return loadBlockMetaInfo(blockletDataMapInfo, indexInfo);
    }
  }

  /**
   * initialise segment properties
   *
   * @param fileFooter
   * @throws IOException
   */
  private void initSegmentProperties(DataFileFooter fileFooter) throws IOException {
    List<ColumnSchema> columnInTable = fileFooter.getColumnInTable();
    // TODO: remove blockletSchemaTime after maintaining the index of segmentProperties
    blockletSchemaTime = fileFooter.getSchemaUpdatedTimeStamp();
    columnCardinality = fileFooter.getSegmentInfo().getColumnCardinality();
    segmentProperties = new SegmentProperties(columnInTable, columnCardinality);
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
  protected DataMapRowImpl loadBlockInfoForOldStore(BlockletDataMapModel blockletDataMapInfo,
      List<DataFileFooter> indexInfo) throws IOException, MemoryException {
    DataMapRowImpl summaryRow = null;
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
        summaryRow =
            loadToUnsafeBlock(fileFooter, segmentProperties, blockInfo.getFilePath(), summaryRow,
                blockMetaInfo, updatedMinValues, updatedMaxValues);
      }
    }
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
  private DataMapRowImpl loadBlockMetaInfo(BlockletDataMapModel blockletDataMapInfo,
      List<DataFileFooter> indexInfo) throws IOException, MemoryException {
    String tempFilePath = null;
    DataFileFooter previousDataFileFooter = null;
    int footerCounter = 0;
    byte[][] blockMinValues = null;
    byte[][] blockMaxValues = null;
    DataMapRowImpl summaryRow = null;
    List<Byte> blockletCountInEachBlock = new ArrayList<>(indexInfo.size());
    byte totalBlockletsInOneBlock = 0;
    boolean isLastFileFooterEntryNeedToBeAdded = false;
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
          previousDataFileFooter = fileFooter;
          totalBlockletsInOneBlock++;
        } else if (blockInfo.getFilePath().equals(tempFilePath)) {
          // After iterating over all the blocklets that belong to one block we need to compute the
          // min and max at block level. So compare min and max values and update if required
          BlockletMinMaxIndex currentFooterMinMaxIndex =
              fileFooter.getBlockletIndex().getMinMaxIndex();
          blockMinValues = compareAndUpdateMinMax(segmentProperties.getColumnsValueSize(),
              currentFooterMinMaxIndex.getMinValues(), blockMinValues, true);
          blockMaxValues = compareAndUpdateMinMax(segmentProperties.getColumnsValueSize(),
              currentFooterMinMaxIndex.getMaxValues(), blockMaxValues, false);
          totalBlockletsInOneBlock++;
        }
        // as one task contains entries for all the blocklets we need iterate and load only the
        // with unique file path because each unique path will correspond to one
        // block in the task. OR condition is to handle the loading of last file footer
        if (!blockInfo.getFilePath().equals(tempFilePath) || footerCounter == indexInfo.size()) {
          TableBlockInfo previousBlockInfo =
              previousDataFileFooter.getBlockInfo().getTableBlockInfo();
          summaryRow = loadToUnsafeBlock(previousDataFileFooter, segmentProperties,
              previousBlockInfo.getFilePath(), summaryRow,
              blockletDataMapInfo.getBlockMetaInfoMap().get(previousBlockInfo.getFilePath()),
              blockMinValues, blockMaxValues);
          // flag to check whether last file footer entry is different from previous entry.
          // If yes then it need to be added at last
          isLastFileFooterEntryNeedToBeAdded =
              (footerCounter == indexInfo.size()) && (!blockInfo.getFilePath()
                  .equals(tempFilePath));
          // assign local variables values using the current file footer
          tempFilePath = blockInfo.getFilePath();
          blockMinValues = fileFooter.getBlockletIndex().getMinMaxIndex().getMinValues();
          blockMaxValues = fileFooter.getBlockletIndex().getMinMaxIndex().getMaxValues();
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
      summaryRow = loadToUnsafeBlock(previousDataFileFooter, segmentProperties,
          previousDataFileFooter.getBlockInfo().getTableBlockInfo().getFilePath(), summaryRow,
          blockletDataMapInfo.getBlockMetaInfoMap()
              .get(previousDataFileFooter.getBlockInfo().getTableBlockInfo().getFilePath()),
          blockMinValues, blockMaxValues);
      blockletCountInEachBlock.add(totalBlockletsInOneBlock);
    }
    byte[] blockletCount = ArrayUtils
        .toPrimitive(blockletCountInEachBlock.toArray(new Byte[blockletCountInEachBlock.size()]));
    summaryRow.setByteArray(blockletCount, SUMMARY_BLOCKLET_COUNT);
    return summaryRow;
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
  protected DataMapRowImpl loadToUnsafeBlock(DataFileFooter fileFooter,
      SegmentProperties segmentProperties, String filePath, DataMapRowImpl summaryRow,
      BlockMetaInfo blockMetaInfo, byte[][] minValues, byte[][] maxValues) {
    int[] minMaxLen = segmentProperties.getColumnsValueSize();
    CarbonRowSchema[] schema = memoryDMStore.getSchema();
    // Add one row to maintain task level min max for segment pruning
    if (summaryRow == null) {
      summaryRow = new DataMapRowImpl(taskSummaryDMStore.getSchema());
    }
    DataMapRow row = new DataMapRowImpl(schema);
    int ordinal = 0;
    int taskMinMaxOrdinal = 0;
    row.setRow(addMinMax(minMaxLen, schema[ordinal], minValues), ordinal);
    // compute and set task level min values
    addTaskMinMaxValues(summaryRow, minMaxLen, taskSummaryDMStore.getSchema(), taskMinMaxOrdinal,
        minValues, TASK_MIN_VALUES_INDEX, true);
    ordinal++;
    taskMinMaxOrdinal++;
    row.setRow(addMinMax(minMaxLen, schema[ordinal], maxValues), ordinal);
    // compute and set task level max values
    addTaskMinMaxValues(summaryRow, minMaxLen, taskSummaryDMStore.getSchema(), taskMinMaxOrdinal,
        maxValues, TASK_MAX_VALUES_INDEX, false);
    ordinal++;
    // add total rows in one carbondata file
    row.setInt((int) fileFooter.getNumberOfRows(), ordinal++);
    // add file path
    // TODO: shorten file path
    byte[] filePathBytes = filePath.getBytes(CarbonCommonConstants.DEFAULT_CHARSET_CLASS);
    row.setByteArray(filePathBytes, ordinal++);
    // add version number
    row.setShort(fileFooter.getVersionId().number(), ordinal++);
    // add schema updated time
    row.setLong(fileFooter.getSchemaUpdatedTimeStamp(), ordinal++);
    // add block offset
    row.setLong(fileFooter.getBlockInfo().getTableBlockInfo().getBlockOffset(), ordinal++);
    try {
      setLocations(blockMetaInfo.getLocationInfo(), row, ordinal++);
      // store block size
      row.setLong(blockMetaInfo.getSize(), ordinal);
      memoryDMStore.addIndexRow(row);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return summaryRow;
  }

  private void addTaskSummaryRowToUnsafeMemoryStore(DataMapRow summaryRow, byte[] schemaBinary,
      byte[] filePath, byte[] fileName, byte[] segmentId) {
    // write the task summary info to unsafe memory store
    if (null != summaryRow) {
      // Add column schema , it is useful to generate segment properties in executor.
      // So we no need to read footer again there.
      if (schemaBinary != null) {
        summaryRow.setByteArray(schemaBinary, SUMMARY_SCHEMA);
      }
      summaryRow.setByteArray(filePath, SUMMARY_INDEX_PATH);
      summaryRow.setByteArray(fileName, SUMMARY_INDEX_FILE_NAME);
      summaryRow.setByteArray(segmentId, SUMMARY_SEGMENTID);
      try {
        taskSummaryDMStore.addIndexRow(summaryRow);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  protected DataMapRow addMinMax(int[] minMaxLen, CarbonRowSchema carbonRowSchema,
      byte[][] minValues) {
    CarbonRowSchema[] minSchemas =
        ((CarbonRowSchema.StructCarbonRowSchema) carbonRowSchema).getChildSchemas();
    DataMapRow minRow = new DataMapRowImpl(minSchemas);
    int minOrdinal = 0;
    // min value adding
    for (int i = 0; i < minMaxLen.length; i++) {
      minRow.setByteArray(minValues[i], minOrdinal++);
    }
    return minRow;
  }

  /**
   * This method will compute min/max values at task level
   *
   * @param taskMinMaxRow
   * @param minMaxLen
   * @param carbonRowSchema
   * @param taskMinMaxOrdinal
   * @param minMaxValue
   * @param ordinal
   * @param isMinValueComparison
   */
  protected void addTaskMinMaxValues(DataMapRow taskMinMaxRow, int[] minMaxLen,
      CarbonRowSchema[] carbonRowSchema, int taskMinMaxOrdinal, byte[][] minMaxValue, int ordinal,
      boolean isMinValueComparison) {
    DataMapRow row = taskMinMaxRow.getRow(ordinal);
    byte[][] updatedMinMaxValues = null;
    if (null == row) {
      CarbonRowSchema[] minSchemas =
          ((CarbonRowSchema.StructCarbonRowSchema) carbonRowSchema[taskMinMaxOrdinal])
              .getChildSchemas();
      row = new DataMapRowImpl(minSchemas);
      updatedMinMaxValues = minMaxValue;
    } else {
      byte[][] existingMinMaxValues = getMinMaxValue(taskMinMaxRow, ordinal);
      updatedMinMaxValues = compareAndUpdateMinMax(minMaxLen, minMaxValue, existingMinMaxValues,
          isMinValueComparison);
    }
    int minMaxOrdinal = 0;
    // min/max value adding
    for (int i = 0; i < minMaxLen.length; i++) {
      row.setByteArray(updatedMinMaxValues[i], minMaxOrdinal++);
    }
    taskMinMaxRow.setRow(row, ordinal);
  }

  /**
   * This method will do min/max comparison of values and update if required
   *
   * @param minMaxLen
   * @param minMaxValueCompare1
   * @param minMaxValueCompare2
   * @param isMinValueComparison
   */
  private byte[][] compareAndUpdateMinMax(int[] minMaxLen, byte[][] minMaxValueCompare1,
      byte[][] minMaxValueCompare2, boolean isMinValueComparison) {
    // Compare and update min max values
    byte[][] updatedMinMaxValues = new byte[minMaxLen.length][];
    System.arraycopy(minMaxValueCompare1, 0, updatedMinMaxValues, 0, minMaxValueCompare1.length);
    for (int i = 0; i < minMaxLen.length; i++) {
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

  protected void createSchema(SegmentProperties segmentProperties, boolean addToUnsafe)
      throws MemoryException {
    CarbonRowSchema[] schema = SchemaGenerator.createBlockSchema(segmentProperties);
    memoryDMStore = getMemoryDMStore(schema, addToUnsafe);
  }

  /**
   * Creates the schema to store summary information or the information which can be stored only
   * once per datamap. It stores datamap level max/min of each column and partition information of
   * datamap
   *
   * @param segmentProperties
   * @throws MemoryException
   */
  protected void createSummarySchema(SegmentProperties segmentProperties, byte[] schemaBinary,
      byte[] filePath, byte[] fileName, byte[] segmentId, boolean addToUnsafe)
      throws MemoryException {
    // flag to check whether it is required to store blocklet count of each carbondata file as
    // binary in summary schema. This will be true when it is not a legacy store (>1.1 version)
    // and CACHE_LEVEL=BLOCK
    boolean storeBlockletCount = !isLegacyStore;
    CarbonRowSchema[] taskSummarySchema = SchemaGenerator
        .createTaskSummarySchema(segmentProperties, schemaBinary, filePath, fileName, segmentId,
            storeBlockletCount);
    taskSummaryDMStore = getMemoryDMStore(taskSummarySchema, addToUnsafe);
  }

  @Override public boolean isScanRequired(FilterResolverIntf filterExp) {
    FilterExecuter filterExecuter =
        FilterUtil.getFilterExecuterTree(filterExp, segmentProperties, null);
    DataMapRow unsafeRow = taskSummaryDMStore.getDataMapRow(taskSummaryDMStore.getRowCount() - 1);
    boolean isScanRequired = FilterExpressionProcessor
        .isScanRequired(filterExecuter, getMinMaxValue(unsafeRow, TASK_MAX_VALUES_INDEX),
            getMinMaxValue(unsafeRow, TASK_MIN_VALUES_INDEX));
    if (isScanRequired) {
      return true;
    }
    return false;
  }

  private List<Blocklet> prune(FilterResolverIntf filterExp, SegmentProperties segmentProperties) {
    if (memoryDMStore.getRowCount() == 0) {
      return new ArrayList<>();
    }
    List<Blocklet> blocklets = new ArrayList<>();
    int numBlocklets = 0;
    if (filterExp == null) {
      numBlocklets = memoryDMStore.getRowCount();
      for (int i = 0; i < numBlocklets; i++) {
        DataMapRow safeRow = memoryDMStore.getDataMapRow(i).convertToSafeRow();
        blocklets.add(createBlocklet(safeRow, getBlockletId(safeRow)));
      }
    } else {
      // Remove B-tree jump logic as start and end key prepared is not
      // correct for old store scenarios
      int startIndex = 0;
      numBlocklets = memoryDMStore.getRowCount();
      FilterExecuter filterExecuter =
          FilterUtil.getFilterExecuterTree(filterExp, segmentProperties, null);
      while (startIndex < numBlocklets) {
        DataMapRow safeRow = memoryDMStore.getDataMapRow(startIndex).convertToSafeRow();
        String filePath = new String(safeRow.getByteArray(FILE_PATH_INDEX),
            CarbonCommonConstants.DEFAULT_CHARSET_CLASS);
        short blockletId = getBlockletId(safeRow);
        boolean isValid =
            addBlockBasedOnMinMaxValue(filterExecuter, getMinMaxValue(safeRow, MAX_VALUES_INDEX),
                getMinMaxValue(safeRow, MIN_VALUES_INDEX), filePath, blockletId);
        if (isValid) {
          blocklets.add(createBlocklet(safeRow, blockletId));
        }
        startIndex++;
      }
    }
    ExplainCollector.addTotalBlocklets(numBlocklets);
    return blocklets;
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
    return prune(filterExp, this.segmentProperties);
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
   * @param filePath
   * @param blockletId
   * @return
   */
  private boolean addBlockBasedOnMinMaxValue(FilterExecuter filterExecuter, byte[][] maxValue,
      byte[][] minValue, String filePath, int blockletId) {
    BitSet bitSet = null;
    if (filterExecuter instanceof ImplicitColumnFilterExecutor) {
      String uniqueBlockPath = filePath.substring(filePath.lastIndexOf("/Part") + 1);
      // this case will come in case of old store where index file does not contain the
      // blocklet information
      if (blockletId != -1) {
        uniqueBlockPath = uniqueBlockPath + CarbonCommonConstants.FILE_SEPARATOR + blockletId;
      }
      bitSet = ((ImplicitColumnFilterExecutor) filterExecuter)
          .isFilterValuesPresentInBlockOrBlocklet(maxValue, minValue, uniqueBlockPath);
    } else {
      bitSet = filterExecuter.isScanRequired(maxValue, minValue);
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
      byte[] blockletRowCountForEachBlock = getBlockletRowCountForEachBlock();
      // Example: absoluteBlockletID = 17, blockletRowCountForEachBlock = {4,3,2,5,7}
      // step1: diff = 17-4, diff = 13
      // step2: diff = 13-3, diff = 10
      // step3: diff = 10-2, diff = 8
      // step4: diff = 8-5, diff = 3
      // step5: diff = 3-7, diff = -4 (satisfies <= 0)
      // step6: relativeBlockletId = -4+7, relativeBlockletId = 3 (4th index starting from 0)
      for (byte blockletCount : blockletRowCountForEachBlock) {
        diff = diff - blockletCount;
        if (diff < 0) {
          relativeBlockletId = (short) (diff + blockletCount);
          break;
        }
        rowIndex++;
      }
    }
    DataMapRow safeRow = memoryDMStore.getDataMapRow(rowIndex).convertToSafeRow();
    return createBlocklet(safeRow, relativeBlockletId);
  }

  private byte[] getBlockletRowCountForEachBlock() {
    // taskSummary DM store will  have only one row
    return taskSummaryDMStore.getDataMapRow(taskSummaryDMStore.getRowCount() - 1)
        .getByteArray(SUMMARY_BLOCKLET_COUNT);
  }

  /**
   * Get the index file name of the blocklet data map
   *
   * @return
   */
  public String getIndexFileName() {
    DataMapRow unsafeRow = taskSummaryDMStore.getDataMapRow(0);
    try {
      return new String(unsafeRow.getByteArray(SUMMARY_INDEX_FILE_NAME),
          CarbonCommonConstants.DEFAULT_CHARSET);
    } catch (UnsupportedEncodingException e) {
      // should never happen!
      throw new IllegalArgumentException("UTF8 encoding is not supported", e);
    }
  }

  private byte[][] getMinMaxValue(DataMapRow row, int index) {
    DataMapRow minMaxRow = row.getRow(index);
    byte[][] minMax = new byte[minMaxRow.getColumnCount()][];
    for (int i = 0; i < minMax.length; i++) {
      minMax[i] = minMaxRow.getByteArray(i);
    }
    return minMax;
  }

  protected short getBlockletId(DataMapRow dataMapRow) {
    return BLOCK_DEFAULT_BLOCKLET_ID;
  }

  protected ExtendedBlocklet createBlocklet(DataMapRow row, short blockletId) {
    ExtendedBlocklet blocklet = new ExtendedBlocklet(
        new String(row.getByteArray(FILE_PATH_INDEX), CarbonCommonConstants.DEFAULT_CHARSET_CLASS),
        blockletId + "");
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
    detailInfo.setDimLens(columnCardinality);
    detailInfo.setBlockletId(blockletId);
    detailInfo.setSchemaUpdatedTimeStamp(row.getLong(SCHEMA_UPADATED_TIME_INDEX));
    try {
      blocklet.setLocation(
          new String(row.getByteArray(LOCATIONS), CarbonCommonConstants.DEFAULT_CHARSET)
              .split(","));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    detailInfo.setBlockFooterOffset(row.getLong(BLOCK_FOOTER_OFFSET));
    detailInfo.setColumnSchemaBinary(getColumnSchemaBinary());
    detailInfo.setBlockSize(row.getLong(BLOCK_LENGTH));
    detailInfo.setLegacyStore(isLegacyStore);
    return detailInfo;
  }

  private String[] getFileDetails() {
    try {
      String[] fileDetails = new String[3];
      DataMapRow unsafeRow = taskSummaryDMStore.getDataMapRow(0);
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

  public byte[] getColumnSchemaBinary() {
    DataMapRow unsafeRow = taskSummaryDMStore.getDataMapRow(0);
    return unsafeRow.getByteArray(SUMMARY_SCHEMA);
  }

  /**
   * Convert schema to binary
   */
  private byte[] convertSchemaToBinary(List<ColumnSchema> columnSchemas) throws IOException {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    DataOutput dataOutput = new DataOutputStream(stream);
    dataOutput.writeShort(columnSchemas.size());
    for (ColumnSchema columnSchema : columnSchemas) {
      if (columnSchema.getColumnReferenceId() == null) {
        columnSchema.setColumnReferenceId(columnSchema.getColumnUniqueId());
      }
      columnSchema.write(dataOutput);
    }
    byte[] byteArray = stream.toByteArray();
    // Compress with snappy to reduce the size of schema
    return Snappy.rawCompress(byteArray, byteArray.length);
  }

  @Override public void clear() {
    if (memoryDMStore != null) {
      memoryDMStore.freeMemory();
      memoryDMStore = null;
      segmentProperties = null;
    }
    // clear task min/max unsafe memory
    if (null != taskSummaryDMStore) {
      taskSummaryDMStore.freeMemory();
      taskSummaryDMStore = null;
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

  public SegmentProperties getSegmentProperties() {
    return segmentProperties;
  }

  public void setSegmentProperties(SegmentProperties segmentProperties) {
    this.segmentProperties = segmentProperties;
  }

  public int[] getColumnCardinality() {
    return columnCardinality;
  }

  protected AbstractMemoryDMStore getMemoryDMStore(CarbonRowSchema[] schema, boolean addToUnsafe)
      throws MemoryException {
    AbstractMemoryDMStore memoryDMStore;
    if (addToUnsafe) {
      memoryDMStore = new UnsafeMemoryDMStore(schema);
    } else {
      memoryDMStore = new SafeMemoryDMStore(schema);
    }
    return memoryDMStore;
  }

  /**
   * This method will ocnvert safe to unsafe memory DM store
   *
   * @throws MemoryException
   */
  public void convertToUnsafeDMStore() throws MemoryException {
    if (memoryDMStore instanceof SafeMemoryDMStore) {
      UnsafeMemoryDMStore unsafeMemoryDMStore = memoryDMStore.convertToUnsafeDMStore();
      memoryDMStore.freeMemory();
      memoryDMStore = unsafeMemoryDMStore;
    }
    if (taskSummaryDMStore instanceof SafeMemoryDMStore) {
      UnsafeMemoryDMStore unsafeSummaryMemoryDMStore = taskSummaryDMStore.convertToUnsafeDMStore();
      taskSummaryDMStore.freeMemory();
      taskSummaryDMStore = unsafeSummaryMemoryDMStore;
    }
  }

  /**
   * Read column schema from binary
   *
   * @param schemaArray
   * @throws IOException
   */
  public List<ColumnSchema> readColumnSchema(byte[] schemaArray) throws IOException {
    // uncompress it.
    schemaArray = Snappy.uncompress(schemaArray);
    ByteArrayInputStream schemaStream = new ByteArrayInputStream(schemaArray);
    DataInput schemaInput = new DataInputStream(schemaStream);
    List<ColumnSchema> columnSchemas = new ArrayList<>();
    int size = schemaInput.readShort();
    for (int i = 0; i < size; i++) {
      ColumnSchema columnSchema = new ColumnSchema();
      columnSchema.readFields(schemaInput);
      columnSchemas.add(columnSchema);
    }
    return columnSchemas;
  }

  public long getBlockletSchemaTime() {
    return blockletSchemaTime;
  }

}
