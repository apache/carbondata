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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.cache.Cacheable;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.dev.DataMapModel;
import org.apache.carbondata.core.datamap.dev.cgdatamap.AbstractCoarseGrainIndexDataMap;
import org.apache.carbondata.core.datastore.IndexKey;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.indexstore.BlockMetaInfo;
import org.apache.carbondata.core.indexstore.Blocklet;
import org.apache.carbondata.core.indexstore.BlockletDetailInfo;
import org.apache.carbondata.core.indexstore.ExtendedBlocklet;
import org.apache.carbondata.core.indexstore.UnsafeMemoryDMStore;
import org.apache.carbondata.core.indexstore.row.DataMapRow;
import org.apache.carbondata.core.indexstore.row.DataMapRowImpl;
import org.apache.carbondata.core.indexstore.schema.CarbonRowSchema;
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.blocklet.BlockletInfo;
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.metadata.blocklet.index.BlockletIndex;
import org.apache.carbondata.core.metadata.blocklet.index.BlockletMinMaxIndex;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.scan.filter.FilterExpressionProcessor;
import org.apache.carbondata.core.scan.filter.FilterUtil;
import org.apache.carbondata.core.scan.filter.executer.FilterExecuter;
import org.apache.carbondata.core.scan.filter.executer.ImplicitColumnFilterExecutor;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.DataFileFooterConverter;
import org.apache.carbondata.core.util.DataTypeUtil;

import org.apache.commons.lang3.StringUtils;
import org.xerial.snappy.Snappy;

/**
 * Datamap implementation for blocklet.
 */
public class BlockletIndexDataMap extends AbstractCoarseGrainIndexDataMap implements Cacheable {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(BlockletIndexDataMap.class.getName());

  private static int KEY_INDEX = 0;

  private static int MIN_VALUES_INDEX = 1;

  private static int MAX_VALUES_INDEX = 2;

  private static int ROW_COUNT_INDEX = 3;

  private static int FILE_PATH_INDEX = 4;

  private static int PAGE_COUNT_INDEX = 5;

  private static int VERSION_INDEX = 6;

  private static int SCHEMA_UPADATED_TIME_INDEX = 7;

  private static int BLOCK_INFO_INDEX = 8;

  private static int BLOCK_FOOTER_OFFSET = 9;

  private static int LOCATIONS = 10;

  private static int BLOCKLET_ID_INDEX = 11;

  private static int BLOCK_LENGTH = 12;

  private static int TASK_MIN_VALUES_INDEX = 0;

  private static int TASK_MAX_VALUES_INDEX = 1;

  private static int SCHEMA = 2;

  private static int PARTITION_INFO = 3;

  private UnsafeMemoryDMStore unsafeMemoryDMStore;

  private UnsafeMemoryDMStore unsafeMemorySummaryDMStore;

  private SegmentProperties segmentProperties;

  private int[] columnCardinality;

  private boolean isPartitionedSegment;

  @Override
  public void init(DataMapModel dataMapModel) throws IOException, MemoryException {
    long startTime = System.currentTimeMillis();
    assert (dataMapModel instanceof BlockletDataMapModel);
    BlockletDataMapModel blockletDataMapInfo = (BlockletDataMapModel) dataMapModel;
    DataFileFooterConverter fileFooterConverter = new DataFileFooterConverter();
    List<DataFileFooter> indexInfo = fileFooterConverter
        .getIndexInfo(blockletDataMapInfo.getFilePath(), blockletDataMapInfo.getFileData());
    isPartitionedSegment = blockletDataMapInfo.isPartitionedSegment();
    DataMapRowImpl summaryRow = null;
    byte[] schemaBinary = null;
    // below 2 variables will be used for fetching the relative blocklet id. Relative blocklet ID
    // is id assigned to a blocklet within a part file
    String tempFilePath = null;
    int relativeBlockletId = 0;
    for (DataFileFooter fileFooter : indexInfo) {
      if (segmentProperties == null) {
        List<ColumnSchema> columnInTable = fileFooter.getColumnInTable();
        schemaBinary = convertSchemaToBinary(columnInTable);
        columnCardinality = fileFooter.getSegmentInfo().getColumnCardinality();
        segmentProperties = new SegmentProperties(columnInTable, columnCardinality);
        createSchema(segmentProperties);
        createSummarySchema(segmentProperties, blockletDataMapInfo.getPartitions(), schemaBinary);
      }
      TableBlockInfo blockInfo = fileFooter.getBlockInfo().getTableBlockInfo();
      BlockMetaInfo blockMetaInfo =
          blockletDataMapInfo.getBlockMetaInfoMap().get(blockInfo.getFilePath());
      // Here it loads info about all blocklets of index
      // Only add if the file exists physically. There are scenarios which index file exists inside
      // merge index but related carbondata files are deleted. In that case we first check whether
      // the file exists physically or not
      if (blockMetaInfo != null) {
        if (fileFooter.getBlockletList() == null) {
          // This is old store scenario, here blocklet information is not available in index file so
          // load only block info
          summaryRow =
              loadToUnsafeBlock(fileFooter, segmentProperties, blockInfo.getFilePath(), summaryRow,
                  blockMetaInfo);
        } else {
          // blocklet ID will start from 0 again only when part file path is changed
          if (null == tempFilePath || !tempFilePath.equals(blockInfo.getFilePath())) {
            tempFilePath = blockInfo.getFilePath();
            relativeBlockletId = 0;
          }
          summaryRow =
              loadToUnsafe(fileFooter, segmentProperties, blockInfo.getFilePath(), summaryRow,
                  blockMetaInfo, relativeBlockletId);
          // this is done because relative blocklet id need to be incremented based on the
          // total number of blocklets
          relativeBlockletId += fileFooter.getBlockletList().size();
        }
      }
    }
    if (unsafeMemoryDMStore != null) {
      unsafeMemoryDMStore.finishWriting();
    }
    if (null != unsafeMemorySummaryDMStore) {
      addTaskSummaryRowToUnsafeMemoryStore(
          summaryRow,
          blockletDataMapInfo.getPartitions(),
          schemaBinary);
      unsafeMemorySummaryDMStore.finishWriting();
    }
    LOGGER.info(
        "Time taken to load blocklet datamap from file : " + dataMapModel.getFilePath() + "is " + (
            System.currentTimeMillis() - startTime));
  }

  private DataMapRowImpl loadToUnsafe(DataFileFooter fileFooter,
      SegmentProperties segmentProperties, String filePath, DataMapRowImpl summaryRow,
      BlockMetaInfo blockMetaInfo, int relativeBlockletId) {
    int[] minMaxLen = segmentProperties.getColumnsValueSize();
    List<BlockletInfo> blockletList = fileFooter.getBlockletList();
    CarbonRowSchema[] schema = unsafeMemoryDMStore.getSchema();
    // Add one row to maintain task level min max for segment pruning
    if (!blockletList.isEmpty() && summaryRow == null) {
      summaryRow = new DataMapRowImpl(unsafeMemorySummaryDMStore.getSchema());
    }
    for (int index = 0; index < blockletList.size(); index++) {
      DataMapRow row = new DataMapRowImpl(schema);
      int ordinal = 0;
      int taskMinMaxOrdinal = 0;
      BlockletInfo blockletInfo = blockletList.get(index);

      // add start key as index key
      row.setByteArray(blockletInfo.getBlockletIndex().getBtreeIndex().getStartKey(), ordinal++);

      BlockletMinMaxIndex minMaxIndex = blockletInfo.getBlockletIndex().getMinMaxIndex();
      byte[][] minValues = updateMinValues(minMaxIndex.getMinValues(), minMaxLen);
      row.setRow(addMinMax(minMaxLen, schema[ordinal], minValues), ordinal);
      // compute and set task level min values
      addTaskMinMaxValues(summaryRow, minMaxLen,
          unsafeMemorySummaryDMStore.getSchema()[taskMinMaxOrdinal], minValues,
          TASK_MIN_VALUES_INDEX, true);
      ordinal++;
      taskMinMaxOrdinal++;
      byte[][] maxValues = updateMaxValues(minMaxIndex.getMaxValues(), minMaxLen);
      row.setRow(addMinMax(minMaxLen, schema[ordinal], maxValues), ordinal);
      // compute and set task level max values
      addTaskMinMaxValues(summaryRow, minMaxLen,
          unsafeMemorySummaryDMStore.getSchema()[taskMinMaxOrdinal], maxValues,
          TASK_MAX_VALUES_INDEX, false);
      ordinal++;

      row.setInt(blockletInfo.getNumberOfRows(), ordinal++);

      // add file path
      byte[] filePathBytes = filePath.getBytes(CarbonCommonConstants.DEFAULT_CHARSET_CLASS);
      row.setByteArray(filePathBytes, ordinal++);

      // add pages
      row.setShort((short) blockletInfo.getNumberOfPages(), ordinal++);

      // add version number
      row.setShort(fileFooter.getVersionId().number(), ordinal++);

      // add schema updated time
      row.setLong(fileFooter.getSchemaUpdatedTimeStamp(), ordinal++);

      // add blocklet info
      byte[] serializedData;
      try {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        DataOutput dataOutput = new DataOutputStream(stream);
        blockletInfo.write(dataOutput);
        serializedData = stream.toByteArray();
        row.setByteArray(serializedData, ordinal++);
        // Add block footer offset, it is used if we need to read footer of block
        row.setLong(fileFooter.getBlockInfo().getTableBlockInfo().getBlockOffset(), ordinal++);
        setLocations(blockMetaInfo.getLocationInfo(), row, ordinal);
        ordinal++;
        // for relative blockelt id i.e blocklet id that belongs to a particular part file
        row.setShort((short) relativeBlockletId++, ordinal++);
        // Store block size
        row.setLong(blockMetaInfo.getSize(), ordinal);
        unsafeMemoryDMStore.addIndexRowToUnsafe(row);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    return summaryRow;
  }

  private void setLocations(String[] locations, DataMapRow row, int ordinal)
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
  private DataMapRowImpl loadToUnsafeBlock(DataFileFooter fileFooter,
      SegmentProperties segmentProperties, String filePath, DataMapRowImpl summaryRow,
      BlockMetaInfo blockMetaInfo) {
    int[] minMaxLen = segmentProperties.getColumnsValueSize();
    BlockletIndex blockletIndex = fileFooter.getBlockletIndex();
    CarbonRowSchema[] schema = unsafeMemoryDMStore.getSchema();
    // Add one row to maintain task level min max for segment pruning
    if (summaryRow == null) {
      summaryRow = new DataMapRowImpl(unsafeMemorySummaryDMStore.getSchema());
    }
    DataMapRow row = new DataMapRowImpl(schema);
    int ordinal = 0;
    int taskMinMaxOrdinal = 0;
    // add start key as index key
    row.setByteArray(blockletIndex.getBtreeIndex().getStartKey(), ordinal++);

    BlockletMinMaxIndex minMaxIndex = blockletIndex.getMinMaxIndex();
    byte[][] minValues = updateMinValues(minMaxIndex.getMinValues(), minMaxLen);
    row.setRow(addMinMax(minMaxLen, schema[ordinal], minValues), ordinal);
    // compute and set task level min values
    addTaskMinMaxValues(summaryRow, minMaxLen,
        unsafeMemorySummaryDMStore.getSchema()[taskMinMaxOrdinal], minValues,
        TASK_MIN_VALUES_INDEX, true);
    ordinal++;
    taskMinMaxOrdinal++;
    byte[][] maxValues = updateMaxValues(minMaxIndex.getMaxValues(), minMaxLen);
    row.setRow(addMinMax(minMaxLen, schema[ordinal], maxValues), ordinal);
    // compute and set task level max values
    addTaskMinMaxValues(summaryRow, minMaxLen,
        unsafeMemorySummaryDMStore.getSchema()[taskMinMaxOrdinal], maxValues,
        TASK_MAX_VALUES_INDEX, false);
    ordinal++;

    row.setInt((int)fileFooter.getNumberOfRows(), ordinal++);

    // add file path
    byte[] filePathBytes = filePath.getBytes(CarbonCommonConstants.DEFAULT_CHARSET_CLASS);
    row.setByteArray(filePathBytes, ordinal++);

    // add pages
    row.setShort((short) 0, ordinal++);

    // add version number
    row.setShort(fileFooter.getVersionId().number(), ordinal++);

    // add schema updated time
    row.setLong(fileFooter.getSchemaUpdatedTimeStamp(), ordinal++);

    // add blocklet info
    row.setByteArray(new byte[0], ordinal++);

    row.setLong(fileFooter.getBlockInfo().getTableBlockInfo().getBlockOffset(), ordinal++);
    try {
      setLocations(blockMetaInfo.getLocationInfo(), row, ordinal);
      ordinal++;
      // for relative blocklet id. Value is -1 because in case of old store blocklet info will
      // not be present in the index file and in that case we will not knwo the total number of
      // blocklets
      row.setShort((short) -1, ordinal++);

      // store block size
      row.setLong(blockMetaInfo.getSize(), ordinal);
      unsafeMemoryDMStore.addIndexRowToUnsafe(row);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return summaryRow;
  }

  private void addTaskSummaryRowToUnsafeMemoryStore(DataMapRow summaryRow,
      List<String> partitions, byte[] schemaBinary) throws IOException {
    // write the task summary info to unsafe memory store
    if (null != summaryRow) {
      // Add column schema , it is useful to generate segment properties in executor.
      // So we no need to read footer again there.
      if (schemaBinary != null) {
        summaryRow.setByteArray(schemaBinary, SCHEMA);
      }
      if (partitions != null && partitions.size() > 0) {
        CarbonRowSchema[] minSchemas =
            ((CarbonRowSchema.StructCarbonRowSchema) unsafeMemorySummaryDMStore
                .getSchema()[PARTITION_INFO]).getChildSchemas();
        DataMapRow partitionRow = new DataMapRowImpl(minSchemas);
        for (int i = 0; i < partitions.size(); i++) {
          partitionRow
              .setByteArray(partitions.get(i).getBytes(CarbonCommonConstants.DEFAULT_CHARSET_CLASS),
                  i);
        }
        summaryRow.setRow(partitionRow, PARTITION_INFO);
      }
      try {
        unsafeMemorySummaryDMStore.addIndexRowToUnsafe(summaryRow);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Fill the measures min values with minimum , this is needed for backward version compatability
   * as older versions don't store min values for measures
   */
  private byte[][] updateMinValues(byte[][] minValues, int[] minMaxLen) {
    byte[][] updatedValues = minValues;
    if (minValues.length < minMaxLen.length) {
      updatedValues = new byte[minMaxLen.length][];
      System.arraycopy(minValues, 0, updatedValues, 0, minValues.length);
      List<CarbonMeasure> measures = segmentProperties.getMeasures();
      ByteBuffer buffer = ByteBuffer.allocate(8);
      for (int i = 0; i < measures.size(); i++) {
        buffer.rewind();
        DataType dataType = measures.get(i).getDataType();
        if (dataType == DataTypes.BYTE) {
          buffer.putLong(Byte.MIN_VALUE);
          updatedValues[minValues.length + i] = buffer.array().clone();
        } else if (dataType == DataTypes.SHORT) {
          buffer.putLong(Short.MIN_VALUE);
          updatedValues[minValues.length + i] = buffer.array().clone();
        } else if (dataType == DataTypes.INT) {
          buffer.putLong(Integer.MIN_VALUE);
          updatedValues[minValues.length + i] = buffer.array().clone();
        } else if (dataType == DataTypes.LONG) {
          buffer.putLong(Long.MIN_VALUE);
          updatedValues[minValues.length + i] = buffer.array().clone();
        } else if (DataTypes.isDecimal(dataType)) {
          updatedValues[minValues.length + i] =
              DataTypeUtil.bigDecimalToByte(BigDecimal.valueOf(Long.MIN_VALUE));
        } else {
          buffer.putDouble(Double.MIN_VALUE);
          updatedValues[minValues.length + i] = buffer.array().clone();
        }
      }
    }
    return updatedValues;
  }

  /**
   * Fill the measures max values with maximum , this is needed for backward version compatability
   * as older versions don't store max values for measures
   */
  private byte[][] updateMaxValues(byte[][] maxValues, int[] minMaxLen) {
    byte[][] updatedValues = maxValues;
    if (maxValues.length < minMaxLen.length) {
      updatedValues = new byte[minMaxLen.length][];
      System.arraycopy(maxValues, 0, updatedValues, 0, maxValues.length);
      List<CarbonMeasure> measures = segmentProperties.getMeasures();
      ByteBuffer buffer = ByteBuffer.allocate(8);
      for (int i = 0; i < measures.size(); i++) {
        buffer.rewind();
        DataType dataType = measures.get(i).getDataType();
        if (dataType == DataTypes.BYTE) {
          buffer.putLong(Byte.MAX_VALUE);
          updatedValues[maxValues.length + i] = buffer.array().clone();
        } else if (dataType == DataTypes.SHORT) {
          buffer.putLong(Short.MAX_VALUE);
          updatedValues[maxValues.length + i] = buffer.array().clone();
        } else if (dataType == DataTypes.INT) {
          buffer.putLong(Integer.MAX_VALUE);
          updatedValues[maxValues.length + i] = buffer.array().clone();
        } else if (dataType == DataTypes.LONG) {
          buffer.putLong(Long.MAX_VALUE);
          updatedValues[maxValues.length + i] = buffer.array().clone();
        } else if (DataTypes.isDecimal(dataType)) {
          updatedValues[maxValues.length + i] =
              DataTypeUtil.bigDecimalToByte(BigDecimal.valueOf(Long.MAX_VALUE));
        } else {
          buffer.putDouble(Double.MAX_VALUE);
          updatedValues[maxValues.length + i] = buffer.array().clone();
        }
      }
    }
    return updatedValues;
  }

  private DataMapRow addMinMax(int[] minMaxLen, CarbonRowSchema carbonRowSchema,
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
   * @param minMaxValue
   * @param ordinal
   * @param isMinValueComparison
   */
  private void addTaskMinMaxValues(DataMapRow taskMinMaxRow, int[] minMaxLen,
      CarbonRowSchema carbonRowSchema, byte[][] minMaxValue, int ordinal,
      boolean isMinValueComparison) {
    DataMapRow row = taskMinMaxRow.getRow(ordinal);
    byte[][] updatedMinMaxValues = minMaxValue;
    if (null == row) {
      CarbonRowSchema[] minSchemas =
          ((CarbonRowSchema.StructCarbonRowSchema) carbonRowSchema).getChildSchemas();
      row = new DataMapRowImpl(minSchemas);
    } else {
      byte[][] existingMinMaxValues = getMinMaxValue(taskMinMaxRow, ordinal);
      // Compare and update min max values
      for (int i = 0; i < minMaxLen.length; i++) {
        int compare =
            ByteUtil.UnsafeComparer.INSTANCE.compareTo(existingMinMaxValues[i], minMaxValue[i]);
        if (isMinValueComparison) {
          if (compare < 0) {
            updatedMinMaxValues[i] = existingMinMaxValues[i];
          }
        } else if (compare > 0) {
          updatedMinMaxValues[i] = existingMinMaxValues[i];
        }
      }
    }
    int minMaxOrdinal = 0;
    // min/max value adding
    for (int i = 0; i < minMaxLen.length; i++) {
      row.setByteArray(updatedMinMaxValues[i], minMaxOrdinal++);
    }
    taskMinMaxRow.setRow(row, ordinal);
  }

  private void createSchema(SegmentProperties segmentProperties) throws MemoryException {
    List<CarbonRowSchema> indexSchemas = new ArrayList<>();

    // Index key
    indexSchemas.add(new CarbonRowSchema.VariableCarbonRowSchema(DataTypes.BYTE_ARRAY));
    getMinMaxSchema(segmentProperties, indexSchemas);

    // for number of rows.
    indexSchemas.add(new CarbonRowSchema.FixedCarbonRowSchema(DataTypes.INT));

    // for table block path
    indexSchemas.add(new CarbonRowSchema.VariableCarbonRowSchema(DataTypes.BYTE_ARRAY));

    // for number of pages.
    indexSchemas.add(new CarbonRowSchema.FixedCarbonRowSchema(DataTypes.SHORT));

    // for version number.
    indexSchemas.add(new CarbonRowSchema.FixedCarbonRowSchema(DataTypes.SHORT));

    // for schema updated time.
    indexSchemas.add(new CarbonRowSchema.FixedCarbonRowSchema(DataTypes.LONG));

    //for blocklet info
    indexSchemas.add(new CarbonRowSchema.VariableCarbonRowSchema(DataTypes.BYTE_ARRAY));

    // for block footer offset.
    indexSchemas.add(new CarbonRowSchema.FixedCarbonRowSchema(DataTypes.LONG));

    // for locations
    indexSchemas.add(new CarbonRowSchema.VariableCarbonRowSchema(DataTypes.BYTE_ARRAY));

    // for relative blocklet id i.e. blocklet id that belongs to a particular part file.
    indexSchemas.add(new CarbonRowSchema.FixedCarbonRowSchema(DataTypes.SHORT));

    // for storing block length.
    indexSchemas.add(new CarbonRowSchema.FixedCarbonRowSchema(DataTypes.LONG));

    unsafeMemoryDMStore =
        new UnsafeMemoryDMStore(indexSchemas.toArray(new CarbonRowSchema[indexSchemas.size()]));
  }

  /**
   * Creates the schema to store summary information or the information which can be stored only
   * once per datamap. It stores datamap level max/min of each column and partition information of
   * datamap
   * @param segmentProperties
   * @param partitions
   * @throws MemoryException
   */
  private void createSummarySchema(SegmentProperties segmentProperties, List<String> partitions,
      byte[] schemaBinary)
      throws MemoryException {
    List<CarbonRowSchema> taskMinMaxSchemas = new ArrayList<>();
    getMinMaxSchema(segmentProperties, taskMinMaxSchemas);
    // for storing column schema
    taskMinMaxSchemas.add(
        new CarbonRowSchema.FixedCarbonRowSchema(DataTypes.BYTE_ARRAY, schemaBinary.length));
    if (partitions != null && partitions.size() > 0) {
      CarbonRowSchema[] mapSchemas = new CarbonRowSchema[partitions.size()];
      for (int i = 0; i < mapSchemas.length; i++) {
        mapSchemas[i] = new CarbonRowSchema.VariableCarbonRowSchema(DataTypes.BYTE_ARRAY);
      }
      CarbonRowSchema mapSchema =
          new CarbonRowSchema.StructCarbonRowSchema(DataTypes.createDefaultStructType(),
              mapSchemas);
      taskMinMaxSchemas.add(mapSchema);
    }
    unsafeMemorySummaryDMStore = new UnsafeMemoryDMStore(
        taskMinMaxSchemas.toArray(new CarbonRowSchema[taskMinMaxSchemas.size()]));
  }

  private void getMinMaxSchema(SegmentProperties segmentProperties,
      List<CarbonRowSchema> minMaxSchemas) {
    // Index key
    int[] minMaxLen = segmentProperties.getColumnsValueSize();
    // do it 2 times, one for min and one for max.
    for (int k = 0; k < 2; k++) {
      CarbonRowSchema[] mapSchemas = new CarbonRowSchema[minMaxLen.length];
      for (int i = 0; i < minMaxLen.length; i++) {
        if (minMaxLen[i] <= 0) {
          mapSchemas[i] = new CarbonRowSchema.VariableCarbonRowSchema(DataTypes.BYTE_ARRAY);
        } else {
          mapSchemas[i] =
              new CarbonRowSchema.FixedCarbonRowSchema(DataTypes.BYTE_ARRAY, minMaxLen[i]);
        }
      }
      CarbonRowSchema mapSchema =
          new CarbonRowSchema.StructCarbonRowSchema(DataTypes.createDefaultStructType(),
              mapSchemas);
      minMaxSchemas.add(mapSchema);
    }
  }

  @Override
  public boolean isScanRequired(FilterResolverIntf filterExp) {
    FilterExecuter filterExecuter =
        FilterUtil.getFilterExecuterTree(filterExp, segmentProperties, null);
    for (int i = 0; i < unsafeMemorySummaryDMStore.getRowCount(); i++) {
      DataMapRow unsafeRow = unsafeMemorySummaryDMStore.getUnsafeRow(i);
      boolean isScanRequired = FilterExpressionProcessor.isScanRequired(
          filterExecuter, getMinMaxValue(unsafeRow, TASK_MAX_VALUES_INDEX),
          getMinMaxValue(unsafeRow, TASK_MIN_VALUES_INDEX));
      if (isScanRequired) {
        return true;
      }
    }
    return false;
  }

  private List<Blocklet> prune(FilterResolverIntf filterExp, SegmentProperties segmentProperties) {
    if (unsafeMemoryDMStore.getRowCount() == 0) {
      return new ArrayList<>();
    }
    // getting the start and end index key based on filter for hitting the
    // selected block reference nodes based on filter resolver tree.
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("preparing the start and end key for finding"
          + "start and end block as per filter resolver");
    }
    List<Blocklet> blocklets = new ArrayList<>();
    Comparator<DataMapRow> comparator =
        new BlockletDMComparator(segmentProperties.getColumnsValueSize(),
            segmentProperties.getNumberOfSortColumns(),
            segmentProperties.getNumberOfNoDictSortColumns());
    List<IndexKey> listOfStartEndKeys = new ArrayList<IndexKey>(2);
    FilterUtil
        .traverseResolverTreeAndGetStartAndEndKey(segmentProperties, filterExp, listOfStartEndKeys);
    // reading the first value from list which has start key
    IndexKey searchStartKey = listOfStartEndKeys.get(0);
    // reading the last value from list which has end key
    IndexKey searchEndKey = listOfStartEndKeys.get(1);
    if (null == searchStartKey && null == searchEndKey) {
      try {
        // TODO need to handle for no dictionary dimensions
        searchStartKey = FilterUtil.prepareDefaultStartIndexKey(segmentProperties);
        // TODO need to handle for no dictionary dimensions
        searchEndKey = FilterUtil.prepareDefaultEndIndexKey(segmentProperties);
      } catch (KeyGenException e) {
        return null;
      }
    }
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "Successfully retrieved the start and end key" + "Dictionary Start Key: " + Arrays
              .toString(searchStartKey.getDictionaryKeys()) + "No Dictionary Start Key " + Arrays
              .toString(searchStartKey.getNoDictionaryKeys()) + "Dictionary End Key: " + Arrays
              .toString(searchEndKey.getDictionaryKeys()) + "No Dictionary End Key " + Arrays
              .toString(searchEndKey.getNoDictionaryKeys()));
    }
    if (filterExp == null) {
      int rowCount = unsafeMemoryDMStore.getRowCount();
      for (int i = 0; i < rowCount; i++) {
        DataMapRow safeRow = unsafeMemoryDMStore.getUnsafeRow(i).convertToSafeRow();
        blocklets.add(createBlocklet(safeRow, safeRow.getShort(BLOCKLET_ID_INDEX)));
      }
    } else {
      int startIndex = findStartIndex(convertToRow(searchStartKey), comparator);
      int endIndex = findEndIndex(convertToRow(searchEndKey), comparator);
      FilterExecuter filterExecuter =
          FilterUtil.getFilterExecuterTree(filterExp, segmentProperties, null);
      while (startIndex <= endIndex) {
        DataMapRow safeRow = unsafeMemoryDMStore.getUnsafeRow(startIndex).convertToSafeRow();
        int blockletId = safeRow.getShort(BLOCKLET_ID_INDEX);
        String filePath = new String(safeRow.getByteArray(FILE_PATH_INDEX),
            CarbonCommonConstants.DEFAULT_CHARSET_CLASS);
        boolean isValid =
            addBlockBasedOnMinMaxValue(filterExecuter, getMinMaxValue(safeRow, MAX_VALUES_INDEX),
                getMinMaxValue(safeRow, MIN_VALUES_INDEX), filePath, blockletId);
        if (isValid) {
          blocklets.add(createBlocklet(safeRow, blockletId));
        }
        startIndex++;
      }
    }
    return blocklets;
  }

  @Override
  public List<Blocklet> prune(FilterResolverIntf filterExp, SegmentProperties segmentProperties,
      List<String> partitions) {
    if (unsafeMemoryDMStore.getRowCount() == 0) {
      return new ArrayList<>();
    }
    // First get the partitions which are stored inside datamap.
    List<String> storedPartitions = getPartitions();
    // if it has partitioned datamap but there is no partitioned information stored, it means
    // partitions are dropped so return empty list.
    if (isPartitionedSegment && (storedPartitions == null || storedPartitions.size() == 0)) {
      return new ArrayList<>();
    }
    if (storedPartitions != null && storedPartitions.size() > 0) {
      // Check the exact match of partition information inside the stored partitions.
      boolean found = false;
      if (partitions != null && partitions.size() > 0) {
        found = partitions.containsAll(storedPartitions);
      }
      if (!found) {
        return new ArrayList<>();
      }
    }
    // Prune with filters if the partitions are existed in this datamap
    return prune(filterExp, segmentProperties);
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
    int index = Integer.parseInt(blockletId);
    DataMapRow safeRow = unsafeMemoryDMStore.getUnsafeRow(index).convertToSafeRow();
    return createBlocklet(safeRow, safeRow.getShort(BLOCKLET_ID_INDEX));
  }

  private byte[][] getMinMaxValue(DataMapRow row, int index) {
    DataMapRow minMaxRow = row.getRow(index);
    byte[][] minMax = new byte[minMaxRow.getColumnCount()][];
    for (int i = 0; i < minMax.length; i++) {
      minMax[i] = minMaxRow.getByteArray(i);
    }
    return minMax;
  }

  private ExtendedBlocklet createBlocklet(DataMapRow row, int blockletId) {
    ExtendedBlocklet blocklet = new ExtendedBlocklet(
        new String(row.getByteArray(FILE_PATH_INDEX), CarbonCommonConstants.DEFAULT_CHARSET_CLASS),
        blockletId + "");
    BlockletDetailInfo detailInfo = new BlockletDetailInfo();
    detailInfo.setRowCount(row.getInt(ROW_COUNT_INDEX));
    detailInfo.setPagesCount(row.getShort(PAGE_COUNT_INDEX));
    detailInfo.setVersionNumber(row.getShort(VERSION_INDEX));
    detailInfo.setBlockletId((short) blockletId);
    detailInfo.setDimLens(columnCardinality);
    detailInfo.setSchemaUpdatedTimeStamp(row.getLong(SCHEMA_UPADATED_TIME_INDEX));
    byte[] byteArray = row.getByteArray(BLOCK_INFO_INDEX);
    BlockletInfo blockletInfo = null;
    try {
      if (byteArray.length > 0) {
        blockletInfo = new BlockletInfo();
        ByteArrayInputStream stream = new ByteArrayInputStream(byteArray);
        DataInputStream inputStream = new DataInputStream(stream);
        blockletInfo.readFields(inputStream);
        inputStream.close();
      }
      blocklet.setLocation(
          new String(row.getByteArray(LOCATIONS), CarbonCommonConstants.DEFAULT_CHARSET)
              .split(","));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    detailInfo.setBlockletInfo(blockletInfo);
    blocklet.setDetailInfo(detailInfo);
    detailInfo.setBlockFooterOffset(row.getLong(BLOCK_FOOTER_OFFSET));
    detailInfo.setColumnSchemaBinary(getColumnSchemaBinary());
    detailInfo.setBlockSize(row.getLong(BLOCK_LENGTH));
    return blocklet;
  }

  /**
   * Binary search used to get the first tentative index row based on
   * search key
   *
   * @param key search key
   * @return first tentative block
   */
  private int findStartIndex(DataMapRow key, Comparator<DataMapRow> comparator) {
    int childNodeIndex;
    int low = 0;
    int high = unsafeMemoryDMStore.getRowCount() - 1;
    int mid = 0;
    int compareRes = -1;
    //
    while (low <= high) {
      mid = (low + high) >>> 1;
      // compare the entries
      compareRes = comparator.compare(key, unsafeMemoryDMStore.getUnsafeRow(mid));
      if (compareRes < 0) {
        high = mid - 1;
      } else if (compareRes > 0) {
        low = mid + 1;
      } else {
        // if key is matched then get the first entry
        int currentPos = mid;
        while (currentPos - 1 >= 0
            && comparator.compare(key, unsafeMemoryDMStore.getUnsafeRow(currentPos - 1)) == 0) {
          currentPos--;
        }
        mid = currentPos;
        break;
      }
    }
    // if compare result is less than zero then we
    // and mid is more than 0 then we need to previous block as duplicates
    // record can be present
    if (compareRes < 0) {
      if (mid > 0) {
        mid--;
      }
      childNodeIndex = mid;
    } else {
      childNodeIndex = mid;
    }
    // get the leaf child
    return childNodeIndex;
  }

  /**
   * Binary search used to get the last tentative block  based on
   * search key
   *
   * @param key search key
   * @return first tentative block
   */
  private int findEndIndex(DataMapRow key, Comparator<DataMapRow> comparator) {
    int childNodeIndex;
    int low = 0;
    int high = unsafeMemoryDMStore.getRowCount() - 1;
    int mid = 0;
    int compareRes = -1;
    //
    while (low <= high) {
      mid = (low + high) >>> 1;
      // compare the entries
      compareRes = comparator.compare(key, unsafeMemoryDMStore.getUnsafeRow(mid));
      if (compareRes < 0) {
        high = mid - 1;
      } else if (compareRes > 0) {
        low = mid + 1;
      } else {
        int currentPos = mid;
        // if key is matched then get the first entry
        while (currentPos + 1 < unsafeMemoryDMStore.getRowCount()
            && comparator.compare(key, unsafeMemoryDMStore.getUnsafeRow(currentPos + 1)) == 0) {
          currentPos++;
        }
        mid = currentPos;
        break;
      }
    }
    // if compare result is less than zero then we
    // and mid is more than 0 then we need to previous block as duplicates
    // record can be present
    if (compareRes < 0) {
      if (mid > 0) {
        mid--;
      }
      childNodeIndex = mid;
    } else {
      childNodeIndex = mid;
    }
    return childNodeIndex;
  }

  private DataMapRow convertToRow(IndexKey key) {
    ByteBuffer buffer =
        ByteBuffer.allocate(key.getDictionaryKeys().length + key.getNoDictionaryKeys().length + 8);
    buffer.putInt(key.getDictionaryKeys().length);
    buffer.putInt(key.getNoDictionaryKeys().length);
    buffer.put(key.getDictionaryKeys());
    buffer.put(key.getNoDictionaryKeys());
    DataMapRowImpl dataMapRow = new DataMapRowImpl(unsafeMemoryDMStore.getSchema());
    dataMapRow.setByteArray(buffer.array(), 0);
    return dataMapRow;
  }

  private List<String> getPartitions() {
    DataMapRow unsafeRow = unsafeMemorySummaryDMStore.getUnsafeRow(0);
    if (unsafeRow.getColumnCount() > PARTITION_INFO) {
      List<String> partitions = new ArrayList<>();
      DataMapRow row = unsafeRow.getRow(PARTITION_INFO);
      for (int i = 0; i < row.getColumnCount(); i++) {
        partitions.add(
            new String(row.getByteArray(i), CarbonCommonConstants.DEFAULT_CHARSET_CLASS));
      }
      return partitions;
    }
    return null;
  }

  private byte[] getColumnSchemaBinary() {
    DataMapRow unsafeRow = unsafeMemorySummaryDMStore.getUnsafeRow(0);
    return unsafeRow.getByteArray(SCHEMA);
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

  @Override
  public void clear() {
    if (unsafeMemoryDMStore != null) {
      unsafeMemoryDMStore.freeMemory();
      unsafeMemoryDMStore = null;
      segmentProperties = null;
    }
    // clear task min/max unsafe memory
    if (null != unsafeMemorySummaryDMStore) {
      unsafeMemorySummaryDMStore.freeMemory();
      unsafeMemorySummaryDMStore = null;
    }
  }

  @Override
  public long getFileTimeStamp() {
    return 0;
  }

  @Override
  public int getAccessCount() {
    return 0;
  }

  @Override
  public long getMemorySize() {
    long memoryUsed = 0L;
    if (unsafeMemoryDMStore != null) {
      memoryUsed += unsafeMemoryDMStore.getMemoryUsed();
    }
    if (null != unsafeMemorySummaryDMStore) {
      memoryUsed += unsafeMemorySummaryDMStore.getMemoryUsed();
    }
    return memoryUsed;
  }

  public SegmentProperties getSegmentProperties() {
    return segmentProperties;
  }

}
