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
import org.apache.carbondata.core.datamap.dev.DataMap;
import org.apache.carbondata.core.datamap.dev.DataMapModel;
import org.apache.carbondata.core.datastore.IndexKey;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
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
import org.apache.carbondata.core.metadata.blocklet.index.BlockletMinMaxIndex;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.scan.filter.FilterUtil;
import org.apache.carbondata.core.scan.filter.executer.FilterExecuter;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataFileFooterConverter;
import org.apache.carbondata.core.util.DataTypeUtil;

/**
 * Datamap implementation for blocklet.
 */
public class BlockletDataMap implements DataMap, Cacheable {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(BlockletDataMap.class.getName());

  public static final String NAME = "clustered.btree.blocklet";

  private static int KEY_INDEX = 0;

  private static int MIN_VALUES_INDEX = 1;

  private static int MAX_VALUES_INDEX = 2;

  private static int ROW_COUNT_INDEX = 3;

  private static int FILE_PATH_INDEX = 4;

  private static int PAGE_COUNT_INDEX = 5;

  private static int VERSION_INDEX = 6;

  private static int SCHEMA_UPADATED_TIME_INDEX = 7;

  private static int BLOCK_INFO_INDEX = 8;

  private UnsafeMemoryDMStore unsafeMemoryDMStore;

  private SegmentProperties segmentProperties;

  private int[] columnCardinality;

  @Override
  public void init(DataMapModel dataMapModel) throws IOException, MemoryException {
    long startTime = System.currentTimeMillis();
    assert (dataMapModel instanceof BlockletDataMapModel);
    BlockletDataMapModel blockletDataMapInfo = (BlockletDataMapModel) dataMapModel;
    DataFileFooterConverter fileFooterConverter = new DataFileFooterConverter();
    List<DataFileFooter> indexInfo = fileFooterConverter
        .getIndexInfo(blockletDataMapInfo.getFilePath(), blockletDataMapInfo.getFileData());
    for (DataFileFooter fileFooter : indexInfo) {
      List<ColumnSchema> columnInTable = fileFooter.getColumnInTable();
      if (segmentProperties == null) {
        columnCardinality = fileFooter.getSegmentInfo().getColumnCardinality();
        segmentProperties = new SegmentProperties(columnInTable, columnCardinality);
        createSchema(segmentProperties);
      }
      TableBlockInfo blockInfo = fileFooter.getBlockInfo().getTableBlockInfo();
      if (fileFooter.getBlockletList() == null || fileFooter.getBlockletList().size() == 0) {
        LOGGER
            .info("Reading carbondata file footer to get blocklet info " + blockInfo.getFilePath());
        fileFooter = CarbonUtil.readMetadatFile(blockInfo);
      }

      loadToUnsafe(fileFooter, segmentProperties, blockInfo.getFilePath());
    }
    if (unsafeMemoryDMStore != null) {
      unsafeMemoryDMStore.finishWriting();
    }
    LOGGER.info(
        "Time taken to load blocklet datamap from file : " + dataMapModel.getFilePath() + "is " + (
            System.currentTimeMillis() - startTime));
  }

  private void loadToUnsafe(DataFileFooter fileFooter, SegmentProperties segmentProperties,
      String filePath) {
    int[] minMaxLen = segmentProperties.getColumnsValueSize();
    List<BlockletInfo> blockletList = fileFooter.getBlockletList();
    CarbonRowSchema[] schema = unsafeMemoryDMStore.getSchema();
    for (int index = 0; index < blockletList.size(); index++) {
      DataMapRow row = new DataMapRowImpl(schema);
      int ordinal = 0;
      BlockletInfo blockletInfo = blockletList.get(index);

      // add start key as index key
      row.setByteArray(blockletInfo.getBlockletIndex().getBtreeIndex().getStartKey(), ordinal++);

      BlockletMinMaxIndex minMaxIndex = blockletInfo.getBlockletIndex().getMinMaxIndex();
      row.setRow(addMinMax(minMaxLen, schema[ordinal],
          updateMinValues(minMaxIndex.getMinValues(), minMaxLen)), ordinal);
      ordinal++;
      row.setRow(addMinMax(minMaxLen, schema[ordinal],
          updateMaxValues(minMaxIndex.getMaxValues(), minMaxLen)), ordinal);
      ordinal++;

      row.setInt(blockletInfo.getNumberOfRows(), ordinal++);

      // add file path
      byte[] filePathBytes =
          filePath.getBytes(CarbonCommonConstants.DEFAULT_CHARSET_CLASS);
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
        row.setByteArray(serializedData, ordinal);
        unsafeMemoryDMStore.addIndexRowToUnsafe(row);
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

  private void createSchema(SegmentProperties segmentProperties) throws MemoryException {
    List<CarbonRowSchema> indexSchemas = new ArrayList<>();

    // Index key
    indexSchemas.add(new CarbonRowSchema.VariableCarbonRowSchema(DataTypes.BYTE_ARRAY));
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
      indexSchemas.add(mapSchema);
    }

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

    unsafeMemoryDMStore =
        new UnsafeMemoryDMStore(indexSchemas.toArray(new CarbonRowSchema[indexSchemas.size()]));
  }

  @Override
  public List<Blocklet> prune(FilterResolverIntf filterExp) {

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
        DataMapRow unsafeRow = unsafeMemoryDMStore.getUnsafeRow(i);
        blocklets.add(createBlocklet(unsafeRow, i));
      }
    } else {
      int startIndex = findStartIndex(convertToRow(searchStartKey), comparator);
      int endIndex = findEndIndex(convertToRow(searchEndKey), comparator);
      FilterExecuter filterExecuter =
          FilterUtil.getFilterExecuterTree(filterExp, segmentProperties, null);
      while (startIndex <= endIndex) {
        DataMapRow unsafeRow = unsafeMemoryDMStore.getUnsafeRow(startIndex);
        BitSet bitSet = filterExecuter.isScanRequired(getMinMaxValue(unsafeRow, MAX_VALUES_INDEX),
            getMinMaxValue(unsafeRow, MIN_VALUES_INDEX));
        if (!bitSet.isEmpty()) {
          blocklets.add(createBlocklet(unsafeRow, startIndex));
        }
        startIndex++;
      }
    }

    return blocklets;
  }

  public ExtendedBlocklet getDetailedBlocklet(String blockletId) {
    int index = Integer.parseInt(blockletId);
    DataMapRow unsafeRow = unsafeMemoryDMStore.getUnsafeRow(index);
    return createBlocklet(unsafeRow, index);
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
    detailInfo.setDimLens(columnCardinality);
    detailInfo.setSchemaUpdatedTimeStamp(row.getLong(SCHEMA_UPADATED_TIME_INDEX));
    BlockletInfo blockletInfo = new BlockletInfo();
    try {
      byte[] byteArray = row.getByteArray(BLOCK_INFO_INDEX);
      ByteArrayInputStream stream = new ByteArrayInputStream(byteArray);
      DataInputStream inputStream = new DataInputStream(stream);
      blockletInfo.readFields(inputStream);
      inputStream.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    detailInfo.setBlockletInfo(blockletInfo);
    blocklet.setDetailInfo(detailInfo);
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

  @Override
  public void clear() {
    if (unsafeMemoryDMStore != null) {
      unsafeMemoryDMStore.freeMemory();
      unsafeMemoryDMStore = null;
      segmentProperties = null;
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
    if (unsafeMemoryDMStore != null) {
      return unsafeMemoryDMStore.getMemoryUsed();
    } else {
      return 0;
    }
  }

}
