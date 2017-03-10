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
package org.apache.carbondata.core.scan.collector.impl;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.cache.update.BlockletLevelDeleteDeltaDataCache;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.core.keygenerator.KeyGenerator;
import org.apache.carbondata.core.keygenerator.mdkey.MultiDimKeyVarLengthGenerator;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.model.QueryDimension;
import org.apache.carbondata.core.scan.model.QueryMeasure;
import org.apache.carbondata.core.scan.result.AbstractScannedResult;
import org.apache.carbondata.core.scan.wrappers.ByteArrayWrapper;
import org.apache.carbondata.core.util.CarbonUtil;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * It is not a collector it is just a scanned result holder.
 */
public class RestructureBasedRawResultCollector extends AbstractScannedResultCollector {

  /**
   * logger
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(RestructureBasedRawResultCollector.class.getName());

  /**
   * Key generator which will form the mdKey according to latest schema
   */
  private KeyGenerator restructuredKeyGenerator;

  /**
   * Key generator for uncompressing current block values
   */
  private KeyGenerator updatedCurrentBlockKeyGenerator;

  public RestructureBasedRawResultCollector(BlockExecutionInfo blockExecutionInfos) {
    super(blockExecutionInfos);
    initRestructuredKeyGenerator();
    initCurrentBlockKeyGenerator();
  }

  /**
   * This method will create a new key generator for generating mdKey according to latest schema
   */
  private void initRestructuredKeyGenerator() {
    SegmentProperties segmentProperties =
        tableBlockExecutionInfos.getDataBlock().getSegmentProperties();
    QueryDimension[] queryDimensions = tableBlockExecutionInfos.getActualQueryDimensions();
    List<Integer> updatedColumnCardinality = new ArrayList<>(queryDimensions.length);
    List<Integer> updatedDimensionPartitioner = new ArrayList<>(queryDimensions.length);
    int[] dictionaryColumnBlockIndex = tableBlockExecutionInfos.getDictionaryColumnBlockIndex();
    int dimCounterInCurrentBlock = 0;
    for (int i = 0; i < queryDimensions.length; i++) {
      if (queryDimensions[i].getDimension().hasEncoding(Encoding.DICTIONARY)) {
        if (tableBlockExecutionInfos.getDimensionInfo().getDimensionExists()[i]) {
          // get the dictionary key ordinal as column cardinality in segment properties
          // will only be for dictionary encoded columns
          CarbonDimension currentBlockDimension = segmentProperties.getDimensions()
              .get(dictionaryColumnBlockIndex[dimCounterInCurrentBlock]);
          updatedColumnCardinality.add(segmentProperties
              .getDimColumnsCardinality()[currentBlockDimension.getKeyOrdinal()]);
          updatedDimensionPartitioner.add(segmentProperties
              .getDimensionPartitions()[currentBlockDimension.getKeyOrdinal()]);
          dimCounterInCurrentBlock++;
        } else {
          // partitioner index will be 1 every column will be in columnar format
          updatedDimensionPartitioner.add(1);
          // for direct dictionary 4 bytes need to be allocated else 1
          if (queryDimensions[i].getDimension().hasEncoding(Encoding.DIRECT_DICTIONARY)) {
            updatedColumnCardinality.add(Integer.MAX_VALUE);
          } else {
            // cardinality will be 2 will user has provided a default value
            byte[] defaultValue = queryDimensions[i].getDimension().getDefaultValue();
            if (null != defaultValue) {
              updatedColumnCardinality
                  .add(CarbonCommonConstants.DICTIONARY_DEFAULT_CARDINALITY + 1);
            } else {
              updatedColumnCardinality.add(CarbonCommonConstants.DICTIONARY_DEFAULT_CARDINALITY);
            }
          }
        }
      }
    }
    if (!updatedColumnCardinality.isEmpty()) {
      int[] latestColumnCardinality = ArrayUtils.toPrimitive(
          updatedColumnCardinality.toArray(new Integer[updatedColumnCardinality.size()]));
      int[] latestColumnPartitioner = ArrayUtils.toPrimitive(
          updatedDimensionPartitioner.toArray(new Integer[updatedDimensionPartitioner.size()]));
      int[] dimensionBitLength =
          CarbonUtil.getDimensionBitLength(latestColumnCardinality, latestColumnPartitioner);
      restructuredKeyGenerator = new MultiDimKeyVarLengthGenerator(dimensionBitLength);
    }
  }

  /**
   * This method will initialize the block key generator for the current block based on the
   * dictionary columns present in the current block
   */
  private void initCurrentBlockKeyGenerator() {
    SegmentProperties segmentProperties =
        tableBlockExecutionInfos.getDataBlock().getSegmentProperties();
    int[] dictionaryColumnBlockIndex = tableBlockExecutionInfos.getDictionaryColumnBlockIndex();
    int[] updatedColumnCardinality = new int[dictionaryColumnBlockIndex.length];
    int[] updatedDimensionPartitioner = new int[dictionaryColumnBlockIndex.length];
    for (int i = 0; i < dictionaryColumnBlockIndex.length; i++) {
      // get the dictionary key ordinal as column cardinality in segment properties
      // will only be for dictionary encoded columns
      CarbonDimension currentBlockDimension = segmentProperties.getDimensions()
          .get(dictionaryColumnBlockIndex[i]);
      updatedColumnCardinality[i] =
          segmentProperties.getDimColumnsCardinality()[currentBlockDimension.getKeyOrdinal()];
      updatedDimensionPartitioner[i] =
          segmentProperties.getDimensionPartitions()[currentBlockDimension.getKeyOrdinal()];
    }
    if (dictionaryColumnBlockIndex.length > 0) {
      int[] dimensionBitLength =
          CarbonUtil.getDimensionBitLength(updatedColumnCardinality, updatedDimensionPartitioner);
      updatedCurrentBlockKeyGenerator = new MultiDimKeyVarLengthGenerator(dimensionBitLength);
    }
  }

  /**
   * This method will add a record both key and value to list object
   * it will keep track of how many record is processed, to handle limit scenario
   */
  @Override public List<Object[]> collectData(AbstractScannedResult scannedResult, int batchSize) {
    List<Object[]> listBasedResult = new ArrayList<>(batchSize);
    QueryMeasure[] queryMeasures = tableBlockExecutionInfos.getActualQueryMeasures();
    ByteArrayWrapper wrapper = null;
    BlockletLevelDeleteDeltaDataCache deleteDeltaDataCache =
        scannedResult.getDeleteDeltaDataCache();
    // scan the record and add to list
    int rowCounter = 0;
    while (scannedResult.hasNext() && rowCounter < batchSize) {
      byte[] dictionaryKeyArray = scannedResult.getDictionaryKeyArray();
      byte[][] noDictionaryKeyArray = scannedResult.getNoDictionaryKeyArray();
      byte[][] complexTypeKeyArray = scannedResult.getComplexTypeKeyArray();
      byte[] implicitColumnByteArray = scannedResult.getBlockletId()
          .getBytes(Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
      if (null != deleteDeltaDataCache && deleteDeltaDataCache
          .contains(scannedResult.getCurrenrRowId())) {
        continue;
      }
      Object[] row = new Object[1 + queryMeasures.length];
      wrapper = new ByteArrayWrapper();
      wrapper.setDictionaryKey(fillDictionaryKeyArrayWithLatestSchema(dictionaryKeyArray));
      wrapper.setNoDictionaryKeys(fillNoDictionaryKeyArrayWithLatestSchema(noDictionaryKeyArray));
      wrapper.setComplexTypesKeys(complexTypeKeyArray);
      wrapper.setImplicitColumnByteArray(implicitColumnByteArray);
      row[0] = wrapper;
      fillMeasureData(row, 1, scannedResult);
      listBasedResult.add(row);
      rowCounter++;
    }
    return listBasedResult;
  }

  /**
   * This method will fill the dictionary key array with newly added dictionary columns if any
   *
   * @param dictionaryKeyArray
   * @return
   */
  private byte[] fillDictionaryKeyArrayWithLatestSchema(byte[] dictionaryKeyArray) {
    QueryDimension[] actualQueryDimensions = tableBlockExecutionInfos.getActualQueryDimensions();
    List<Long> keyArrayWithNewColumnValues = new ArrayList<>(actualQueryDimensions.length);
    long[] keyArray = updatedCurrentBlockKeyGenerator.getKeyArray(dictionaryKeyArray);
    int existingColumnKeyArrayIndex = 0;
    for (int i = 0; i < dimensionInfo.getDimensionExists().length; i++) {
      if (CarbonUtil
          .hasEncoding(actualQueryDimensions[i].getDimension().getEncoder(), Encoding.DICTIONARY)) {
        // if dimension exists then add the key array value else add the default value
        if (dimensionInfo.getDimensionExists()[i]) {
          keyArrayWithNewColumnValues.add(keyArray[existingColumnKeyArrayIndex++]);
        } else {
          Long defaultValueAsLong = null;
          Object defaultValue = dimensionInfo.getDefaultValues()[i];
          if (null != defaultValue) {
            defaultValueAsLong = ((Integer) defaultValue).longValue();
          } else {
            defaultValueAsLong =
                new Integer(CarbonCommonConstants.MEMBER_DEFAULT_VAL_SURROGATE_KEY).longValue();
          }
          keyArrayWithNewColumnValues.add(defaultValueAsLong);
        }
      }
    }
    if (!keyArrayWithNewColumnValues.isEmpty()) {
      long[] keyArrayWithLatestSchema = ArrayUtils.toPrimitive(
          keyArrayWithNewColumnValues.toArray(new Long[keyArrayWithNewColumnValues.size()]));
      try {
        dictionaryKeyArray = restructuredKeyGenerator.generateKey(keyArrayWithLatestSchema);
      } catch (KeyGenException e) {
        LOGGER.error(e, e.getMessage());
      }
    }
    return dictionaryKeyArray;
  }

  /**
   * This method will fill the no dictionary byte array with newly added no dictionary columns
   *
   * @param noDictionaryKeyArray
   * @return
   */
  private byte[][] fillNoDictionaryKeyArrayWithLatestSchema(byte[][] noDictionaryKeyArray) {
    QueryDimension[] actualQueryDimensions = tableBlockExecutionInfos.getActualQueryDimensions();
    List<byte[]> noDictionaryValueList = new ArrayList<>(actualQueryDimensions.length);
    int existingColumnValueIndex = 0;
    for (int i = 0; i < dimensionInfo.getDimensionExists().length; i++) {
      if (!actualQueryDimensions[i].getDimension().hasEncoding(Encoding.DICTIONARY)) {
        // if dimension exists then add the byte array value else add the default value
        if (dimensionInfo.getDimensionExists()[i]) {
          noDictionaryValueList.add(noDictionaryKeyArray[existingColumnValueIndex++]);
        } else {
          byte[] newColumnDefaultValue = null;
          Object defaultValue = dimensionInfo.getDefaultValues()[i];
          if (null != defaultValue) {
            newColumnDefaultValue = UTF8String.fromString((String) defaultValue).getBytes();
          } else {
            newColumnDefaultValue =
                UTF8String.fromString(CarbonCommonConstants.MEMBER_DEFAULT_VAL).getBytes();
          }
          noDictionaryValueList.add(newColumnDefaultValue);
        }
      }
    }
    // fill the 2-D byte array with all the values of columns in latest schema
    if (!noDictionaryValueList.isEmpty()) {
      noDictionaryKeyArray = new byte[noDictionaryValueList.size()][];
      for (int i = 0; i < noDictionaryKeyArray.length; i++) {
        noDictionaryKeyArray[i] = noDictionaryValueList.get(i);
      }
    }
    return noDictionaryKeyArray;
  }
}
