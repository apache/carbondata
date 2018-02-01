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

import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.core.keygenerator.KeyGenerator;
import org.apache.carbondata.core.keygenerator.mdkey.MultiDimKeyVarLengthGenerator;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.model.ProjectionDimension;
import org.apache.carbondata.core.scan.model.ProjectionMeasure;
import org.apache.carbondata.core.scan.result.BlockletScannedResult;
import org.apache.carbondata.core.util.CarbonUtil;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * It is not a collector it is just a scanned result holder.
 */
public class RestructureBasedRawResultCollector extends RawBasedResultCollector {

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
        executionInfo.getDataBlock().getSegmentProperties();
    ProjectionDimension[] queryDimensions = executionInfo.getActualQueryDimensions();
    List<Integer> updatedColumnCardinality = new ArrayList<>(queryDimensions.length);
    List<Integer> updatedDimensionPartitioner = new ArrayList<>(queryDimensions.length);
    int[] dictionaryColumnBlockIndex = executionInfo.getDictionaryColumnChunkIndex();
    int dimCounterInCurrentBlock = 0;
    for (int i = 0; i < queryDimensions.length; i++) {
      if (queryDimensions[i].getDimension().hasEncoding(Encoding.DICTIONARY)) {
        if (executionInfo.getDimensionInfo().getDimensionExists()[i]) {
          // get the dictionary key ordinal as column cardinality in segment properties
          // will only be for dictionary encoded columns
          CarbonDimension currentBlockDimension = segmentProperties.getDimensions()
              .get(dictionaryColumnBlockIndex[dimCounterInCurrentBlock]);
          updatedColumnCardinality.add(
              segmentProperties.getDimColumnsCardinality()[currentBlockDimension.getKeyOrdinal()]);
          updatedDimensionPartitioner.add(
              segmentProperties.getDimensionPartitions()[currentBlockDimension.getKeyOrdinal()]);
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
        executionInfo.getDataBlock().getSegmentProperties();
    int[] dictionaryColumnBlockIndex = executionInfo.getDictionaryColumnChunkIndex();
    int[] updatedColumnCardinality = new int[dictionaryColumnBlockIndex.length];
    int[] updatedDimensionPartitioner = new int[dictionaryColumnBlockIndex.length];
    for (int i = 0; i < dictionaryColumnBlockIndex.length; i++) {
      // get the dictionary key ordinal as column cardinality in segment properties
      // will only be for dictionary encoded columns
      CarbonDimension currentBlockDimension =
          segmentProperties.getDimensions().get(dictionaryColumnBlockIndex[i]);
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
  @Override
  public List<Object[]> collectResultInRow(BlockletScannedResult scannedResult, int batchSize) {
    List<Object[]> listBasedResult = new ArrayList<>(batchSize);
    ProjectionMeasure[] queryMeasures = executionInfo.getActualQueryMeasures();
    // scan the record and add to list
    int rowCounter = 0;
    while (scannedResult.hasNext() && rowCounter < batchSize) {
      scanResultAndGetData(scannedResult);
      if (scannedResult.containsDeletedRow(scannedResult.getCurrentRowId())) {
        continue;
      }
      // re-fill dictionary and no dictionary key arrays for the newly added columns
      if (dimensionInfo.isDictionaryColumnAdded()) {
        dictionaryKeyArray = fillDictionaryKeyArrayWithLatestSchema(dictionaryKeyArray);
      }
      if (dimensionInfo.isNoDictionaryColumnAdded()) {
        noDictionaryKeyArray = fillNoDictionaryKeyArrayWithLatestSchema(noDictionaryKeyArray);
      }
      prepareRow(scannedResult, listBasedResult, queryMeasures);
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
    ProjectionDimension[] actualQueryDimensions = executionInfo.getActualQueryDimensions();
    int newKeyArrayLength = dimensionInfo.getNewDictionaryColumnCount();
    long[] keyArray = null;
    if (null != updatedCurrentBlockKeyGenerator) {
      keyArray = updatedCurrentBlockKeyGenerator.getKeyArray(dictionaryKeyArray);
      newKeyArrayLength += keyArray.length;
    }
    long[] keyArrayWithNewAddedColumns = new long[newKeyArrayLength];
    int existingColumnKeyArrayIndex = 0;
    int newKeyArrayIndex = 0;
    for (int i = 0; i < dimensionInfo.getDimensionExists().length; i++) {
      if (CarbonUtil
          .hasEncoding(actualQueryDimensions[i].getDimension().getEncoder(), Encoding.DICTIONARY)) {
        // if dimension exists then add the key array value else add the default value
        if (dimensionInfo.getDimensionExists()[i]) {
          keyArrayWithNewAddedColumns[newKeyArrayIndex++] = keyArray[existingColumnKeyArrayIndex++];
        } else {
          long defaultValueAsLong;
          Object defaultValue = dimensionInfo.getDefaultValues()[i];
          if (null != defaultValue) {
            defaultValueAsLong = ((Integer) defaultValue).longValue();
          } else {
            defaultValueAsLong = (long)CarbonCommonConstants.MEMBER_DEFAULT_VAL_SURROGATE_KEY;
          }
          keyArrayWithNewAddedColumns[newKeyArrayIndex++] = defaultValueAsLong;
        }
      }
    }
    try {
      dictionaryKeyArray = restructuredKeyGenerator.generateKey(keyArrayWithNewAddedColumns);
    } catch (KeyGenException e) {
      LOGGER.error(e, e.getMessage());
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
    ProjectionDimension[] actualQueryDimensions = executionInfo.getActualQueryDimensions();
    byte[][] noDictionaryKeyArrayWithNewlyAddedColumns =
        new byte[noDictionaryKeyArray.length + dimensionInfo.getNewNoDictionaryColumnCount()][];
    int existingColumnValueIndex = 0;
    int newKeyArrayIndex = 0;
    for (int i = 0; i < dimensionInfo.getDimensionExists().length; i++) {
      if (!actualQueryDimensions[i].getDimension().hasEncoding(Encoding.DICTIONARY)
          && !actualQueryDimensions[i].getDimension().hasEncoding(Encoding.IMPLICIT)) {
        // if dimension exists then add the byte array value else add the default value
        if (dimensionInfo.getDimensionExists()[i]) {
          noDictionaryKeyArrayWithNewlyAddedColumns[newKeyArrayIndex++] =
              noDictionaryKeyArray[existingColumnValueIndex++];
        } else {
          byte[] newColumnDefaultValue = null;
          Object defaultValue = dimensionInfo.getDefaultValues()[i];
          if (null != defaultValue) {
            newColumnDefaultValue = ((UTF8String) defaultValue).getBytes();
          } else if (actualQueryDimensions[i].getDimension().getDataType() == DataTypes.STRING) {
            newColumnDefaultValue =
                UTF8String.fromString(CarbonCommonConstants.MEMBER_DEFAULT_VAL).getBytes();
          } else {
            newColumnDefaultValue = CarbonCommonConstants.EMPTY_BYTE_ARRAY;
          }
          noDictionaryKeyArrayWithNewlyAddedColumns[newKeyArrayIndex++] = newColumnDefaultValue;
        }
      }
    }
    return noDictionaryKeyArrayWithNewlyAddedColumns;
  }
}
