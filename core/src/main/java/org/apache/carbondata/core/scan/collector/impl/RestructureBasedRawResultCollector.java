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

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.model.ProjectionDimension;
import org.apache.carbondata.core.scan.model.ProjectionMeasure;
import org.apache.carbondata.core.scan.result.BlockletScannedResult;
import org.apache.carbondata.core.scan.wrappers.ByteArrayWrapper;
import org.apache.carbondata.core.stats.QueryStatistic;
import org.apache.carbondata.core.stats.QueryStatisticsConstants;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.DataTypeUtil;

/**
 * It is not a collector it is just a scanned result holder.
 */
public class RestructureBasedRawResultCollector extends RawBasedResultCollector {

  public RestructureBasedRawResultCollector(BlockExecutionInfo blockExecutionInfos) {
    super(blockExecutionInfos);
  }

  /**
   * This method will add a record both key and value to list object
   * it will keep track of how many record is processed, to handle limit scenario
   */
  @Override
  public List<Object[]> collectResultInRow(BlockletScannedResult scannedResult, int batchSize) {
    long startTime = System.currentTimeMillis();
    List<Object[]> listBasedResult = new ArrayList<>(batchSize);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2099
    ProjectionMeasure[] queryMeasures = executionInfo.getActualQueryMeasures();
    // scan the record and add to list
    scanAndFillData(scannedResult, batchSize, listBasedResult, queryMeasures);
    // re-fill dictionary and no dictionary key arrays for the newly added columns
    if (dimensionInfo.isDictionaryColumnAdded()) {
      fillDictionaryKeyArrayBatchWithLatestSchema(listBasedResult);
    }
    if (dimensionInfo.isNoDictionaryColumnAdded()) {
      fillNoDictionaryKeyArrayBatchWithLatestSchema(listBasedResult);
    }
    QueryStatistic resultPrepTime = queryStatisticsModel.getStatisticsTypeAndObjMap()
        .get(QueryStatisticsConstants.RESULT_PREP_TIME);
    resultPrepTime.addCountStatistic(QueryStatisticsConstants.RESULT_PREP_TIME,
        resultPrepTime.getCount() + (System.currentTimeMillis() - startTime));
    return listBasedResult;
  }

  /**
   * This method will fill the dictionary key array with newly added dictionary columns if any
   *
   * @param rows
   * @return
   */
  private void fillDictionaryKeyArrayBatchWithLatestSchema(List<Object[]> rows) {
    for (Object[] row : rows) {
      ByteArrayWrapper byteArrayWrapper = (ByteArrayWrapper) row[0];
      byte[] dictKeyArray = byteArrayWrapper.getDictionaryKey();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2099
      ProjectionDimension[] actualQueryDimensions = executionInfo.getActualQueryDimensions();
      int newKeyArrayLength = dimensionInfo.getNewDictionaryColumnCount();
      long[] keyArray = null;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3684
      if (executionInfo.getDataBlock().getSegmentProperties().getNumberOfDictDimensions() > 0) {
        keyArray = ByteUtil.convertBytesToLongArray(dictKeyArray);
        newKeyArrayLength += keyArray.length;
      }
      long[] keyArrayWithNewAddedColumns = new long[newKeyArrayLength];
      int existingColumnKeyArrayIndex = 0;
      int newKeyArrayIndex = 0;
      for (int i = 0; i < dimensionInfo.getDimensionExists().length; i++) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3674
        if (actualQueryDimensions[i].getDimension().getDataType() == DataTypes.DATE) {
          // if dimension exists then add the key array value else add the default value
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2489
          if (dimensionInfo.getDimensionExists()[i] && null != keyArray && 0 != keyArray.length) {
            keyArrayWithNewAddedColumns[newKeyArrayIndex++] =
                keyArray[existingColumnKeyArrayIndex++];
          } else {
            long defaultValueAsLong;
            Object defaultValue = dimensionInfo.getDefaultValues()[i];
            if (null != defaultValue) {
              defaultValueAsLong = ((Integer) defaultValue).longValue();
            } else {
              defaultValueAsLong = (long) CarbonCommonConstants.MEMBER_DEFAULT_VAL_SURROGATE_KEY;
            }
            keyArrayWithNewAddedColumns[newKeyArrayIndex++] = defaultValueAsLong;
          }
        }
      }
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3684
      dictKeyArray = ByteUtil.convertLongArrayToBytes(keyArrayWithNewAddedColumns);
      byteArrayWrapper.setDictionaryKey(dictKeyArray);
    }
  }

  /**
   * This method will fill the no dictionary byte array with newly added no dictionary columns
   *
   * @param rows
   * @return
   */
  private void fillNoDictionaryKeyArrayBatchWithLatestSchema(List<Object[]> rows) {
    for (Object[] row : rows) {
      ByteArrayWrapper byteArrayWrapper = (ByteArrayWrapper) row[0];
      byte[][] noDictKeyArray = byteArrayWrapper.getNoDictionaryKeys();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2099
      ProjectionDimension[] actualQueryDimensions = executionInfo.getActualQueryDimensions();
      byte[][] noDictionaryKeyArrayWithNewlyAddedColumns =
          new byte[noDictKeyArray.length + dimensionInfo.getNewNoDictionaryColumnCount()][];
      int existingColumnValueIndex = 0;
      int newKeyArrayIndex = 0;
      for (int i = 0; i < dimensionInfo.getDimensionExists().length; i++) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3674
        if (actualQueryDimensions[i].getDimension().getDataType() != DataTypes.DATE
            && !actualQueryDimensions[i].getDimension().hasEncoding(Encoding.IMPLICIT)) {
          // if dimension exists then add the byte array value else add the default value
          if (dimensionInfo.getDimensionExists()[i]) {
            noDictionaryKeyArrayWithNewlyAddedColumns[newKeyArrayIndex++] =
                noDictKeyArray[existingColumnValueIndex++];
          } else {
            byte[] newColumnDefaultValue = null;
            Object defaultValue = dimensionInfo.getDefaultValues()[i];
            if (null != defaultValue) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2163
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2164
              newColumnDefaultValue = (byte[]) defaultValue;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1539
            } else if (actualQueryDimensions[i].getDimension().getDataType() == DataTypes.STRING) {
              newColumnDefaultValue =
                  DataTypeUtil.getDataTypeConverter().convertFromByteToUTF8Bytes(
                      CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY);
            } else {
              newColumnDefaultValue = CarbonCommonConstants.EMPTY_BYTE_ARRAY;
            }
            noDictionaryKeyArrayWithNewlyAddedColumns[newKeyArrayIndex++] = newColumnDefaultValue;
          }
        }
      }
      byteArrayWrapper.setNoDictionaryKeys(noDictionaryKeyArrayWithNewlyAddedColumns);
    }
  }
}
