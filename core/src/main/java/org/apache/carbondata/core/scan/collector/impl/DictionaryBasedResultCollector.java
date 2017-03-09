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

import java.nio.ByteBuffer;
import java.util.*;

import org.apache.carbondata.core.cache.update.BlockletLevelDeleteDeltaDataCache;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.filter.GenericQueryType;
import org.apache.carbondata.core.scan.model.QueryDimension;
import org.apache.carbondata.core.scan.model.QueryMeasure;
import org.apache.carbondata.core.scan.result.AbstractScannedResult;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeUtil;

import org.apache.commons.lang3.ArrayUtils;

/**
 * It is not a collector it is just a scanned result holder.
 */
public class DictionaryBasedResultCollector extends AbstractScannedResultCollector {

  public DictionaryBasedResultCollector(BlockExecutionInfo blockExecutionInfos) {
    super(blockExecutionInfos);
  }

  /**
   * This method will add a record both key and value to list object
   * it will keep track of how many record is processed, to handle limit scenario
   */
  @Override public List<Object[]> collectData(AbstractScannedResult scannedResult, int batchSize) {

    List<Object[]> listBasedResult = new ArrayList<>(batchSize);
    boolean isMsrsPresent = measureDatatypes.length > 0;

    QueryDimension[] queryDimensions = tableBlockExecutionInfos.getQueryDimensions();
    List<Integer> dictionaryIndexes = new ArrayList<Integer>();
    for (int i = 0; i < queryDimensions.length; i++) {
      if (queryDimensions[i].getDimension().hasEncoding(Encoding.DICTIONARY) || queryDimensions[i]
          .getDimension().hasEncoding(Encoding.DIRECT_DICTIONARY)) {
        dictionaryIndexes.add(queryDimensions[i].getDimension().getOrdinal());
      }
    }
    int[] primitive = ArrayUtils.toPrimitive(dictionaryIndexes.toArray(
        new Integer[dictionaryIndexes.size()]));
    Arrays.sort(primitive);
    int[] actualIndexInSurrogateKey = new int[dictionaryIndexes.size()];
    int index = 0;
    for (int i = 0; i < queryDimensions.length; i++) {
      if (queryDimensions[i].getDimension().hasEncoding(Encoding.DICTIONARY) || queryDimensions[i]
          .getDimension().hasEncoding(Encoding.DIRECT_DICTIONARY)) {
        actualIndexInSurrogateKey[index++] =
            Arrays.binarySearch(primitive, queryDimensions[i].getDimension().getOrdinal());
      }
    }

    QueryMeasure[] queryMeasures = tableBlockExecutionInfos.getQueryMeasures();
    BlockletLevelDeleteDeltaDataCache deleteDeltaDataCache =
        scannedResult.getDeleteDeltaDataCache();
    Map<Integer, GenericQueryType> comlexDimensionInfoMap =
        tableBlockExecutionInfos.getComlexDimensionInfoMap();
    boolean[] dictionaryEncodingArray = CarbonUtil.getDictionaryEncodingArray(queryDimensions);
    boolean[] directDictionaryEncodingArray =
        CarbonUtil.getDirectDictionaryEncodingArray(queryDimensions);
    boolean[] implictColumnArray = CarbonUtil.getImplicitColumnArray(queryDimensions);
    boolean[] complexDataTypeArray = CarbonUtil.getComplexDataTypeArray(queryDimensions);
    int dimSize = queryDimensions.length;
    boolean isDimensionsExist = dimSize > 0;
    int[] order = new int[dimSize + queryMeasures.length];
    for (int i = 0; i < dimSize; i++) {
      order[i] = queryDimensions[i].getQueryOrder();
    }
    for (int i = 0; i < queryMeasures.length; i++) {
      order[i + dimSize] = queryMeasures[i].getQueryOrder();
    }
    // scan the record and add to list
    int rowCounter = 0;
    int dictionaryColumnIndex = 0;
    int noDictionaryColumnIndex = 0;
    int complexTypeColumnIndex = 0;
    int[] surrogateResult;
    String[] noDictionaryKeys;
    byte[][] complexTypeKeyArray;

    DirectDictionaryGenerator[] directDictionaryGenerators = new DirectDictionaryGenerator[dimSize];
    for (int i = 0; i < dimSize; i++) {
      directDictionaryGenerators[i] = DirectDictionaryKeyGeneratorFactory
              .getDirectDictionaryGenerator(queryDimensions[i].getDimension().getDataType());
    }
    while (scannedResult.hasNext() && rowCounter < batchSize) {
      Object[] row = new Object[dimSize + queryMeasures.length];
      if (isDimensionsExist) {
        surrogateResult = scannedResult.getDictionaryKeyIntegerArray();
        noDictionaryKeys = scannedResult.getNoDictionaryKeyStringArray();
        complexTypeKeyArray = scannedResult.getComplexTypeKeyArray();
        dictionaryColumnIndex = 0;
        noDictionaryColumnIndex = 0;
        complexTypeColumnIndex = 0;
        for (int i = 0; i < dimSize; i++) {
          if (!dictionaryEncodingArray[i]) {
            if (implictColumnArray[i]) {
              if (CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID
                  .equals(queryDimensions[i].getDimension().getColName())) {
                row[order[i]] = DataTypeUtil.getDataBasedOnDataType(
                    scannedResult.getBlockletId() + CarbonCommonConstants.FILE_SEPARATOR
                        + scannedResult.getCurrenrRowId(), DataType.STRING);
              } else {
                row[order[i]] = DataTypeUtil
                    .getDataBasedOnDataType(scannedResult.getBlockletId(), DataType.STRING);
              }
            } else {
              row[order[i]] = DataTypeUtil
                  .getDataBasedOnDataType(noDictionaryKeys[noDictionaryColumnIndex++],
                      queryDimensions[i].getDimension().getDataType());
            }
          } else if (directDictionaryEncodingArray[i]) {
            if (directDictionaryGenerators[i] != null) {
              row[order[i]] = directDictionaryGenerators[i].getValueFromSurrogate(
                  surrogateResult[actualIndexInSurrogateKey[dictionaryColumnIndex++]]);
            }
          } else if (complexDataTypeArray[i]) {
            row[order[i]] = comlexDimensionInfoMap
                .get(queryDimensions[i].getDimension().getOrdinal())
                .getDataBasedOnDataTypeFromSurrogates(
                    ByteBuffer.wrap(complexTypeKeyArray[complexTypeColumnIndex++]));
          } else {
            row[order[i]] = surrogateResult[actualIndexInSurrogateKey[dictionaryColumnIndex++]];
          }
        }

      } else {
        scannedResult.incrementCounter();
      }
      if (null != deleteDeltaDataCache && deleteDeltaDataCache
          .contains(scannedResult.getCurrenrRowId())) {
        continue;
      }
      if (isMsrsPresent) {
        Object[] msrValues = new Object[measureDatatypes.length];
        fillMeasureData(msrValues, 0, scannedResult);
        for (int i = 0; i < msrValues.length; i++) {
          row[order[i + dimSize]] = msrValues[i];
        }
      }
      listBasedResult.add(row);
      rowCounter++;
    }
    return listBasedResult;
  }

}
