/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.carbondata.query.carbon.aggregator.dimension.impl;

import java.nio.ByteBuffer;

import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.carbon.aggregator.dimension.DimensionDataAggregator;
import org.carbondata.query.carbon.executor.util.QueryUtil;
import org.carbondata.query.carbon.model.DimensionAggregatorInfo;
import org.carbondata.query.carbon.result.AbstractScannedResult;

/**
 * Class which will be used to aggregate the direct dictionary dimension data
 */
public class DirectDictionaryDimensionAggregator implements DimensionDataAggregator {

  /**
   * info object which store information about dimension to be aggregated
   */
  private DimensionAggregatorInfo dimensionAggeragtorInfo;

  /**
   * start index of the aggregator for current dimension column
   */
  private int aggregatorStartIndex;

  /**
   * buffer used to convert mdkey to surrogate key
   */
  private ByteBuffer buffer;

  /**
   * data index in the file
   */
  private int blockIndex;

  /**
   * to store index which will be used to aggregate
   * number type value like sum avg
   */
  private int[] numberTypeAggregatorIndex;

  /**
   * DirectDictionaryGenerator
   */
  private DirectDictionaryGenerator directDictionaryGenerator;

  /**
   * to store index which will be used to aggregate
   * actual type value like max, min, dictinct count
   */
  private int[] actualTypeAggregatorIndex;

  public DirectDictionaryDimensionAggregator(DimensionAggregatorInfo dimensionAggeragtorInfo,
      int aggregatorStartIndex, int blockIndex) {
    this.dimensionAggeragtorInfo = dimensionAggeragtorInfo;
    this.aggregatorStartIndex = aggregatorStartIndex;
    this.blockIndex = blockIndex;
    buffer = ByteBuffer.allocate(CarbonCommonConstants.INT_SIZE_IN_BYTE);
    numberTypeAggregatorIndex =
        QueryUtil.getNumberTypeIndex(this.dimensionAggeragtorInfo.getAggList());
    actualTypeAggregatorIndex =
        QueryUtil.getActualTypeIndex(this.dimensionAggeragtorInfo.getAggList());
    directDictionaryGenerator = DirectDictionaryKeyGeneratorFactory
        .getDirectDictionaryGenerator(this.dimensionAggeragtorInfo.getDim().getDataType());
  }

  /**
   * Below method will be used to aggregate the dimension data
   *
   * @param scannedResult scanned result
   * @param aggeragtor    aggregator used to aggregate the data
   */
  @Override public void aggregateDimensionData(AbstractScannedResult scannedResult,
      MeasureAggregator[] aggeragtor) {
    byte[] dimensionData = scannedResult.getDimensionKey(blockIndex);
    int surrogateKey = CarbonUtil.getSurrogateKey(dimensionData, buffer);
    Object dataBasedOnDataType =
        (long) directDictionaryGenerator.getValueFromSurrogate(surrogateKey) / 1000;

    if (actualTypeAggregatorIndex.length > 0) {
      for (int j = 0; j < actualTypeAggregatorIndex.length; j++) {
        aggeragtor[aggregatorStartIndex + actualTypeAggregatorIndex[j]].agg(dataBasedOnDataType);
      }
    }
    if (numberTypeAggregatorIndex.length > 0) {
      for (int j = 0; j < numberTypeAggregatorIndex.length; j++) {
        aggeragtor[aggregatorStartIndex + numberTypeAggregatorIndex[j]].agg(dataBasedOnDataType);
      }
    }
  }

}
