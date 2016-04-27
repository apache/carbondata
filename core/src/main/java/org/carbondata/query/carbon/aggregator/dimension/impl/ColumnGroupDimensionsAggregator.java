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

import java.util.List;

import org.carbondata.core.cache.dictionary.Dictionary;
import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.carbon.aggregator.dimension.DimensionDataAggregator;
import org.carbondata.query.carbon.model.DimensionAggregatorInfo;
import org.carbondata.query.carbon.result.AbstractScannedResult;
import org.carbondata.query.carbon.util.DataTypeUtil;

/**
 * This class will be used to aggregate row group dimension This class will
 * aggregate all the dimensions present in a row group together, as we row group
 * columns data will be stored together so if multiple dimensions of same row
 * group is selected in query, This will be useful when multiple columns of same
 * row group is selected, so unpacking the row group tuple of for the column
 * will be done only once
 */
public class ColumnGroupDimensionsAggregator implements DimensionDataAggregator {

  /**
   * info object which store information about dimension to be aggregated
   */
  protected List<DimensionAggregatorInfo> dimensionAggeragtorInfo;

  /**
   * this will be used to check whether dimension is present in current block
   * or not this will be useful in case of restructuring when new dimension
   * has been added so in older table block it will not present, so default
   * value will be used to aggregate the data.
   */
  protected boolean isDimenionPresentInOldBlock;

  /**
   * row group key generator which will be used to unpack the row group column
   * values
   */
  private KeyGenerator columnGroupKeyGenerator;

  /**
   * index of the block in the file for this column group
   */
  private int blockIndex;

  /**
   * dictinoanryInfo;
   */
  private List<Dictionary> columnDictionary;

  private int aggregatorStartIndexes;

  public ColumnGroupDimensionsAggregator(List<DimensionAggregatorInfo> dimensionAggeragtorInfo,
      KeyGenerator columnGroupKeyGenerator, int blockIndex, List<Dictionary> columnDictionary,
      int aggregatorStartIndexes) {

    this.dimensionAggeragtorInfo = dimensionAggeragtorInfo;
    this.columnDictionary = columnDictionary;
    this.columnGroupKeyGenerator = columnGroupKeyGenerator;
    this.blockIndex = blockIndex;
    this.aggregatorStartIndexes = aggregatorStartIndexes;
  }

  /**
   * Below method will be used to aggregate the dimension data
   *
   * @param scannedResult scanned result
   * @param aggeragtor    aggregator used to aggregate the data
   */
  @Override public void aggregateDimensionData(AbstractScannedResult scannedResult,
      MeasureAggregator[] aggeragtor) {
    long[] surrogateKeyOfColumnGroup = null;
    surrogateKeyOfColumnGroup =
        columnGroupKeyGenerator.getKeyArray(scannedResult.getDimensionKey(blockIndex));
    Object actualData = null;
    int surrogate = 0;
    int aggStartIndex = aggregatorStartIndexes;
    for (int i = 0; i < dimensionAggeragtorInfo.size(); i++) {
      surrogate = (int) surrogateKeyOfColumnGroup[dimensionAggeragtorInfo.get(i).getDim()
          .getColumnGroupOrdinal()];

      if (1 == surrogate) {
        continue;
      }

      actualData = DataTypeUtil
          .getDataBasedOnDataType(columnDictionary.get(i).getDictionaryValueForKey(surrogate),
              dimensionAggeragtorInfo.get(i).getDim().getDataType());
      if (null != actualData) {
        continue;
      }
      for (int j = 0; j < dimensionAggeragtorInfo.get(i).getAggList().size(); j++) {
        aggeragtor[aggStartIndex++].agg(actualData);
      }
    }

  }
}
