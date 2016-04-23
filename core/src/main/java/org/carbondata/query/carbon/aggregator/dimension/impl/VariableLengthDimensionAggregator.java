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

import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.carbon.aggregator.dimension.DimensionDataAggregator;
import org.carbondata.query.carbon.model.DimensionAggregatorInfo;
import org.carbondata.query.carbon.result.AbstractScannedResult;
import org.carbondata.query.carbon.util.DataTypeUtil;

/**
 * Class which will be used to aggregate the Variable length dimension data
 */
public class VariableLengthDimensionAggregator implements DimensionDataAggregator {

  /**
   * info object which store information about dimension to be aggregated
   */
  private DimensionAggregatorInfo dimensionAggeragtorInfo;

  /**
   * default which was added for new dimension after restructuring for the
   * older blocks
   */
  private Object defaultValue;

  /**
   * index of the aggregator
   */
  private int aggreagtorStartIndex;

  /**
   * index of block in file
   */
  private int blockIndex;

  public VariableLengthDimensionAggregator(DimensionAggregatorInfo dimensionAggeragtorInfo,
      Object defaultValue, int aggregatorStartIndex, int blockIndex) {
    this.dimensionAggeragtorInfo = dimensionAggeragtorInfo;
    this.aggreagtorStartIndex = aggregatorStartIndex;
    this.blockIndex = blockIndex;
  }

  /**
   * Below method will be used to aggregate the variable length dimension data
   *
   * @param scannedResult scanned result
   * @param aggeragtor    aggregator used to aggregate the data
   */
  @Override public void aggregateDimensionData(AbstractScannedResult scannedResult,
      MeasureAggregator[] aggeragtor) {

    String data = null;
    if (defaultValue != null) {
      data = (String) defaultValue;
    } else {
      data = new String(scannedResult.getDimensionKey(blockIndex));
      if (CarbonCommonConstants.MEMBER_DEFAULT_VAL.equals(data)) {
        return;
      }
    }
    Object dataBasedOnDataType =
        DataTypeUtil.getDataBasedOnDataType(data, dimensionAggeragtorInfo.getDim().getDataType());
    if (null == dataBasedOnDataType) {
      return;
    }
    for (int i = 0; i < dimensionAggeragtorInfo.getAggList().size(); i++) {
      aggeragtor[aggreagtorStartIndex + i].agg(dataBasedOnDataType);
    }
  }

}
