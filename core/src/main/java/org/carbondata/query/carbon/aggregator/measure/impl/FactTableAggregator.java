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
package org.carbondata.query.carbon.aggregator.measure.impl;

import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.carbon.aggregator.measure.MeasureDataAggregator;
import org.carbondata.query.carbon.executor.infos.BlockExecutionInfo;
import org.carbondata.query.carbon.result.AbstractScannedResult;

/**
 * Below class will be used to aggregate the data.
 * This class will be used only for fact table.
 * as aggregate table aggregation logic will be different
 * For example:
 * Restructuring is only supported in fact table
 * In case of aggregate table some of the measure columns
 * will be stored in byte array, this based on the aggregation function
 * applied on that measure
 */
public class FactTableAggregator extends MeasureDataAggregator {

  /**
   * to check whether measure exists in current table block or not this to
   * handle restructuring scenario
   */
  private boolean[] isMeasureExistsInCurrentBlock;

  /**
   * default value of the measures in case of restructuring some measure wont
   * be present in the table so in that default value will be used to
   * aggregate the data for that measure columns
   */
  private Object[] measureDefaultValue;

  /**
   * as measure column will be aggregated at last so this index will be used
   * to get the aggergator
   */
  private short measureColumnStartIndex;

  public FactTableAggregator(BlockExecutionInfo tableBlockExecutionInfos) {
    super(tableBlockExecutionInfos);
    isMeasureExistsInCurrentBlock = tableBlockExecutionInfos.getAggregatorInfo().getMeasureExists();
    measureDefaultValue = tableBlockExecutionInfos.getAggregatorInfo().getDefaultValues();
    measureColumnStartIndex = tableBlockExecutionInfos.getMeasureColumnStartIndex();
  }

  /**
   * Below method will be used to aggregate the measure value
   *
   * @param scannedRsult scanned result;
   * @param aggrgeator   aggregator for aggregation
   */
  @Override public void aggregateMeasure(AbstractScannedResult scannedResult,
      MeasureAggregator[] aggrgeator) {
    for (short i = 0; i < measuresOrdinal.length; i++) {
      // if measure exists is block then pass measure column
      // data chunk to the aggregator
      if (isMeasureExistsInCurrentBlock[i]) {
        aggrgeator[measureColumnStartIndex + i]
            .agg(scannedResult.getMeasureChunk(measuresOrdinal[i]),
                scannedResult.getCurrenrRowId());
      } else {
        // if not then get the default value and use that value in aggregation
        aggrgeator[measureColumnStartIndex + i].agg(measureDefaultValue[i]);
      }
    }
  }

}
