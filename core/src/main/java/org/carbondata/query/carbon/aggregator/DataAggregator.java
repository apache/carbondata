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
package org.carbondata.query.carbon.aggregator;

import java.util.List;

import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.carbon.aggregator.dimension.DimensionDataAggregator;
import org.carbondata.query.carbon.aggregator.expression.ExpressionAggregator;
import org.carbondata.query.carbon.aggregator.measure.MeasureDataAggregator;
import org.carbondata.query.carbon.aggregator.measure.impl.FactTableAggregator;
import org.carbondata.query.carbon.executor.infos.BlockExecutionInfo;
import org.carbondata.query.carbon.result.AbstractScannedResult;

/**
 * Class which will be used to aggregate all type of aggregation present in the query
 */
public class DataAggregator {

  /**
   * measure aggergator which will be used to aggregate the measure columns
   * preset in the query
   */
  private MeasureDataAggregator measureAggregator;

  /**
   * dimension data aggregator list which will be used to aggregate the
   * dimension column aggregate function
   */
  private List<DimensionDataAggregator> dimensionDataAggergatorList;

  /**
   * expression aggergator, which will be used to aggregate the expressions
   * present in the query
   */
  private ExpressionAggregator expressionAggregator;

  public DataAggregator(BlockExecutionInfo blockExecutionInfo) {
    measureAggregator = new FactTableAggregator(blockExecutionInfo);
    dimensionDataAggergatorList = blockExecutionInfo.getDimensionAggregator();
    this.expressionAggregator = new ExpressionAggregator(blockExecutionInfo);

  }

  /**
   * Below method will be used to aggregate the data for all type aggregation
   * function present in the query Order of aggregation. 1. Dimension column
   * Aggregation 2. Expression Aggregation 3. Measure column aggregation
   *
   * @param scannedResult scanned result
   * @param aggregators   aggregator
   */
  public void aggregateData(AbstractScannedResult scannedResult, MeasureAggregator[] aggregators) {
    for (int i = 0; i < dimensionDataAggergatorList.size(); i++) {
      dimensionDataAggergatorList.get(i).aggregateDimensionData(scannedResult, aggregators);
    }
    expressionAggregator.aggregateExpression(scannedResult, aggregators);
    measureAggregator.aggregateMeasure(scannedResult, aggregators);
  }
}
