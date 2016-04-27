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
package org.carbondata.query.carbon.aggregator.measure;

import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.carbon.executor.infos.BlockExecutionInfo;
import org.carbondata.query.carbon.result.AbstractScannedResult;

/**
 * Interface for measure aggregation
 */
public abstract class MeasureDataAggregator {

  /**
   * measure ordinal selected in query
   */
  protected int[] measuresOrdinal;

  public MeasureDataAggregator(BlockExecutionInfo tableBlockExecutionInfos) {
    // get the measure ordinal
    measuresOrdinal = tableBlockExecutionInfos.getAggregatorInfo().getMeasureOrdinals();
  }

  /**
   * Below method will be used to aggregate the measures
   *
   * @param scannedRsult scanned result
   * @param aggrgeator   aggregator selected for each measure
   */
  public abstract void aggregateMeasure(AbstractScannedResult scannedResult,
      MeasureAggregator[] aggrgeator);
}
