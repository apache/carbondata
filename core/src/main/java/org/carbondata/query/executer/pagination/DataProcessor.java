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

package org.carbondata.query.executer.pagination;

import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.executer.groupby.GroupByHolder;
import org.carbondata.query.executer.pagination.exception.CarbonPaginationException;

public interface DataProcessor {

  /**
   * Initialize the processor
   *
   * @param model
   */
  void initModel(PaginationModel model) throws CarbonPaginationException;

  /**
   * Process the data
   *
   * @param key
   * @param measures
   */
  void processRow(byte[] key, MeasureAggregator[] measures) throws CarbonPaginationException;

  /**
   * It process the aggregated data which is done in previous step,which is used in measure
   * filters and topN.
   *
   * @param groupByHolder
   */
  void processGroup(GroupByHolder groupByHolder) throws CarbonPaginationException;

  /**
   * finish processing
   */
  void finish() throws CarbonPaginationException;

}
