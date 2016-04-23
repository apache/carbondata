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
package org.carbondata.query.carbon.executor;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.query.carbon.executor.impl.AggregationQueryExecutor;
import org.carbondata.query.carbon.executor.impl.CountStarQueryExecutor;
import org.carbondata.query.carbon.executor.impl.DetailQueryExecutor;
import org.carbondata.query.carbon.executor.impl.DetailWithOrderByQueryExecutor;
import org.carbondata.query.carbon.executor.impl.FunctionQueryExecutor;
import org.carbondata.query.carbon.model.QueryModel;
import org.carbondata.query.util.CarbonEngineLogEvent;

/**
 * Factory class to get the query executor from RDD
 * This will return the executor based on query type
 */
public class QueryExecutorFactory {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(QueryExecutorFactory.class.getName());

  public static QueryExecutor getQueryExecutor(QueryModel queryModel) {
    // if all the other query property like query dimension dimension
    // aggregation expression are empty and is count start query is true
    // with one measure on which counter will be executed
    // then its a counter start query
    if (queryModel.isCountStarQuery() && null == queryModel.getFilterEvaluatorTree()
        && queryModel.getQueryDimension().size() < 1 && queryModel.getQueryDimension().size() < 2
        && queryModel.getDimAggregationInfo().size() < 1
        && queryModel.getExpressions().size() == 0) {
      LOGGER.info(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, "Count(*) query: ");
      return new CountStarQueryExecutor();
    }
    // if all the query property is empty then is a function query like
    // count(1)
    // in that case we need to return empty record of size number of records
    // present in the carbon data file
    else if (null == queryModel.getFilterEvaluatorTree()
        && queryModel.getQueryDimension().size() == 0 && queryModel.getQueryMeasures().size() == 0
        && queryModel.getDimAggregationInfo().size() == 0
        && queryModel.getExpressions().size() == 0) {
      LOGGER.info(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, "Function query: ");
      return new FunctionQueryExecutor();
    }
    // if not a detail query then it is a aggregation query
    else if (!queryModel.isDetailQuery()) {
      LOGGER.info(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, "Aggergation query: ");
      return new AggregationQueryExecutor();
    }
    // to handle detail with order by query
    else if (queryModel.isDetailQuery() && queryModel.getSortDimension().size() > 0) {
      LOGGER.info(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, "Detail with order by query: ");
      return new DetailWithOrderByQueryExecutor();
    } else {
      // detail query
      LOGGER.info(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, "Detail query: ");
      return new DetailQueryExecutor();
    }
  }
}
