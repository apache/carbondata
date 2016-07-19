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
package org.carbondata.core.carbon.querystatistics;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;

/**
 * Class will be used to record and log the query statistics
 */
public class QueryStatisticsRecorder implements Serializable {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(QueryStatisticsRecorder.class.getName());
  /**
   * serialization version
   */
  private static final long serialVersionUID = -5719752001674467864L;

  /**
   * list for statistics to record time taken
   * by each phase of the query for example aggregation
   * scanning,block loading time etc.
   */
  private List<QueryStatistic> queryStatistics;

  /**
   * query with taskd
   */
  private String queryIWthTask;

  public QueryStatisticsRecorder(String queryId) {
    queryStatistics = new ArrayList<QueryStatistic>();
    this.queryIWthTask = queryId;
  }

  /**
   * Below method will be used to add the statistics
   *
   * @param statistic
   */
  public synchronized void recordStatistics(QueryStatistic statistic) {
    queryStatistics.add(statistic);
  }

  /**
   * Below method will be used to log the statistic
   */
  public void logStatistics() {
    for (QueryStatistic statistic : queryStatistics) {
      LOGGER.statistic(statistic.getStatistics(queryIWthTask));
    }
  }
}
