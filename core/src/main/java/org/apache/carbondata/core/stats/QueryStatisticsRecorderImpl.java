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
package org.apache.carbondata.core.stats;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.common.logging.impl.StatisticLevel;

import org.apache.log4j.Logger;

/**
 * Class will be used to record and log the query statistics
 */
public class QueryStatisticsRecorderImpl implements QueryStatisticsRecorder, Serializable {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(QueryStatisticsRecorderImpl.class.getName());

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
   * query id with task
   */
  private String queryId;

  public QueryStatisticsRecorderImpl(String queryId) {
    queryStatistics = new ArrayList<QueryStatistic>();
    this.queryId = queryId;
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
      LOGGER.log(StatisticLevel.STATISTIC, statistic.getStatistics(queryId));
    }
  }

  /**
   * Below method will be used to show statistic log as table
   */
  public void logStatisticsForTask(TaskStatistics result) {
    if (null != result) {
      LOGGER.log(StatisticLevel.STATISTIC,
          "Print query statistic for each task id:" + "\n" + result.toString());
    }
  }

  public TaskStatistics statisticsForTask(long taskId, long startTime) {
    try {
      return new TaskStatistics(queryId, taskId).build(startTime, queryStatistics);
    } catch (Exception ex) {
      LOGGER.error(ex);
      return null;
    }
  }

  public void recordStatisticsForDriver(QueryStatistic statistic, String queryId) {

  }

  public void logStatisticsAsTableDriver() {

  }

}
