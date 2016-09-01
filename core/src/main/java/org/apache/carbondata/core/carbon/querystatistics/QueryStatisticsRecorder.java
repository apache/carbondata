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
package org.apache.carbondata.core.carbon.querystatistics;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;

import static org.apache.carbondata.core.util.CarbonUtil.printLine;

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

  /**
   * lock for log statistics table
   */
  private static final Object lock = new Object();

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

  /**
   * Below method will be used to show statistic log as table
   */
  public void logStatisticsAsTableExecutor() {
    synchronized (lock) {
      String tableInfo = collectExecutorStatistics();
      if (null != tableInfo) {
        LOGGER.statistic(tableInfo);
      }
    }
  }

  /**
   * Below method will parse queryStatisticsMap and put time into table
   */
  public String collectExecutorStatistics() {
    String load_blocks_time = "";
    String scan_blocks_time = "";
    String scan_blocks_num = "";
    String load_dictionary_time = "";
    String result_size = "";
    String total_executor_time = "";
    String splitChar = " ";
    try {
      for (QueryStatistic statistic : queryStatistics) {
        switch (statistic.getMessage()) {
          case QueryStatisticsConstants.LOAD_BLOCKS_EXECUTOR:
            load_blocks_time += statistic.getTimeTaken() + splitChar;
            break;
          case QueryStatisticsConstants.SCAN_BLOCKS_TIME:
            scan_blocks_time += statistic.getTimeTaken() + splitChar;
            break;
          case QueryStatisticsConstants.SCAN_BLOCKS_NUM:
            scan_blocks_num += statistic.getCount() + splitChar;
            break;
          case QueryStatisticsConstants.LOAD_DICTIONARY:
            load_dictionary_time += statistic.getTimeTaken() + splitChar;
            break;
          case QueryStatisticsConstants.RESULT_SIZE:
            result_size += statistic.getCount() + " ";
            break;
          case QueryStatisticsConstants.EXECUTOR_PART:
            total_executor_time += statistic.getTimeTaken() + splitChar;
            break;
          default:
            break;
        }
      }
      String headers = "task_id,load_blocks_time,load_dictionary_time,scan_blocks_time," +
          "scan_blocks_num,result_size,total_executor_time";
      List<String> values = new ArrayList<String>();
      values.add(queryIWthTask);
      values.add(load_blocks_time);
      values.add(load_dictionary_time);
      values.add(scan_blocks_time);
      values.add(scan_blocks_num);
      values.add(result_size);
      values.add(total_executor_time);
      StringBuilder tableInfo = new StringBuilder();
      String[] columns = headers.split(",");
      String line = "";
      String hearLine = "";
      String valueLine = "";
      for (int i = 0; i < columns.length; i++) {
        int len = Math.max(columns[i].length(), values.get(i).length());
        line += "+" + printLine("-", len);
        hearLine += "|" + printLine(" ", len - columns[i].length()) + columns[i];
        valueLine += "|" + printLine(" ", len - values.get(i).length()) + values.get(i);
      }
      // struct table info
      tableInfo.append(line + "+").append("\n");
      tableInfo.append(hearLine + "|").append("\n");
      tableInfo.append(line + "+").append("\n");
      tableInfo.append(valueLine + "|").append("\n");
      tableInfo.append(line + "+").append("\n");
      return "Print query statistic for each task id:" + "\n" + tableInfo.toString();
    } catch (Exception ex) {
      return "Put statistics into table failed, catch exception: " + ex.getMessage();
    }
  }

}
