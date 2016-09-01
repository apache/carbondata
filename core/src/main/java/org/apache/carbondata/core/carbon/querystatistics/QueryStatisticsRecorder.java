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
import java.util.HashMap;
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;

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

  /**
   * singleton QueryStatisticsRecorder for driver
   */
  private HashMap<String, List<QueryStatistic>> queryStatisticsMap;

  private QueryStatisticsRecorder() {
    queryStatisticsMap = new HashMap<String, List<QueryStatistic>>();
  }

  private static QueryStatisticsRecorder carbonLoadStatisticsImplInstance =
      new QueryStatisticsRecorder();

  public static QueryStatisticsRecorder getInstance() {
    return carbonLoadStatisticsImplInstance;
  }

  /**
   * Below method will be used to add the statistics
   *
   * @param statistic
   */
  public synchronized void recordStatisticsForDriver(QueryStatistic statistic, String queryId) {
    // refresh query Statistics Map
    if (queryStatisticsMap.get(queryId) != null) {
      queryStatisticsMap.get(queryId).add(statistic);
    } else {
      List<QueryStatistic> newQueryStatistics = new ArrayList<QueryStatistic>();
      newQueryStatistics.add(statistic);
      queryStatisticsMap.put(queryId, newQueryStatistics);
    }
  }

  /**
   * Below method will be used to show statistic log as table
   */
  public void logStatisticsAsTable(boolean isDriver) {
    String tableInfo = isDriver? collectDriverStatistics(): collectExecutorStatistics();
    if (null != tableInfo) {
      LOGGER.statistic(tableInfo);
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
    try {
      for (QueryStatistic statistic : queryStatistics) {
        switch (statistic.getMessage()) {
          case QueryStatisticsConstants.LOAD_BLOCKS_EXECUTOR:
            load_blocks_time += statistic.getTimeTaken();
            break;
          case QueryStatisticsConstants.SCAN_BLOCKS_TIME:
            scan_blocks_time += statistic.getTimeTaken();
            break;
          case QueryStatisticsConstants.SCAN_BLOCKS_NUM:
            scan_blocks_num += statistic.getCount();
            break;
          case QueryStatisticsConstants.LOAD_DICTIONARY:
            load_dictionary_time += statistic.getTimeTaken();
            break;
          case QueryStatisticsConstants.RESULT_SIZE:
            result_size += statistic.getCount();
            break;
          case QueryStatisticsConstants.EXECUTOR_PART:
            total_executor_time += statistic.getTimeTaken();
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

  /**
   * Below method will parse queryStatisticsMap and put time into table
   */
  public String collectDriverStatistics() {
    for (String key: queryStatisticsMap.keySet()) {
      try {
        // TODO: get the finished query, and print Statistics
        if (queryStatisticsMap.get(key).size() > 1) {
          String sql_parse_time = "";
          String load_meta_time = "";
          String block_allocation_time = "";
          String block_identification_time = "";
          String driver_part_time = "";
          Double driver_part_time_tmp = 0.0;
          // get statistic time from the QueryStatistic
          for (QueryStatistic statistic : queryStatisticsMap.get(key)) {
            switch (statistic.getMessage()) {
              case QueryStatisticsConstants.SQL_PARSE:
                sql_parse_time += statistic.getTimeTaken();
                driver_part_time_tmp += statistic.getTimeTaken();
                break;
              case QueryStatisticsConstants.LOAD_META:
                load_meta_time += statistic.getTimeTaken();
                driver_part_time_tmp += statistic.getTimeTaken();
                break;
              case QueryStatisticsConstants.BLOCK_ALLOCATION:
                block_allocation_time += statistic.getTimeTaken();
                driver_part_time_tmp += statistic.getTimeTaken();
                break;
              case QueryStatisticsConstants.BLOCK_IDENTIFICATION:
                block_identification_time += statistic.getTimeTaken();
                driver_part_time_tmp += statistic.getTimeTaken();
                break;
              default:
                break;
            }
          }
          driver_part_time = driver_part_time_tmp + "";
          // structure the query statistics info table
          StringBuilder tableInfo = new StringBuilder();
          int len1 = 8;
          int len2 = 20;
          int len3 = 21;
          int len4 = 22;
          String line = "+" + printLine("-", len1) + "+" + printLine("-", len2) + "+" +
              printLine("-", len3) + "+" + printLine("-", len4) + "+";
          String line2 = "|" + printLine(" ", len1) + "+" + printLine("-", len2) + "+" +
              printLine(" ", len3) + "+" + printLine("-", len4) + "+";
          // table header
          tableInfo.append(line).append("\n");
          tableInfo.append("|" + printLine(" ", (len1 - "Module".length())) + "Module" + "|" +
              printLine(" ", (len2 - "Operation Step".length())) + "Operation Step" + "|" +
              printLine(" ", (len3 + len4 + 1 - "Query Cost".length())) +
              "Query Cost" + "|" + "\n");
          // driver part
          tableInfo.append(line).append("\n");
          tableInfo.append("|" + printLine(" ", len1) + "|" +
              printLine(" ", (len2 - "SQL parse".length())) + "SQL parse" + "|" +
              printLine(" ", len3) + "|" +
              printLine(" ", (len4 - sql_parse_time.length())) + sql_parse_time + "|" + "\n");
          tableInfo.append(line2).append("\n");
          tableInfo.append("|" +printLine(" ", (len1 - "Driver".length())) + "Driver" + "|" +
              printLine(" ", (len2 - "Load meta data".length())) + "Load meta data" + "|" +
              printLine(" ", (len3 - driver_part_time.length())) + driver_part_time + "|" +
              printLine(" ", (len4 - load_meta_time.length())) +
              load_meta_time + "|" + "\n");
          tableInfo.append(line2).append("\n");
          tableInfo.append("|" +
              printLine(" ", (len1 - "Part".length())) + "Part" + "|" +
              printLine(" ", (len2 - "Block allocation".length())) +
              "Block allocation" + "|" +
              printLine(" ", len3) + "|" +
              printLine(" ", (len4 - block_allocation_time.length())) +
              block_allocation_time + "|" + "\n");
          tableInfo.append(line2).append("\n");
          tableInfo.append("|" +
              printLine(" ", len1) + "|" +
              printLine(" ", (len2 - "Block identification".length())) +
              "Block identification" + "|" +
              printLine(" ", len3) + "|" +
              printLine(" ", (len4 - block_identification_time.length())) +
              block_identification_time + "|" + "\n");
          tableInfo.append(line).append("\n");

          // once the statistics be printed, remove it from the map
          queryStatisticsMap.remove(key);
          // show query statistic as "query id" + "table"
          return "Print query statistic for query id: " + key + "\n" + tableInfo.toString();
        }
      } catch (Exception ex) {
        return "Put statistics into table failed, catch exception: " + ex.getMessage();
      }
    }
    return null;
  }

  /**
   * Below method will create string like "***********"
   *
   * @param a
   * @param num
   */
  public static String printLine(String a, int num)
  {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < num; i++) {
      builder.append(a);
    }
    return builder.toString();
  }
}
