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
public class SingleQueryStatisticsRecorder implements Serializable {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(SingleQueryStatisticsRecorder.class.getName());
  /**
   * serialization version
   */
  private static final long serialVersionUID = -1L;

  /**
   * singleton QueryStatisticsRecorder for driver
   */
  private HashMap<String, List<QueryStatistic>> queryStatisticsMap;

  private SingleQueryStatisticsRecorder() {
    queryStatisticsMap = new HashMap<String, List<QueryStatistic>>();
  }

  private static SingleQueryStatisticsRecorder carbonLoadStatisticsImplInstance =
      new SingleQueryStatisticsRecorder();

  public static SingleQueryStatisticsRecorder getInstance() {
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
  public void logStatisticsAsTableDriver() {
    String tableInfo = collectDriverStatistics();
    if (null != tableInfo) {
      LOGGER.statistic(tableInfo);
    }
  }

  /**
   * Below method will parse queryStatisticsMap and put time into table
   */
  public String collectDriverStatistics() {
    for (String key: queryStatisticsMap.keySet()) {
      try {
        // TODO: get the finished query, and print Statistics
        if (queryStatisticsMap.get(key).size() > 2) {
          String sql_parse_time = "";
          String load_meta_time = "";
          String block_allocation_time = "";
          String block_identification_time = "";
          Double driver_part_time_tmp = 0.0;
          String splitChar = " ";
          // get statistic time from the QueryStatistic
          for (QueryStatistic statistic : queryStatisticsMap.get(key)) {
            switch (statistic.getMessage()) {
              case QueryStatisticsConstants.SQL_PARSE:
                sql_parse_time += statistic.getTimeTaken() + splitChar;
                driver_part_time_tmp += statistic.getTimeTaken();
                break;
              case QueryStatisticsConstants.LOAD_META:
                load_meta_time += statistic.getTimeTaken() + splitChar;
                driver_part_time_tmp += statistic.getTimeTaken();
                break;
              case QueryStatisticsConstants.BLOCK_ALLOCATION:
                block_allocation_time += statistic.getTimeTaken() + splitChar;
                driver_part_time_tmp += statistic.getTimeTaken();
                break;
              case QueryStatisticsConstants.BLOCK_IDENTIFICATION:
                block_identification_time += statistic.getTimeTaken() + splitChar;
                driver_part_time_tmp += statistic.getTimeTaken();
                break;
              default:
                break;
            }
          }
          String driver_part_time = driver_part_time_tmp + splitChar;
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
