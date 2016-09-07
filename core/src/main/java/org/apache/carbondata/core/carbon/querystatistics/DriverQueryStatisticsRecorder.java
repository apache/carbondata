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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;

import static org.apache.carbondata.core.util.CarbonUtil.printLine;

import org.apache.commons.lang3.StringUtils;

/**
 * Class will be used to record and log the query statistics
 */
public class DriverQueryStatisticsRecorder {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(DriverQueryStatisticsRecorder.class.getName());

  /**
   * singleton QueryStatisticsRecorder for driver
   */
  private Map<String, List<QueryStatistic>> queryStatisticsMap;

  /**
   * lock for log statistics table
   */
  private static final Object lock = new Object();

  private DriverQueryStatisticsRecorder() {
    // use ConcurrentHashMap, it is thread-safe
    queryStatisticsMap = new ConcurrentHashMap<String, List<QueryStatistic>>();
  }

  private static DriverQueryStatisticsRecorder carbonLoadStatisticsImplInstance =
      new DriverQueryStatisticsRecorder();

  public static DriverQueryStatisticsRecorder getInstance() {
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
    synchronized (lock) {
      Iterator<Map.Entry<String, List<QueryStatistic>>> entries =
              queryStatisticsMap.entrySet().iterator();
      while (entries.hasNext()) {
        Map.Entry<String, List<QueryStatistic>> entry = entries.next();
        String queryId = entry.getKey();
        // clear the unknown query statistics
        if(StringUtils.isEmpty(queryId)) {
          entries.remove();
        } else {
          // clear the timeout query statistics
          long interval = System.nanoTime() - Long.parseLong(queryId);
          if (interval > QueryStatisticsConstants.CLEAR_STATISTICS_TIMEOUT) {
            entries.remove();
          } else {
            // print sql_parse_t,load_meta_t,block_allocation_t,block_identification_t
            // or just print block_allocation_t,block_identification_t
            if (entry.getValue().size() >= 2) {
              String tableInfo = collectDriverStatistics(entry.getValue(), queryId);
              if (null != tableInfo) {
                LOGGER.statistic(tableInfo);
                // clear the statistics that has been printed
                entries.remove();
              }
            }
          }
        }
      }
    }
  }

  /**
   * Below method will parse queryStatisticsMap and put time into table
   */
  public String collectDriverStatistics(List<QueryStatistic> statisticsList, String queryId) {
    String sql_parse_time = "";
    String load_meta_time = "";
    String block_allocation_time = "";
    String block_identification_time = "";
    long driver_part_time_tmp = 0L;
    long driver_part_time_tmp2 = 0L;
    String splitChar = " ";
    try {
      // get statistic time from the QueryStatistic
      for (QueryStatistic statistic : statisticsList) {
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
            driver_part_time_tmp2 += statistic.getTimeTaken();
            break;
          case QueryStatisticsConstants.BLOCK_IDENTIFICATION:
            block_identification_time += statistic.getTimeTaken() + splitChar;
            driver_part_time_tmp += statistic.getTimeTaken();
            driver_part_time_tmp2 += statistic.getTimeTaken();
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
          printLine(" ", (len3 + len4 + 1 - "Query Cost".length())) + "Query Cost" + "|" + "\n");
      tableInfo.append(line).append("\n");
      // print sql_parse_t,load_meta_t,block_allocation_t,block_identification_t
      if (!StringUtils.isEmpty(sql_parse_time) &&
          !StringUtils.isEmpty(load_meta_time) &&
          !StringUtils.isEmpty(block_allocation_time) &&
          !StringUtils.isEmpty(block_identification_time)) {
        tableInfo.append("|" + printLine(" ", len1) + "|" +
            printLine(" ", (len2 - "SQL parse".length())) + "SQL parse" + "|" +
            printLine(" ", len3) + "|" +
            printLine(" ", (len4 - sql_parse_time.length())) + sql_parse_time + "|" + "\n");
        tableInfo.append(line2).append("\n");
        tableInfo.append("|" + printLine(" ", (len1 - "Driver".length())) + "Driver" + "|" +
            printLine(" ", (len2 - "Load meta data".length())) + "Load meta data" + "|" +
            printLine(" ", (len3 - driver_part_time.length())) + driver_part_time + "|" +
            printLine(" ", (len4 - load_meta_time.length())) +
            load_meta_time + "|" + "\n");
        tableInfo.append(line2).append("\n");
        tableInfo.append("|" + printLine(" ", (len1 - "Part".length())) + "Part" + "|" +
            printLine(" ", (len2 - "Block allocation".length())) + "Block allocation" + "|" +
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

        // show query statistic as "query id" + "table"
        return "Print query statistic for query id: " + queryId + "\n" + tableInfo.toString();
      } else if (!StringUtils.isEmpty(block_allocation_time) &&
          !StringUtils.isEmpty(block_identification_time)) {
        // when we can't get sql parse time, we only print the last two
        driver_part_time = driver_part_time_tmp2 + splitChar;
        tableInfo.append("|" + printLine(" ", (len1 - "Driver".length())) + "Driver" + "|" +
            printLine(" ", (len2 - "Block allocation".length())) + "Block allocation" + "|" +
            printLine(" ", (len3 - driver_part_time.length())) + driver_part_time + "|" +
            printLine(" ", (len4 - block_allocation_time.length())) +
            block_allocation_time + "|" + "\n");
        tableInfo.append(line2).append("\n");
        tableInfo.append("|" +
            printLine(" ", (len1 - "Part".length())) + "Part" + "|" +
            printLine(" ", (len2 - "Block identification".length())) +
            "Block identification" + "|" +
            printLine(" ", len3) + "|" +
            printLine(" ", (len4 - block_identification_time.length())) +
            block_identification_time + "|" + "\n");
        tableInfo.append(line).append("\n");

        // show query statistic as "query id" + "table"
        return "Print query statistic for query id: " + queryId + "\n" + tableInfo.toString();
      }

      return null;
    } catch (Exception ex) {
      return "Put statistics into table failed, catch exception: " + ex.getMessage();
    }
  }
}
