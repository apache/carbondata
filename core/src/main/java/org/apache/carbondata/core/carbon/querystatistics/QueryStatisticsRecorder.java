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

import org.apache.commons.lang3.StringUtils;

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

  private QueryStatisticsRecorder() {
    queryStatistics = new ArrayList<QueryStatistic>();
    queryStatisticsMap = new HashMap<String, List<QueryStatistic>>();
  }

  private static QueryStatisticsRecorder carbonLoadStatisticsImplInstance =
      new QueryStatisticsRecorder();

  public static QueryStatisticsRecorder getInstance() {
    return carbonLoadStatisticsImplInstance;
  }
  /**
   * list for statistics to record time taken
   * by each phase of the query for example aggregation
   * scanning,block loading time etc.
   */
  private List<QueryStatistic> queryStatistics;

  private HashMap<String, List<QueryStatistic>> queryStatisticsMap;

  /**
   * Below method will be used to add the statistics
   *
   * @param statistic
   */
  public synchronized void recordStatistics(QueryStatistic statistic) {
    queryStatistics.add(statistic);
    // refresh query Statistics Map
    String key = statistic.getQueryId();
    if (!StringUtils.isEmpty(key)) {
      // 240954528274124_0 and 240954528274124 is the same query id
      key = key.substring(0, 15);
    }
    if (queryStatisticsMap.get(key) != null) {
      queryStatisticsMap.get(key).add(statistic);
    } else {
      List<QueryStatistic> newQueryStatistics = new ArrayList<QueryStatistic>();
      newQueryStatistics.add(statistic);
      queryStatisticsMap.put(key, newQueryStatistics);
    }
  }

  /**
   * Below method will be used to log the statistic
   */
  public void logStatistics() {
    for (QueryStatistic statistic : queryStatistics) {
      LOGGER.statistic(statistic.getStatistics());
    }
  }

  /**
   * Below method will be used to show statistic log as table
   */
  public void logStatisticsTable() {
    String tableInfo = putStatisticsIntoTable();
    if (null != tableInfo) {
      LOGGER.statistic(tableInfo);
    }
  }

  /**
   * Below method will parse queryStatisticsMap and put time into table
   */
  public String putStatisticsIntoTable() {
    for (String key: queryStatisticsMap.keySet()) {
      try {
        // TODO: get the finished query, and print Statistics
        if (queryStatisticsMap.get(key).size() > 8) {
          String jdbc_connection_time = "";
          String sql_parse_time = "";
          String load_meta_time = "";
          String block_identification_time = "";
          String schedule_time = "";
          String driver_part_time = "";
          String executor_part_time = "";
          String load_index_time = "";
          String scan_data_time = "";
          String dictionary_load_time = "";
          String prepare_result_time = "";
          String print_result_time = "";
          String total_query_time = "";
          // get statistic time from the QueryStatistic
          for (QueryStatistic statistic : queryStatisticsMap.get(key)) {
            switch (statistic.getMessage()) {
              case QueryStatisticsCommonConstants.JDBC_CONNECTION:
                jdbc_connection_time += statistic.getTimeTaken();
                break;
              case QueryStatisticsCommonConstants.SQL_PARSE:
                sql_parse_time += statistic.getTimeTaken();
                break;
              case QueryStatisticsCommonConstants.LOAD_META:
                load_meta_time += statistic.getTimeTaken();
                break;
              case QueryStatisticsCommonConstants.BLOCK_IDENTIFICATION:
                block_identification_time += statistic.getTimeTaken();
                break;
              case QueryStatisticsCommonConstants.SCHEDULE_TIME:
                schedule_time += statistic.getTimeTaken();
                break;
              case QueryStatisticsCommonConstants.DRIVER_PART:
                driver_part_time += statistic.getTimeTaken();
                break;
              case QueryStatisticsCommonConstants.EXECUTOR_PART:
                executor_part_time += statistic.getTimeTaken();
                break;
              case QueryStatisticsCommonConstants.LOAD_INDEX:
                load_index_time += statistic.getTimeTaken();
                break;
              case QueryStatisticsCommonConstants.SCAN_DATA:
                scan_data_time += statistic.getTimeTaken();
                break;
              case QueryStatisticsCommonConstants.DICTIONARY_LOAD:
                dictionary_load_time += statistic.getTimeTaken();
                break;
              case QueryStatisticsCommonConstants.PREPARE_RESULT:
                prepare_result_time += statistic.getTimeTaken();
                break;
              case QueryStatisticsCommonConstants.PRINT_RESULT:
                print_result_time += statistic.getTimeTaken();
                break;
              case QueryStatisticsCommonConstants.TOTAL_TIME:
                total_query_time += statistic.getTimeTaken();
                break;
              default:
                break;
            }
          }
          // structure the query statistics info table
          int len1 = 8;
          int len2 = 20;
          int len3 = 21;
          int len4 = 22;
          String col1 = QueryStatistic.sameCharBuilder("-", len1);
          String col2 = QueryStatistic.sameCharBuilder("-", len2);
          String col3 = QueryStatistic.sameCharBuilder("-", len3);
          String col4 = QueryStatistic.sameCharBuilder("-", len4);

          StringBuilder tableInfo = new StringBuilder();
          // table header
          tableInfo.append("+" + col1 + "+" + col2 + "+" + col3 + "+" + col4 + "+" + "\n");
          tableInfo.append("|" +
              QueryStatistic.sameCharBuilder(" ", (len1 - "Module".length())) + "Module" + "|" +
              QueryStatistic.sameCharBuilder(" ", (len2 - "Operation Step".length())) +
              "Operation Step" + "|" +
              QueryStatistic.sameCharBuilder(" ", (len3 + len4 + 1 - "Query Cost".length())) +
              "Query Cost" + "|" + "\n");
          // jdbc
          tableInfo.append("+" + col1 + "+" + col2 + "+" + col3 + "+" + col4 + "+" + "\n");
          tableInfo.append("|" +
              QueryStatistic.sameCharBuilder(" ", (len1 + len2 + 1 - "JDBC connection".length())) +
              "JDBC connection" + "|" +
              QueryStatistic.sameCharBuilder(" ", (len3 - jdbc_connection_time.length())) +
              jdbc_connection_time + "|" +
              QueryStatistic.sameCharBuilder(" ", (len4 - jdbc_connection_time.length())) +
              jdbc_connection_time + "|" + "\n");
          // driver part
          tableInfo.append("+" + col1 + "+" + col2 + "+" + col3 + "+" + col4 + "+" + "\n");
          tableInfo.append("|" +
              QueryStatistic.sameCharBuilder(" ", len1) + "|" +
              QueryStatistic.sameCharBuilder(" ", (len2 - "SQL parse".length())) + "SQL parse"
              + "|" +
              QueryStatistic.sameCharBuilder(" ", len3) + "|" +
              QueryStatistic.sameCharBuilder(" ", (len4 - sql_parse_time.length())) +
              sql_parse_time + "|" + "\n");
          tableInfo.append("|" + QueryStatistic.sameCharBuilder(" ", len1) +
              "+" + col2 + "+" + QueryStatistic.sameCharBuilder(" ", len3) +
              "+" + col4 + "+" + "\n");
          tableInfo.append("|" +
              QueryStatistic.sameCharBuilder(" ", len1) + "|" +
              QueryStatistic.sameCharBuilder(" ", (len2 - "Connect time to Hive".length())) +
              "Connect time to Hive" + "|" +
              QueryStatistic.sameCharBuilder(" ", len3) + "|" +
              QueryStatistic.sameCharBuilder(" ", (len4 - load_meta_time.length())) +
              load_meta_time + "|" + "\n");
          tableInfo.append("|" +
              QueryStatistic.sameCharBuilder(" ", (len1 - "Driver".length())) + "Driver" + "|" +
              QueryStatistic.sameCharBuilder(" ", (len2 - "metastore carbon".length())) +
              "metastore carbon" + "|" +
              QueryStatistic.sameCharBuilder(" ", (len3 - driver_part_time.length())) +
              driver_part_time + "|" +
              QueryStatistic.sameCharBuilder(" ", len4) + "|" + "\n");
          tableInfo.append("|" + QueryStatistic.sameCharBuilder(" ", len1) +
              "+" + col2 + "+" + QueryStatistic.sameCharBuilder(" ", len3) +
              "+" + col4 + "+" + "\n");
          tableInfo.append("|" +
              QueryStatistic.sameCharBuilder(" ", (len1 - "Part".length())) + "Part" + "|" +
              QueryStatistic.sameCharBuilder(" ", (len2 - "Block identification".length())) +
              "Block identification" + "|" +
              QueryStatistic.sameCharBuilder(" ", len3) + "|" +
              QueryStatistic.sameCharBuilder(" ", (len4 - block_identification_time.length())) +
              block_identification_time + "|" + "\n");
          tableInfo.append("|" + QueryStatistic.sameCharBuilder(" ", len1) +
              "+" + col2 + "+" + QueryStatistic.sameCharBuilder(" ", len3) +
              "+" + col4 + "+" + "\n");
          tableInfo.append("|" +
              QueryStatistic.sameCharBuilder(" ", len1) + "|" +
              QueryStatistic.sameCharBuilder(" ", (len2 - "Schedule task to".length())) +
              "Schedule task to" + "|" +
              QueryStatistic.sameCharBuilder(" ", len3) + "|" +
              QueryStatistic.sameCharBuilder(" ", (len4 - schedule_time.length())) +
              schedule_time + "|" + "\n");
          tableInfo.append("|" +
              QueryStatistic.sameCharBuilder(" ", len1) + "|" +
              QueryStatistic.sameCharBuilder(" ", (len2 - "executor".length())) +
              "executor" + "|" +
              QueryStatistic.sameCharBuilder(" ", len3) + "|" +
              QueryStatistic.sameCharBuilder(" ", len4) + "|" + "\n");
          // executor part
          tableInfo.append("+" + col1 + "+" + col2 + "+" + col3 + "+" + col4 + "+" + "\n");
          tableInfo.append("|" +
              QueryStatistic.sameCharBuilder(" ", len1) + "|" +
              QueryStatistic.sameCharBuilder(" ", (len2 - "Load index".length())) +
              "Load index" + "|" +
              QueryStatistic.sameCharBuilder(" ", len3) + "|" +
              QueryStatistic.sameCharBuilder(" ", (len4 - load_index_time.length())) +
              load_index_time + "|" + "\n");
          tableInfo.append("|" + QueryStatistic.sameCharBuilder(" ", len1) +
              "+" + col2 + "+" + QueryStatistic.sameCharBuilder(" ", len3) +
              "+" + col4 + "+" + "\n");
          tableInfo.append("|" +
              QueryStatistic.sameCharBuilder(" ", (len1 - "Executor".length())) +
              "Executor" + "|" +
              QueryStatistic.sameCharBuilder(" ", (len2 - "Scan(include read".length())) +
              "Scan(include read" + "|" +
              QueryStatistic.sameCharBuilder(" ", len3) + "|" +
              QueryStatistic.sameCharBuilder(" ", (len4 - scan_data_time.length())) +
              scan_data_time + "|" + "\n");
          tableInfo.append("|" +
              QueryStatistic.sameCharBuilder(" ", (len1 - "Part".length())) + "Part" + "|" +
              QueryStatistic.sameCharBuilder(" ", (len2 - "data from file&".length())) +
              "data from file&" + "|" +
              QueryStatistic.sameCharBuilder(" ", len3) + "|" +
              QueryStatistic.sameCharBuilder(" ", len4) + "|" + "\n");
          tableInfo.append("|" +
              QueryStatistic.sameCharBuilder(" ", len1) + "|" +
              QueryStatistic.sameCharBuilder(" ", (len2 - "process".length())) + "process" + "|" +
              QueryStatistic.sameCharBuilder(" ", (len3 - executor_part_time.length())) +
              executor_part_time + "|" +
              QueryStatistic.sameCharBuilder(" ", len4) + "|" + "\n");
          tableInfo.append("|" + QueryStatistic.sameCharBuilder(" ", len1) +
              "+" + col2 + "+" + QueryStatistic.sameCharBuilder(" ", len3) +
              "+" + col4 + "+" + "\n");
          tableInfo.append("|" +
              QueryStatistic.sameCharBuilder(" ", len1) + "|" +
              QueryStatistic.sameCharBuilder(" ", (len2 - "Dictionary load".length())) +
              "Dictionary load" + "|" +
              QueryStatistic.sameCharBuilder(" ", len3) + "|" +
              QueryStatistic.sameCharBuilder(" ", (len4 - dictionary_load_time.length())) +
              dictionary_load_time + "|" + "\n");
          tableInfo.append("|" + QueryStatistic.sameCharBuilder(" ", len1) +
              "+" + col2 + "+" + QueryStatistic.sameCharBuilder(" ", len3) +
              "" + "+" + col4 + "+" + "\n");
          tableInfo.append("|" +
              QueryStatistic.sameCharBuilder(" ", len1) + "|" +
              QueryStatistic.sameCharBuilder(" ", (len2 - "Prepare query result".length())) +
              "Prepare query result" + "|" +
              QueryStatistic.sameCharBuilder(" ", len3) + "|" +
              QueryStatistic.sameCharBuilder(" ", (len4 - prepare_result_time.length())) +
              prepare_result_time + "|" + "\n");
          tableInfo.append("|" +
              QueryStatistic.sameCharBuilder(" ", len1) + "|" +
              QueryStatistic.sameCharBuilder(" ", (len2 - "and give to spark".length())) +
              "and give to spark" + "|" +
              QueryStatistic.sameCharBuilder(" ", len3) + "|" +
              QueryStatistic.sameCharBuilder(" ", len4) + "|" + "\n");
          // print
          tableInfo.append("+" + col1 + "+" + col2 + "+" + col3 + "+" + col4 + "+" + "\n");
          tableInfo.append("|" +
              QueryStatistic.sameCharBuilder(" ",
                  (len1 + len2 + 1 - "Print result at beeline".length())) +
              "Print result at beeline" + "|" +
              QueryStatistic.sameCharBuilder(" ", (len3 - print_result_time.length())) +
              print_result_time + "|" +
              QueryStatistic.sameCharBuilder(" ", (len4 - print_result_time.length())) +
              print_result_time + "|" + "\n");
          // total
          tableInfo.append("+" + col1 + "+" + col2 + "+" + col3 + "+" + col4 + "+" + "\n");
          tableInfo.append("|" +
              QueryStatistic.sameCharBuilder(" ", (len1 + len2 + 1 - "Total".length())) +
              "Total" + "|" +
              QueryStatistic.sameCharBuilder(" ", (len3 - total_query_time.length())) +
              total_query_time + "|" +
              QueryStatistic.sameCharBuilder(" ", (len4 - total_query_time.length())) +
              total_query_time + "|" + "\n");
          tableInfo.append("+" + col1 + "+" + col2 + "+" + col3 + "+" + col4 + "+" + "\n");

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
}
