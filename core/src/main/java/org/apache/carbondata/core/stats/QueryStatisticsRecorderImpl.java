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

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;

import static org.apache.carbondata.core.util.CarbonUtil.printLine;

/**
 * Class will be used to record and log the query statistics
 */
public class QueryStatisticsRecorderImpl implements QueryStatisticsRecorder, Serializable {

  private static final LogService LOGGER =
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
   * query with taskd
   */
  private String queryIWthTask;

  public QueryStatisticsRecorderImpl(String queryId) {
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
    String tableInfo = collectExecutorStatistics();
    if (null != tableInfo) {
      LOGGER.statistic(tableInfo);
    }
  }

  /**
   * Below method will parse queryStatisticsMap and put time into table
   */
  public String collectExecutorStatistics() {
    long load_blocks_time = 0;
    long scan_blocks_time = 0;
    long scan_blocks_num = 0;
    long load_dictionary_time = 0;
    long result_size = 0;
    long total_executor_time = 0;
    long total_blocklet = 0;
    long valid_scan_blocklet = 0;
    long valid_pages_blocklet = 0;
    long total_pages = 0;
    long readTime = 0;
    try {
      for (QueryStatistic statistic : queryStatistics) {
        switch (statistic.getMessage()) {
          case QueryStatisticsConstants.LOAD_BLOCKS_EXECUTOR:
            load_blocks_time += statistic.getTimeTaken();
            break;
          case QueryStatisticsConstants.SCAN_BLOCKlET_TIME:
            scan_blocks_time += statistic.getCount();
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
          case QueryStatisticsConstants.TOTAL_BLOCKLET_NUM:
            total_blocklet = statistic.getCount();
            break;
          case QueryStatisticsConstants.VALID_SCAN_BLOCKLET_NUM:
            valid_scan_blocklet = statistic.getCount();
            break;
          case QueryStatisticsConstants.VALID_PAGE_SCANNED:
            valid_pages_blocklet = statistic.getCount();
            break;
          case QueryStatisticsConstants.TOTAL_PAGE_SCANNED:
            total_pages = statistic.getCount();
            break;
          case QueryStatisticsConstants.READ_BLOCKlET_TIME:
            readTime = statistic.getCount();
            break;
          default:
            break;
        }
      }
      String headers =
          "task_id,load_blocks_time,load_dictionary_time,carbon_scan_time,carbon_IO_time, "
              + "total_executor_time,scan_blocks_num,total_blocklets,"
              + "valid_blocklets,total_pages,valid_pages,result_size";
      List<String> values = new ArrayList<String>();
      values.add(queryIWthTask);
      values.add(load_blocks_time + "ms");
      values.add(load_dictionary_time + "ms");
      values.add(scan_blocks_time + "ms");
      values.add(readTime + "ms");
      values.add(total_executor_time + "ms");
      values.add(String.valueOf(scan_blocks_num));
      values.add(String.valueOf(total_blocklet));
      values.add(String.valueOf(valid_scan_blocklet));
      values.add(String.valueOf(total_pages));
      values.add(String.valueOf(valid_pages_blocklet));
      values.add(String.valueOf(result_size));
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
      tableInfo.append(line).append("+").append("\n");
      tableInfo.append(hearLine).append("|").append("\n");
      tableInfo.append(line).append("+").append("\n");
      tableInfo.append(valueLine).append("|").append("\n");
      tableInfo.append(line).append("+").append("\n");
      return "Print query statistic for each task id:" + "\n" + tableInfo.toString();
    } catch (Exception ex) {
      return "Put statistics into table failed, catch exception: " + ex.getMessage();
    }
  }

  public void recordStatisticsForDriver(QueryStatistic statistic, String queryId) {

  }

  public void logStatisticsAsTableDriver() {

  }

}
