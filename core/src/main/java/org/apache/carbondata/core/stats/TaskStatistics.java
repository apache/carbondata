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
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.List;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.core.util.CarbonUtil;

@InterfaceAudience.Developer("stats")
@InterfaceStability.Evolving
public class TaskStatistics implements Serializable {

  private static final Column[] columns = {
      new Column("query_id", "query id"),
      new Column("task_id", "spark task id"),
      new Column("start_time", "start time"),
      new Column("total_time", QueryStatisticsConstants.EXECUTOR_PART, true),
      new Column("load_blocks_time", QueryStatisticsConstants.LOAD_BLOCKS_EXECUTOR, true),
      new Column("load_dictionary_time", QueryStatisticsConstants.LOAD_DICTIONARY, true),
      new Column("carbon_scan_time", QueryStatisticsConstants.SCAN_BLOCKlET_TIME),
      new Column("carbon_IO_time", QueryStatisticsConstants.READ_BLOCKlET_TIME),
      new Column("scan_blocks_num", QueryStatisticsConstants.SCAN_BLOCKS_NUM),
      new Column("total_blocklets", QueryStatisticsConstants.TOTAL_BLOCKLET_NUM),
      new Column("scanned_blocklets", QueryStatisticsConstants.BLOCKLET_SCANNED_NUM),
      new Column("valid_blocklets", QueryStatisticsConstants.VALID_SCAN_BLOCKLET_NUM),
      new Column("total_pages", QueryStatisticsConstants.TOTAL_PAGE_SCANNED),
      new Column("scanned_pages", QueryStatisticsConstants.PAGE_SCANNED),
      new Column("valid_pages", QueryStatisticsConstants.VALID_PAGE_SCANNED),
      new Column("result_size", QueryStatisticsConstants.RESULT_SIZE),
      new Column("key_column_filling_time", QueryStatisticsConstants.KEY_COLUMN_FILLING_TIME),
      new Column("measure_filling_time", QueryStatisticsConstants.MEASURE_FILLING_TIME),
      new Column("page_uncompress_time", QueryStatisticsConstants.PAGE_UNCOMPRESS_TIME),
      new Column("result_preparation_time", QueryStatisticsConstants.RESULT_PREP_TIME)
  };

  private static final int numOfColumns = columns.length;

  private String queryId;

  private long[] values = new long[numOfColumns];

  private long fileSize;

  private String[] files;

  TaskStatistics(String queryId, long taskId) {
    this.queryId = queryId;
    this.values[1] = taskId;
  }

  public TaskStatistics(String queryId, long[] values, long fileSize, String[] files) {
    this.queryId = queryId;
    this.values = values;
    this.fileSize = fileSize;
    this.files = files;
  }

  public TaskStatistics build(long startTime, List<QueryStatistic> queryStatistics) {
    this.values[2] = startTime;
    for (QueryStatistic statistic : queryStatistics) {
      if (statistic.getMessage() != null) {
        for (int columnIndex = 3; columnIndex <= numOfColumns - 1; columnIndex++) {
          if (columns[columnIndex].comment.equals(statistic.getMessage())) {
            if (columns[columnIndex].isDuration) {
              values[columnIndex] += statistic.getTimeTaken();
            } else {
              values[columnIndex] += statistic.getCount();
            }
            break;
          }
        }
      }
    }
    return this;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    printStatisticTable(Collections.singletonList(this), builder, "");
    return builder.toString();
  }

  public static void printStatisticTable(List<TaskStatistics> stats, StringBuilder builder,
      String indent) {
    int numOfRows = stats.size();
    int numOfColumns = columns.length;

    // header as string[]
    String[] header = new String[numOfColumns];
    for (int columnIndex = 0; columnIndex < numOfColumns; columnIndex++) {
      header[columnIndex] = columns[columnIndex].name;
    }

    // convert rows to string[][]
    String[][] rows = new String[numOfRows][];
    for (int rowIndex = 0; rowIndex < numOfRows; rowIndex++) {
      rows[rowIndex] = stats.get(rowIndex).convertValueToString();
    }

    CarbonUtil.logTable(builder, header, rows, indent);
  }

  private String[] convertValueToString() {
    String[] valueStrings = new String[numOfColumns];
    valueStrings[0] = queryId;
    for (int i = 1; i < numOfColumns; i++) {
      if (columns[i].isDuration) {
        valueStrings[i] = values[i] + "ms";
      } else {
        valueStrings[i] = String.valueOf(values[i]);
      }
    }
    valueStrings[2] = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(values[2]);
    return valueStrings;
  }

  private static class Column {
    String name;
    String comment;
    boolean isDuration;

    Column(String name, String comment) {
      this.name = name;
      this.comment = comment;
      this.isDuration = false;
    }

    Column(String name, String comment, boolean isDuration) {
      this(name, comment);
      this.isDuration = isDuration;
    }
  }

  public String getQueryId() {
    return queryId;
  }

  public long[] getValues() {
    return values;
  }

  public long getFileSize() {
    return fileSize;
  }

  public String[] getFiles() {
    return files;
  }
}
