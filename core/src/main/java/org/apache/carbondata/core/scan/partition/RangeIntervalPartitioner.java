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

package org.apache.carbondata.core.scan.partition;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.schema.PartitionInfo;
import org.apache.carbondata.core.util.CarbonProperties;

/**
 * Range Interval Partitioner
 */

public class RangeIntervalPartitioner implements Partitioner {

  private int numPartitions;
  private RangeIntervalComparator comparator;
  private List<Object> boundsList;
  private String intervalType;
  private DataType partitionColumnDataType;

  private SimpleDateFormat timestampFormatter = new SimpleDateFormat(CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
          CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT));

  private SimpleDateFormat dateFormatter = new SimpleDateFormat(CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
          CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT));

  public RangeIntervalPartitioner(PartitionInfo partitionInfo) {
    List<String> values = partitionInfo.getRangeIntervalInfo();
    partitionColumnDataType = partitionInfo.getColumnSchemaList().get(0).getDataType();
    numPartitions = values.size() - 1;
    boundsList.add(0, null);
    for (int i = 1; i <= numPartitions; i++) {
      boundsList.add(i, PartitionUtil.getDataBasedOnDataType(values.get(i), partitionColumnDataType,
          timestampFormatter, dateFormatter));
    }
    comparator = new RangeIntervalComparator();
    intervalType = values.get(numPartitions);
  }

  @Override public int numPartitions() {
    return numPartitions + 1;
  }

  @Override public int getPartition(Object key) {
    int partitionIndex = -1;
    Object lastBound = boundsList.get(numPartitions);
    if (key == null) {
      return 0;
    } else {
      for (int i = 1; i <= numPartitions; i++) {
        if (comparator.compareTo(key, boundsList.get(i))) {
          return i;
        }
      }
      switch (intervalType.toLowerCase()) {
        case "year":
          partitionIndex = getDynamicPartitionForYear(key, lastBound);
          break;
        case "month":
          partitionIndex = getDynamicPartitionForMonth(key, lastBound);
          break;
        case "week":
          partitionIndex = getDynamicPartitionForWeek(key, lastBound);
          break;
        case "day":
          partitionIndex = getDynamicPartitionForDay(key, lastBound);
          break;
        case "hour":
          partitionIndex = getDynamicPartitionForHour(key, lastBound);
          break;
        default:
          partitionIndex = -1;
      }
      return partitionIndex;
    }
  }

  public int getDynamicPartitionForYear(Object key, Object lastBound) {
    int partitionId = -1;
    Calendar lastCalendar = Calendar.getInstance();
    Calendar keyCal = Calendar.getInstance();
    keyCal.setTimeInMillis((long)key);
    lastCalendar.setTimeInMillis((long)lastBound);
    for (int addYear = 1;;addYear++) {
      lastCalendar.add(Calendar.YEAR, 1);
      boundsList.add(numPartitions + addYear, lastCalendar.getTimeInMillis());
      ++numPartitions;
      if (keyCal.compareTo(lastCalendar) == -1) {
        partitionId = numPartitions + addYear;
        break;
      }
    }
    return partitionId;
  }

  public int getDynamicPartitionForMonth(Object key, Object lastBound) {
    int partitionId = -1;
    Calendar lastCalendar = Calendar.getInstance();
    Calendar keyCal = Calendar.getInstance();
    keyCal.setTimeInMillis((long)key);
    lastCalendar.setTimeInMillis((long)lastBound);
    for (int addMonth = 1;;addMonth++) {
      lastCalendar.add(Calendar.MONTH, 1);
      boundsList.add(numPartitions + addMonth, lastCalendar.getTimeInMillis());
      ++numPartitions;
      if (keyCal.compareTo(lastCalendar) == -1) {
        partitionId = numPartitions + addMonth;
        break;
      }
    }
    return partitionId;
  }

  public int getDynamicPartitionForWeek(Object key, Object lastBound) {
    int partitionId = -1;
    Calendar lastCalendar = Calendar.getInstance();
    Calendar keyCal = Calendar.getInstance();
    keyCal.setTimeInMillis((long)key);
    lastCalendar.setTimeInMillis((long)lastBound);
    for (int addWeek = 1;;addWeek++) {
      lastCalendar.add(Calendar.WEEK_OF_MONTH, 1);
      boundsList.add(numPartitions + addWeek, lastCalendar.getTimeInMillis());
      ++numPartitions;
      if (keyCal.compareTo(lastCalendar) == -1) {
        partitionId = numPartitions + addWeek;
        break;
      }
    }
    return partitionId;
  }

  public int getDynamicPartitionForDay(Object key, Object lastBound) {
    int partitionId = -1;
    Calendar lastCalendar = Calendar.getInstance();
    Calendar keyCal = Calendar.getInstance();
    keyCal.setTimeInMillis((long)key);
    lastCalendar.setTimeInMillis((long)lastBound);
    for (int addDay = 1;;addDay++) {
      lastCalendar.add(Calendar.DAY_OF_WEEK, 1);
      boundsList.add(numPartitions + addDay, lastCalendar.getTimeInMillis());
      ++numPartitions;
      if (keyCal.compareTo(lastCalendar) == -1) {
        partitionId = numPartitions + addDay;
        break;
      }
    }
    return partitionId;
  }

  public int getDynamicPartitionForHour(Object key , Object lastBound) {
    int partitionId = -1;
    Calendar lastCalendar = Calendar.getInstance();
    Calendar keyCal = Calendar.getInstance();
    keyCal.setTimeInMillis((long)key);
    lastCalendar.setTimeInMillis((long)lastBound);
    for (int addHour = 1;;addHour++) {
      lastCalendar.add(Calendar.HOUR_OF_DAY, 1);
      boundsList.add(numPartitions + addHour, lastCalendar.getTimeInMillis());
      ++numPartitions;
      if (keyCal.compareTo(lastCalendar) == -1) {
        partitionId = numPartitions + addHour;
        break;
      }
    }
    return partitionId;
  }

  public Timestamp date2Timestamp(Date date, SimpleDateFormat dateFormatter) {
    String time = dateFormatter.format(date);
    Timestamp ts = Timestamp.valueOf(time);
    return ts;
  }

}

class RangeIntervalComparator implements Serializable {
  public boolean compareTo(Object key1, Object key2) {
    return (long) key1 - (long) key2 < 0;
  }
}
