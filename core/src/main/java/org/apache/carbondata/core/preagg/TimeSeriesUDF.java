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
package org.apache.carbondata.core.preagg;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;

/**
 * class for applying timeseries udf
 */
public class TimeSeriesUDF {

  public final List<String> TIMESERIES_FUNCTION = new ArrayList<>();

  // thread local for keeping calender instance
  private ThreadLocal<Calendar> calanderThreadLocal = new ThreadLocal<>();

  /**
   * singleton instance
   */
  public static final TimeSeriesUDF INSTANCE = new TimeSeriesUDF();

  private TimeSeriesUDF() {
    initialize();
  }

  /**
   * Below method will be used to apply udf on data provided
   * Method will work based on below logic.
   * Data: 2016-7-23 01:01:30,10
   * Year Level UDF will return: 2016-1-1 00:00:00,0
   * Month Level UDF will return: 2016-7-1 00:00:00,0
   * Day Level UDF will return: 2016-7-23 00:00:00,0
   * Hour Level UDF will return: 2016-7-23 01:00:00,0
   * Minute Level UDF will return: 2016-7-23 01:01:00,0
   * Second Level UDF will return: 2016-7-23 01:01:30,0
   * If function does not match with any of the above functions
   * it will throw IllegalArgumentException
   *
   * @param data     timestamp data
   * @param function time series function name
   * @return data after applying udf
   */
  public Timestamp applyUDF(Timestamp data, String function) {
    if (null == data) {
      return data;
    }
    initialize();
    Calendar calendar = calanderThreadLocal.get();
    calendar.clear();
    calendar.setTimeInMillis(data.getTime());
    TimeSeriesFunctionEnum timeSeriesFunctionEnum =
        TimeSeriesFunctionEnum.valueOf(function.toUpperCase());
    switch (timeSeriesFunctionEnum) {
      case SECOND:
        calendar.set(Calendar.MILLISECOND, 0);
        break;
      case MINUTE:
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.set(Calendar.SECOND, 0);
        break;
      case HOUR:
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MINUTE, 0);
        break;
      case DAY:
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        break;
      case MONTH:
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        break;
      case YEAR:
        calendar.set(Calendar.MONTH, 1);
        calendar.set(Calendar.DAY_OF_YEAR, 1);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        break;
      default:
        throw new IllegalArgumentException("Invalid timeseries function name: " + function);
    }
    data.setTime(calendar.getTimeInMillis());
    return data;
  }

  /**
   * Below method will be used to initialize the thread local
   */
  private void initialize() {
    if (calanderThreadLocal.get() == null) {
      calanderThreadLocal.set(new GregorianCalendar());
    }
    if (TIMESERIES_FUNCTION.isEmpty()) {
      TIMESERIES_FUNCTION.add("second");
      TIMESERIES_FUNCTION.add("minute");
      TIMESERIES_FUNCTION.add("hour");
      TIMESERIES_FUNCTION.add("day");
      TIMESERIES_FUNCTION.add("month");
      TIMESERIES_FUNCTION.add("year");
    }
  }
}
