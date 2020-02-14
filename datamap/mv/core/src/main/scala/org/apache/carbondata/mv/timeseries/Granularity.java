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

package org.apache.carbondata.mv.timeseries;

/**
 * type for create datamap
 * The syntax of datamap creation is as follows.
 * CREATE DATAMAP IF NOT EXISTS dataMapName ON TABLE tableName USING 'DataMapProvider'
 * DMPROPERTIES('KEY'='VALUE') AS SELECT COUNT(COL1) FROM tableName
 *
 * Please refer {{org.apache.spark.sql.parser.CarbonSpark2SqlParser}}
 */

public enum Granularity {
  YEAR("year_granularity"),
  MONTH("month_granularity"),
  WEEK("week_granularity"),
  DAY("day_granularity"),
  HOUR("hour_granularity"),
  THIRTY_MINUTE("thirty_minute_granularity"),
  FIFTEEN_MINUTE("fifteen_minute_granularity"),
  TEN_MINUTE("ten_minute_granularity"),
  FIVE_MINUTE("five_minute_granularity"),
  MINUTE("minute_granularity"),
  SECOND("second_granularity");

  private String name;

  Granularity(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

}
