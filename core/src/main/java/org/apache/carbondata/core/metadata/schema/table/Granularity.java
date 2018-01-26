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

package org.apache.carbondata.core.metadata.schema.table;

/**
 * type for create datamap
 * The syntax of datamap creation is as follows.
 * CREATE DATAMAP IF NOT EXISTS dataMapName ON TABLE tableName USING 'DataMapClassName'
 * DMPROPERTIES('KEY'='VALUE') AS SELECT COUNT(COL1) FROM tableName
 *
 * Please refer {{org.apache.spark.sql.parser.CarbonSpark2SqlParser}}
 */

public enum Granularity {
  YEAR((short) 1, "year_granularity", "year"),
  MONTH((short) 2, "month_granularity", "month"),
  DAY((short) 3, "day_granularity", "day"),
  HOUR((short) 4, "hour_granularity", "hour"),
  MINUTE((short) 5, "minute_granularity", "minute"),
  SECOND((short) 6, "second_granularity", "second");
  private int value;
  private String name;
  private String time;

  Granularity(int value, String name, String time) {
    this.value = value;
    this.name = name;
    this.time = time;
  }

  public int getValue() {
    return value;
  }

  public String getName() {
    return name;
  }

  public String getTime() {
    return time;
  }

  @Override
  public String toString() {
    return this.name;
  }
}
