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

public enum DataMapClassName {
  PREAGGREGATE((short) 1, "preaggregate"),
  TIMESERIES((short) 2, "timeseries");
  private int value;
  private String name;

  DataMapClassName(int value, String name) {
    this.value = value;
    this.name = name;
  }

  public int getValue() {
    return value;
  }

  public String getName() {
    return name;
  }

  @Override
  public String toString() {
    return this.name;
  }
}
