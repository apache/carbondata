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

package org.apache.carbondata.core.metadata.schema.datamap;

/**
 * type for create datamap
 * The syntax of datamap creation is as follows.
 * CREATE DATAMAP IF NOT EXISTS dataMapName ON TABLE tableName USING 'DataMapProvider'
 * DMPROPERTIES('KEY'='VALUE') AS SELECT COUNT(COL1) FROM tableName
 *
 * Please refer {{org.apache.spark.sql.parser.CarbonSpark2SqlParser}}
 */

public enum DataMapProvider {
  PREAGGREGATE("org.apache.carbondata.core.datamap.AggregateDataMap", "preaggregate"),
  TIMESERIES("org.apache.carbondata.core.datamap.TimeSeriesDataMap", "timeseries");

  /**
   * Fully qualified class name of datamap
   */
  private String className;

  /**
   * Short name representation of datamap
   */
  private String shortName;

  DataMapProvider(String className, String shortName) {
    this.className = className;
    this.shortName = shortName;
  }

  public String getClassName() {
    return className;
  }

  private boolean isEqual(String dataMapClass) {
    return (dataMapClass != null &&
        (dataMapClass.equals(className) ||
        dataMapClass.equalsIgnoreCase(shortName)));
  }

  public static DataMapProvider getDataMapProvider(String dataMapClass) {
    if (TIMESERIES.isEqual(dataMapClass)) {
      return TIMESERIES;
    } else if (PREAGGREGATE.isEqual(dataMapClass)) {
      return PREAGGREGATE;
    } else {
      throw new UnsupportedOperationException("Unknown datamap provider/class " + dataMapClass);
    }
  }
}
