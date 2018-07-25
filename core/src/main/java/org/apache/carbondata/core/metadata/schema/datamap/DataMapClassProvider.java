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
 * CREATE DATAMAP IF NOT EXISTS dataMapName ON TABLE tableName USING 'DataMapClassProvider'
 * DMPROPERTIES('KEY'='VALUE') AS SELECT COUNT(COL1) FROM tableName
 *
 * Please refer {{org.apache.spark.sql.parser.CarbonSpark2SqlParser}}
 */

public enum DataMapClassProvider {
  PREAGGREGATE("org.apache.carbondata.core.datamap.AggregateDataMap", "preaggregate"),
  TIMESERIES("org.apache.carbondata.core.datamap.TimeSeriesDataMap", "timeseries"),
  LUCENE("org.apache.carbondata.datamap.lucene.LuceneFineGrainDataMapFactory","lucene"),
  BLOOMFILTER("org.apache.carbondata.datamap.bloom.BloomCoarseGrainDataMapFactory", "bloomfilter"),
  MV("org.apache.carbondata.core.datamap.MVDataMap", "mv");

  /**
   * Fully qualified class name of datamap
   */
  private String className;

  /**
   * Short name representation of datamap
   */
  private String shortName;

  DataMapClassProvider(String className, String shortName) {
    this.className = className;
    this.shortName = shortName;
  }

  public String getClassName() {
    return className;
  }

  public String getShortName() {
    return shortName;
  }

  private boolean isEqual(String dataMapClass) {
    return (dataMapClass != null &&
        (dataMapClass.equals(className) ||
        dataMapClass.equalsIgnoreCase(shortName)));
  }

  public static DataMapClassProvider getDataMapProviderOnName(String dataMapShortname) {
    if (TIMESERIES.isEqual(dataMapShortname)) {
      return TIMESERIES;
    } else if (PREAGGREGATE.isEqual(dataMapShortname)) {
      return PREAGGREGATE;
    } else if (LUCENE.isEqual(dataMapShortname)) {
      return LUCENE;
    } else if (BLOOMFILTER.isEqual(dataMapShortname)) {
      return BLOOMFILTER;
    } else {
      throw new UnsupportedOperationException("Unknown datamap provider" + dataMapShortname);
    }
  }
}
