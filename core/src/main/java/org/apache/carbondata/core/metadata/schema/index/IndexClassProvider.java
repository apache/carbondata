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

package org.apache.carbondata.core.metadata.schema.index;

/**
 * Please refer {{org.apache.spark.sql.parser.CarbonSpark2SqlParser}}
 */
public enum IndexClassProvider {
  LUCENE("org.apache.carbondata.datamap.lucene.LuceneFineGrainIndexFactory", "lucene"),
  BLOOMFILTER("org.apache.carbondata.datamap.bloom.BloomCoarseGrainIndexFactory", "bloomfilter"),
  MV("org.apache.carbondata.core.datamap.MVDataMap", "mv");

  /**
   * Fully qualified class name of index
   */
  private String className;

  /**
   * Short name representation of index
   */
  private String shortName;

  IndexClassProvider(String className, String shortName) {
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

  public static IndexClassProvider get(String indexProviderName) {
    if (LUCENE.isEqual(indexProviderName)) {
      return LUCENE;
    } else if (BLOOMFILTER.isEqual(indexProviderName)) {
      return BLOOMFILTER;
    } else {
      throw new UnsupportedOperationException("Unknown index provider" + indexProviderName);
    }
  }
}
