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

package org.apache.carbondata.core.metadata.index;

public enum IndexType {
  LUCENE("org.apache.carbondata.index.lucene.LuceneFineGrainIndexFactory", "lucene"),
  BLOOMFILTER("org.apache.carbondata.index.bloom.BloomCoarseGrainIndexFactory", "bloomfilter"),
  SI("org.apache.carbondata.index.secondary.SecondaryIndexFactory", "si");


  /**
   * Fully qualified class name of index
   */
  private String className;

  /**
   * Short name representation of index
   */
  private String shortName;

  IndexType(String className, String shortName) {
    this.className = className;
    this.shortName = shortName;
  }

  public String getIndexProviderName() {
    return shortName;
  }

  public String getClassName() {
    return className;
  }

  private boolean isEqual(String indexClass) {
    return (indexClass != null &&
        (indexClass.equals(className) ||
            indexClass.equalsIgnoreCase(shortName)));
  }

  public static IndexType get(String indexProviderName) {
    if (LUCENE.isEqual(indexProviderName)) {
      return LUCENE;
    } else if (BLOOMFILTER.isEqual(indexProviderName)) {
      return BLOOMFILTER;
    } else if (SI.isEqual(indexProviderName)) {
      return SI;
    } else {
      throw new UnsupportedOperationException("Unknown index provider" + indexProviderName);
    }
  }
}
