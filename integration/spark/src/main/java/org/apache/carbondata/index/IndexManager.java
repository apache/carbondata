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

package org.apache.carbondata.index;

import org.apache.carbondata.common.exceptions.sql.MalformedIndexCommandException;
import org.apache.carbondata.core.index.CarbonIndexProvider;
import org.apache.carbondata.core.metadata.schema.index.IndexClassProvider;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.IndexSchema;
import org.apache.carbondata.spark.util.CarbonScalaUtil;

import org.apache.spark.sql.SparkSession;

public class IndexManager {

  private static IndexManager INSTANCE;

  private IndexManager() { }

  public static synchronized IndexManager get() {
    if (INSTANCE == null) {
      INSTANCE = new IndexManager();
    }
    return INSTANCE;
  }

  /**
   * Return a IndexClassProvider instance for specified indexSchema.
   */
  public CarbonIndexProvider getIndexProvider(CarbonTable mainTable, IndexSchema indexSchema,
      SparkSession sparkSession) throws MalformedIndexCommandException {
    org.apache.carbondata.core.index.CarbonIndexProvider provider;
    if (indexSchema.getProviderName().equalsIgnoreCase(IndexClassProvider.MV.toString())) {
      provider = (org.apache.carbondata.core.index.CarbonIndexProvider) CarbonScalaUtil
          .createIndexProvider("org.apache.carbondata.mv.extension.MVDataMapProvider",
              sparkSession, mainTable, indexSchema);
    } else {
      provider =
          new org.apache.carbondata.index.IndexProvider(mainTable, indexSchema, sparkSession);
    }
    return provider;
  }

}
