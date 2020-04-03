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

package org.apache.carbondata.datamap;

import org.apache.carbondata.common.exceptions.sql.MalformedIndexCommandException;
import org.apache.carbondata.core.index.DataMapProvider;
import org.apache.carbondata.core.metadata.schema.datamap.DataMapClassProvider;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.IndexSchema;
import org.apache.carbondata.spark.util.CarbonScalaUtil;

import org.apache.spark.sql.SparkSession;

public class DataMapManager {

  private static DataMapManager INSTANCE;

  private DataMapManager() { }

  public static synchronized DataMapManager get() {
    if (INSTANCE == null) {
      INSTANCE = new DataMapManager();
    }
    return INSTANCE;
  }

  /**
   * Return a DataMapClassProvider instance for specified indexSchema.
   */
  public DataMapProvider getDataMapProvider(CarbonTable mainTable, IndexSchema indexSchema,
      SparkSession sparkSession) throws MalformedIndexCommandException {
    DataMapProvider provider;
    if (indexSchema.getProviderName().equalsIgnoreCase(DataMapClassProvider.MV.toString())) {
      provider = (DataMapProvider) CarbonScalaUtil.createDataMapProvider(
          "org.apache.carbondata.mv.extension.MVDataMapProvider",
              sparkSession,
              mainTable, indexSchema);
    } else {
      provider = new IndexProvider(mainTable, indexSchema, sparkSession);
    }
    return provider;
  }

}
