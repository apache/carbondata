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

import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;

import static org.apache.carbondata.core.metadata.schema.datamap.DataMapProvider.PREAGGREGATE;
import static org.apache.carbondata.core.metadata.schema.datamap.DataMapProvider.TIMESERIES;

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
   * Return a DataMapProvider instance for specified dataMapSchema.
   */
  public DataMapProvider getDataMapProvider(DataMapSchema dataMapSchema) {
    DataMapProvider provider;
    if (dataMapSchema.getProviderName().equalsIgnoreCase(PREAGGREGATE.toString())) {
      provider = new PreAggregateDataMapProvider();
    } else if (dataMapSchema.getProviderName().equalsIgnoreCase(TIMESERIES.toString())) {
      provider = new TimeseriesDataMapProvider();
    } else {
      provider = new IndexDataMapProvider();
    }
    return provider;
  }

}
