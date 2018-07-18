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

import org.apache.carbondata.core.metadata.schema.datamap.DataMapClassProvider;

public class DataMapSchemaFactory {
  public static final DataMapSchemaFactory INSTANCE = new DataMapSchemaFactory();

  /**
   * Below class will be used to get data map schema object
   * based on class name
   * @param providerName
   * @return data map schema
   */
  public DataMapSchema getDataMapSchema(String dataMapName, String providerName) {
    if (providerName.equalsIgnoreCase(DataMapClassProvider.PREAGGREGATE.toString()) || providerName
        .equalsIgnoreCase(DataMapClassProvider.PREAGGREGATE.getClassName())) {
      return new AggregationDataMapSchema(dataMapName, providerName);
    } else if (providerName.equalsIgnoreCase(DataMapClassProvider.TIMESERIES.toString())
        || providerName.equalsIgnoreCase(DataMapClassProvider.TIMESERIES.getClassName())) {
      return new AggregationDataMapSchema(dataMapName, providerName);
    } else {
      return new DataMapSchema(dataMapName, providerName);
    }
  }
}
