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

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CarbonProperties;

public class IndexSchemaFactory {
  public static final IndexSchemaFactory INSTANCE = new IndexSchemaFactory();

  public static IndexSchemaStorageProvider getIndexSchemaStorageProvider() {
    String provider = CarbonProperties.getDataMapStorageProvider();
    switch (provider) {
      case CarbonCommonConstants.CARBON_DATAMAP_SCHEMA_STORAGE_DATABASE:
        return new DatabaseIndexSchemaStorageProvider();
      case CarbonCommonConstants.CARBON_DATAMAP_SCHEMA_STORAGE_DISK:
      default:
        return new DiskBasedIndexSchemaStorageProvider(
            CarbonProperties.getInstance().getSystemFolderLocation());
    }
  }
}
