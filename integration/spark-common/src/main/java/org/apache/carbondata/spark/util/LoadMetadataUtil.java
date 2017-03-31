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

/**
 * Utility for load data
 */
package org.apache.carbondata.spark.util;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;

public final class LoadMetadataUtil {
  private LoadMetadataUtil() {

  }

  public static boolean isLoadDeletionRequired(String dbName, String tableName) {
    CarbonTable table = CarbonMetadata.getInstance().getCarbonTable(dbName + '_' + tableName);

    String metaDataLocation = table.getMetaDataFilepath();
    LoadMetadataDetails[] details = SegmentStatusManager.readLoadMetadata(metaDataLocation);
    if (details != null && details.length != 0) {
      for (LoadMetadataDetails oneRow : details) {
        if ((CarbonCommonConstants.MARKED_FOR_DELETE.equalsIgnoreCase(oneRow.getLoadStatus())
            || CarbonCommonConstants.COMPACTED.equalsIgnoreCase(oneRow.getLoadStatus()))
            && oneRow.getVisibility().equalsIgnoreCase("true")) {
          return true;
        }
      }
    }

    return false;

  }
}
