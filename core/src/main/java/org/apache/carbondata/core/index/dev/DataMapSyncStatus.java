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

package org.apache.carbondata.core.index.dev;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.index.IndexUtil;
import org.apache.carbondata.core.index.status.MVSegmentStatusUtil;
import org.apache.carbondata.core.metadata.schema.table.IndexSchema;
import org.apache.carbondata.core.metadata.schema.table.RelationIdentifier;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.path.CarbonTablePath;

/**
 * Interface to check whether datamap can be enabled
 */
@InterfaceAudience.Developer("Index")
public abstract class DataMapSyncStatus {

  /**
   * This method checks if main table and datamap table are synchronised or not. If synchronised
   * return true to enable the datamap
   *
   * @param indexSchema of datamap to be disabled or enabled
   * @return flag to enable or disable datamap
   * @throws IOException
   */
  public static boolean canDataMapBeEnabled(IndexSchema indexSchema) throws IOException {
    boolean isDataMapInSync = true;
    String metaDataPath =
        CarbonTablePath.getMetadataPath(indexSchema.getRelationIdentifier().getTablePath());
    LoadMetadataDetails[] dataMapLoadMetadataDetails =
        SegmentStatusManager.readLoadMetadata(metaDataPath);
    Map<String, List<String>> dataMapSegmentMap = new HashMap<>();
    for (LoadMetadataDetails loadMetadataDetail : dataMapLoadMetadataDetails) {
      if (loadMetadataDetail.getSegmentStatus() == SegmentStatus.SUCCESS) {
        Map<String, List<String>> segmentMap =
            MVSegmentStatusUtil.getSegmentMap(loadMetadataDetail.getExtraInfo());
        if (dataMapSegmentMap.isEmpty()) {
          dataMapSegmentMap.putAll(segmentMap);
        } else {
          for (Map.Entry<String, List<String>> entry : segmentMap.entrySet()) {
            if (null != dataMapSegmentMap.get(entry.getKey())) {
              dataMapSegmentMap.get(entry.getKey()).addAll(entry.getValue());
            }
          }
        }
      }
    }
    List<RelationIdentifier> parentTables = indexSchema.getParentTables();
    for (RelationIdentifier parentTable : parentTables) {
      List<String> mainTableValidSegmentList =
          IndexUtil.getMainTableValidSegmentList(parentTable);
      if (!mainTableValidSegmentList.isEmpty() && !dataMapSegmentMap.isEmpty()) {
        isDataMapInSync = dataMapSegmentMap.get(
            parentTable.getDatabaseName() + CarbonCommonConstants.POINT + parentTable
                .getTableName()).containsAll(mainTableValidSegmentList);
      } else if (dataMapSegmentMap.isEmpty() && !mainTableValidSegmentList.isEmpty()) {
        isDataMapInSync = false;
      }
    }
    return isDataMapInSync;
  }
}
