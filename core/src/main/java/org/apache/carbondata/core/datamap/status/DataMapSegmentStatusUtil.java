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

package org.apache.carbondata.core.datamap.status;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;
import org.apache.carbondata.core.metadata.schema.table.RelationIdentifier;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatus;

import com.google.gson.Gson;

/**
 * Utility class to get updated segment mapping for datamap table
 */
public class DataMapSegmentStatusUtil {

  /**
   * This method will convert string to map and return segmentMap
   */
  public static Map<String, List<String>> getSegmentMap(String segmentMap) {
    Gson gson = new Gson();
    return gson.fromJson(segmentMap, Map.class);
  }

  /**
   * In case of compaction on dataMap table,this method will merge the segment list of main table
   * and return updated segment mapping
   *
   * @param mergedLoadName      to find which all segments are merged to new compacted segment
   * @param dataMapSchema       of datamap table
   * @param loadMetadataDetails of datamap table
   * @return updated segment map after merging segment list
   */
  public static String getUpdatedSegmentMap(String mergedLoadName, DataMapSchema dataMapSchema,
      LoadMetadataDetails[] loadMetadataDetails) {
    Map<String, List<String>> segmentMapping = new HashMap<>();
    List<RelationIdentifier> relationIdentifiers = dataMapSchema.getParentTables();
    for (RelationIdentifier relationIdentifier : relationIdentifiers) {
      for (LoadMetadataDetails loadMetadataDetail : loadMetadataDetails) {
        if (loadMetadataDetail.getSegmentStatus() == SegmentStatus.COMPACTED) {
          if (mergedLoadName.equalsIgnoreCase(loadMetadataDetail.getMergedLoadName())) {
            if (segmentMapping.isEmpty()) {
              segmentMapping.putAll(getSegmentMap(loadMetadataDetail.getExtraInfo()));
            } else {
              segmentMapping.get(relationIdentifier.getDatabaseName() + CarbonCommonConstants.POINT
                  + relationIdentifier.getTableName()).addAll(
                  getSegmentMap(loadMetadataDetail.getExtraInfo()).get(
                      relationIdentifier.getDatabaseName() + CarbonCommonConstants.POINT
                          + relationIdentifier.getTableName()));
            }
          }
        }
      }
    }
    Gson gson = new Gson();
    return gson.toJson(segmentMapping);
  }
}
