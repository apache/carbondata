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

package org.apache.carbondata.index.secondary;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.carbondata.core.index.dev.IndexModel;

import org.apache.hadoop.conf.Configuration;

/**
 * Secondary Index Model. it is used to initialize the Secondary Index Coarse Grain Index
 */
public class SecondaryIndexModel extends IndexModel {
  private final String indexName; // Secondary Index name
  private final String currentSegmentId; // Segment Id to prune
  private final List<String> validSegmentIds; // Valid segment Ids for Secondary Index pruning
  private final PositionReferenceInfo positionReferenceInfo; // Position reference information

  public SecondaryIndexModel(String indexName, String currentSegmentId,
      List<String> validSegmentIds, PositionReferenceInfo positionReferenceInfo,
      Configuration configuration) {
    super(null, configuration);
    this.indexName = indexName;
    this.currentSegmentId = currentSegmentId;
    this.validSegmentIds = validSegmentIds;
    this.positionReferenceInfo = positionReferenceInfo;
  }

  public String getIndexName() {
    return indexName;
  }

  public String getCurrentSegmentId() {
    return currentSegmentId;
  }

  public List<String> getValidSegmentIds() {
    return validSegmentIds;
  }

  public PositionReferenceInfo getPositionReferenceInfo() {
    return positionReferenceInfo;
  }

  /**
   * Position Reference information. One instance of position reference information is shared across
   * all the {@link SecondaryIndex} instances for the particular query pruning with the given index
   * filter. This ensures to run a single sql query to get position references from the valid
   * segments of the given secondary index table with given index filter and populate them in map.
   * First secondary index segment prune in the query will run the sql query for position
   * references and store them in map. And subsequent segments prune in the same query can avoid
   * the individual sql for position references within the respective segment and return position
   * references from the map directly
   */
  public static class PositionReferenceInfo {
    /**
     * Indicates whether position references are available or not. Initially it is false. It is set
     * to true during the first secondary index segment prune after sql query for position
     * references from the valid segments (passed in the {@link SecondaryIndexModel}) of the
     * secondary index table. Those obtained position references are used to populate
     * {@link #segmentToPosReferences} map
     */
    private boolean fetched;
    /**
     * Map of Segment Id to set of position references within that segment. First secondary index
     * segment prune populates this map and the subsequent segment prune will return the position
     * references for the respective segment from this map directly without further sql query for
     * position references in the segment
     */
    private final Map<String, Set<String>> segmentToPosReferences = new HashMap<>();

    public boolean isFetched() {
      return fetched;
    }

    public void setFetched(boolean fetched) {
      this.fetched = fetched;
    }

    public Map<String, Set<String>> getSegmentToPosReferences() {
      return segmentToPosReferences;
    }
  }
}
