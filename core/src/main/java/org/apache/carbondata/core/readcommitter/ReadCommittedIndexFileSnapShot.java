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

package org.apache.carbondata.core.readcommitter;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.core.statusmanager.SegmentRefreshInfo;

/**
 * This class is going to save the the Index files which are taken snapshot
 * from the readCommitter Interface.
 */
@InterfaceAudience.Internal
@InterfaceStability.Evolving
public class ReadCommittedIndexFileSnapShot implements Serializable {

  /**
   * Segment Numbers are mapped with list of Index Files.
   */
  private Map<String, List<String>> segmentIndexFileMap;
  private Map<String, SegmentRefreshInfo> segmentTimestampUpdaterMap;

  public ReadCommittedIndexFileSnapShot(Map<String, List<String>> segmentIndexFileMap,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2431
      Map<String, SegmentRefreshInfo> segmentTimestampUpdaterMap) {
    this.segmentIndexFileMap = segmentIndexFileMap;
    this.segmentTimestampUpdaterMap = segmentTimestampUpdaterMap;
  }

  public Map<String, List<String>> getSegmentIndexFileMap() {
    return segmentIndexFileMap;
  }

  public Map<String, SegmentRefreshInfo> getSegmentTimestampUpdaterMap() {
    return segmentTimestampUpdaterMap;
  }
}
