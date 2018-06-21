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
package org.apache.carbondata.core.statusmanager;

import java.util.List;

import org.apache.carbondata.core.datamap.Segment;

/**
 * Manages the segments of a table.
 */
public interface SegmentManager {

  /**
   * Create new segment for loading.
   */
  LoadMetadataDetails createNewSegment();

  /**
   * It gives the valid segments from the store at this point of time.
   *
   * @param tableId
   * @return
   */
  List<Segment> getValidSegments(String tableId);

  /**
   * After dataloading is completed, this commit should be called. It inserts one new segment
   * to segment store. After commit success this segment will be available for reading.
   *
   * @param tableId
   * @param segment
   * @return
   */
  boolean commitLoadSegment(String tableId, Segment segment);

  /**
   * Commit the compact segment. Deleting the
   * old segments and adding the newly compacted segmented should be done in a transaction
   *
   * @param tableId
   * @param compactedSegment
   * @param oldSegments
   */
  void commitCompactedSegment(String tableId, Segment compactedSegment, List<Segment> oldSegments);

  /**
   * Uses for committing the overwrite segment(it is the segment created for insert overwrite case).
   * In this case all old segments need to be invalidated and new segment should be added in a
   * transaction.
   *
   * @param tableId
   * @param overwriteSegment
   */
  void commitOverwriteSegment(String tableId, Segment overwriteSegment);

  /**
   * clean the stale segments from segment store.
   *
   * @param tableId
   * @return
   */
  void cleanInvalidSegments(String tableId);

  /**
   * Returns the list of segments which are needed for compaction.
   *
   * @param tableId
   * @param compactionType
   * @return
   */
  List<Segment> getSegmentsToCompact(String tableId, String compactionType);

  /**
   * Removes/invalidates all the segments which are less than passed load time.
   *
   * @param tableId
   * @param loadTime
   * @return
   */
  boolean deleteSegmentByLoadTime(String tableId, long loadTime);

}