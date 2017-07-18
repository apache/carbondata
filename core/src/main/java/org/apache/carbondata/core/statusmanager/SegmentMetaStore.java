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

/**
 * Stores all information related to segments.
 */
public interface SegmentMetaStore {

  /**
   * Returns all the segments from metastore for a table.
   *
   * @param tableId
   * @return
   */
  List<Segment> getSegments(String tableId);

  /**
   * Inserts the segment to segment store. This segment is available for reading as soon as insert
   * success
   *
   * @param tableId
   * @param segment
   */
  void insertSegment(String tableId, Segment segment);

  /**
   * Update the segment, usually we update only the status.
   *
   * @param tableId
   * @param segment
   */
  void updateSegment(String tableId, Segment segment);

  /**
   * Delete the segment from carbonstore, it happens while compaction.
   *
   * @param tableId
   * @param segment
   */
  void deleteSegment(String tableId, Segment segment);

}
