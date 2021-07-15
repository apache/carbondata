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

package org.apache.carbondata.acid;

import java.util.List;

import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;

/**
 * Stores all information related to segments.
 * Implementation can be file based or db based also.
 */
public interface SegmentStore {

  /**
   * Write the current segment info to segment store.
   *
   * @param identifier absolute identifier of the table
   * @param segment    current segment information to be written
   */
  void writeSegment(AbsoluteTableIdentifier identifier, SegmentDetailVO segment);

  /**
   * Read all the segment info from segment store.
   *
   * @param identifier absolute identifier of the table
   * @return list of SegmentDetailVO objects
   */
  List<SegmentDetailVO> readSegments(AbsoluteTableIdentifier identifier);

  /**
   * Update the segments using table identifier.
   *
   * @param identifier absolute identifier of the table
   * @param detailVOs  list of segments information that need to be updated
   */
  void updateSegments(AbsoluteTableIdentifier identifier, List<SegmentDetailVO> detailVOs);

  /**
   * Delete the segments for the corresponding table.
   * If the filter is null, delete all the segments (example: drop table scenario)
   * Else delete the segments that matches the segment id given
   *
   * @param identifier absolute identifier of the table
   * @param segmentIDs list of segment id to be deleted. If null, all the segments will be deleted
   */
  void deleteSegments(AbsoluteTableIdentifier identifier, List<String> segmentIDs);
}