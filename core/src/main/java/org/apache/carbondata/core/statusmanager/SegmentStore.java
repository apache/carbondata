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
import java.util.Map;

import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.scan.expression.Expression;

/**
 * Stores all information related to segments.
 */
public interface SegmentStore {

  /**
   * Generate new segment name for data loading
   *
   * @param identifier
   * @return
   */
  String generateSegmentForLoading(AbsoluteTableIdentifier identifier);

  /**
   * Returns the segments from metastore using identifier and applied filters.
   *
   * @param identifier
   * @param filters
   * @return
   */
  List<Segment> getSegments(AbsoluteTableIdentifier identifier, List<Expression> filters);

  /**
   * Inserts the segment to segment store. This segment is available for reading as soon as insert
   * success
   *
   * @param identifier
   * @param segment
   */
  void insertSegment(AbsoluteTableIdentifier identifier, Segment segment);

  /**
   * Update the segments using table identifier.All the segments need to be committed in a single
   * transaction.
   *
   * @param identifier
   * @param updateDetails
   */
  void updateSegments(AbsoluteTableIdentifier identifier, List<SegmentUpdateDetail> updateDetails);

  /**
   * Delete all segments for the corresponding table. It happens during drop table.
   *
   * @param identifier
   */
  void deleteSegments(AbsoluteTableIdentifier identifier);

  /**
   * Segment update info
   */
  class SegmentUpdateDetail {

    /**
     * Segment id
     */
    String segmentId;

    /**
     * Fields to be updated in the segment.
     */
    Map<String, String> fieldsToBeUpdated;

  }

}