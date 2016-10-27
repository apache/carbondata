/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.carbondata.hadoop.internal.segment;

import java.io.IOException;

public interface SegmentManager {

  // use for get all segments for scann
  Segment[] getAllValidSegments();

  // use for data load
  // caller should open new segment by calling this, after data success, call commitSegment,
  // call closeSegment if any failure
  Segment openNewSegment() throws IOException;

  // use for finishing data load or compaction
  void commitSegment(Segment segment) throws IOException;

  // use for ending failed data load or compaction
  void closeSegment(Segment segment) throws IOException;

  // use for mutation of the segment
  void markSegmentForMutation(Segment segment) throws IOException;
}
