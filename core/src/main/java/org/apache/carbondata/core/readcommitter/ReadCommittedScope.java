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

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.mutate.UpdateVO;
import org.apache.carbondata.core.statusmanager.SegmentRefreshInfo;
import org.apache.carbondata.core.statusmanager.SegmentsHolder;

/**
 * ReadCommitted interface that defines a read scope.
 */
@InterfaceAudience.Internal
@InterfaceStability.Stable public interface ReadCommittedScope extends Serializable {

  SegmentsHolder getSegments() throws IOException;

  /**
   * @param segment
   * @return map of Absolute path of index file as key and null as value -- without mergeIndex
   * map of AbsolutePath with fileName of MergeIndex parent file as key and mergeIndexFileName
   * as value -- with mergeIndex
   * @throws IOException
   */
  Map<String, String> getCommittedIndexFile(Segment segment) throws IOException ;

  SegmentRefreshInfo getCommittedSegmentRefreshInfo(
      Segment segment, UpdateVO updateVo) throws IOException;

  void takeCarbonIndexFileSnapShot() throws IOException;
}
