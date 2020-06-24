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

package org.apache.carbondata.core.indexstore;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.carbondata.core.index.Segment;

import org.apache.hadoop.fs.Path;

/**
 * Fetches the detailed blocklet which has more information to execute the query
 */
public interface BlockletDetailsFetcher {

  /**
   * Get the blocklet detail information based on blockletid, blockid and segmentId.
   *
   * @param blocklets
   * @param segment
   * @return
   * @throws IOException
   */
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2187
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2361
  List<ExtendedBlocklet> getExtendedBlocklets(List<Blocklet> blocklets, Segment segment)
      throws IOException;

  /**
   * Get the blocklet detail information based on blockletid, blockid and segmentId.
   *
   * @param blocklet
   * @param segment
   * @return
   * @throws IOException
   */
  ExtendedBlocklet getExtendedBlocklet(Blocklet blocklet, Segment segment) throws IOException;

  /**
   * Get all the blocklets in a segment
   *
   * @param segment
   * @return
   */
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3781
  List<Blocklet> getAllBlocklets(Segment segment, Set<Path> partitionLocations)
      throws IOException;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1544

  /**
   * clears the index from cache and segmentMap from executor
   */
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2484
  void clear();
}
