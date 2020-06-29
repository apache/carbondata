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
import java.util.Set;

import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.index.Segment;
import org.apache.carbondata.core.index.dev.Index;

import org.apache.hadoop.fs.Path;

/**
 * Fetches the detailed segmentProperties which has more information to execute the query
 */
public interface SegmentPropertiesFetcher {

  /**
   * get the Segment properties based on the SegmentID.
   * @param segment
   * @return
   * @throws IOException
   */
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2361
  SegmentProperties getSegmentProperties(Segment segment)
      throws IOException;

//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3781
  SegmentProperties getSegmentProperties(Segment segment, Set<Path> partitionLocations)
      throws IOException;

//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765
  SegmentProperties getSegmentPropertiesFromIndex(Index coarseGrainIndex);
}
