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

import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.readcommitter.ReadCommittedScope;

/**
 * Fetches the detailed blocklet which has more information to execute the query
 */
public interface BlockletDetailsFetcher {

  /**
   * Get the blocklet detail information based on blockletid, blockid and segmentid.
   *
   * @param blocklets
   * @param segment
   * @param readCommittedScope
   * @return
   * @throws IOException
   */
  List<ExtendedBlocklet> getExtendedBlocklets(List<Blocklet> blocklets, Segment segment,
      ReadCommittedScope readCommittedScope)
      throws IOException;

  /**
   * Get the blocklet detail information based on blockletid, blockid and segmentid.
   *
   * @param blocklet
   * @param segment
   * @param readCommittedScope
   * @return
   * @throws IOException
   */
  ExtendedBlocklet getExtendedBlocklet(Blocklet blocklet, Segment segment,
      ReadCommittedScope readCommittedScope) throws IOException;

  /**
   * Get all the blocklets in a segment
   *
   * @param segment
   * @param readCommittedScope
   * @return
   */
  List<Blocklet> getAllBlocklets(Segment segment, List<PartitionSpec> partitions,
      ReadCommittedScope readCommittedScope)
      throws IOException;
}
