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

/**
 * Fetches the detailed blocklet which has more information to execute the query
 */
public interface BlockletDetailsFetcher {

  /**
   * Get the blocklet detail information based on blockletid, blockid and segmentid.
   *
   * @param blocklets
   * @param segmentId
   * @return
   * @throws IOException
   */
  List<ExtendedBlocklet> getExtendedBlocklets(List<Blocklet> blocklets, String segmentId)
      throws IOException;

  /**
   * Get the blocklet detail information based on blockletid, blockid and segmentid.
   *
   * @param blocklet
   * @param segmentId
   * @return
   * @throws IOException
   */
  ExtendedBlocklet getExtendedBlocklet(Blocklet blocklet, String segmentId) throws IOException;

  /**
   * Get all the blocklets in a segment
   *
   * @param segmentId
   * @return
   */
  List<Blocklet> getAllBlocklets(String segmentId, List<String> partitions) throws IOException;
}
