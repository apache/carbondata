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

import java.util.HashMap;
import java.util.Map;

import org.apache.carbondata.acid.transaction.TransactionDetail;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;

import org.apache.log4j.Logger;

/**
 * Manages Load/Segment status
 */
public class SegmentManager {

  private static final Logger LOG = LogServiceFactory.getLogService(SegmentManager.class.getName());

  private SegmentStore segmentStore;

  public SegmentManager(SegmentStore segmentStore) {
    this.segmentStore = segmentStore;
  }

  /**
   * Get the SegmentStatusReader instance to read the segment metadata files
   * (table status file, segment file, update status files).
   *
   * @param identifier of the table which table status should be read
   * @return new instance of SegmentStatusReader to read table status.
   */
  public SegmentStatusReader getSegmentStatusReader(AbsoluteTableIdentifier identifier) {
    return new SegmentStatusReader(identifier, segmentStore);
  }

  /**
   * Get the SegmentStatusUpdater instance to update/insert the segment metadata files
   * (table status file, segment file, update status files)
   * While getting it, acquire lock to avoid any concurrent updates. It is user
   * responsibility to call {SegmentStatusUpdater.close()} method to release the lock.
   *
   * @param identifier of the table for which segment metadata files should update/insert
   * @return new instance of SegmentStatusUpdater to update/insert table status.
   */
  public SegmentStatusUpdater getSegmentStatusUpdater(AbsoluteTableIdentifier identifier) {
    return new SegmentStatusUpdater(identifier, segmentStore);
  }

}
