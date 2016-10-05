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
package org.apache.carbondata.hadoop;

import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.cache.CacheProvider;
import org.apache.carbondata.core.cache.CacheType;
import org.apache.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.apache.carbondata.core.carbon.datastore.SegmentTaskIndexStore;
import org.apache.carbondata.core.carbon.datastore.TableSegmentUniqueIdentifier;
import org.apache.carbondata.core.carbon.datastore.block.SegmentTaskIndexWrapper;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CarbonUtilException;

/**
 * CacheClient : Class used to request the segments cache
 */
public class CacheClient {
  /**
   * List of segments
   */
  private List<TableSegmentUniqueIdentifier> segmentList =
      new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

  /**
   * absolute table identifier
   */
  private AbsoluteTableIdentifier absoluteTableIdentifier;

  private SegmentTaskIndexStore segmentCache;

  /**
   * @param absoluteTableIdentifier
   */
  public CacheClient(AbsoluteTableIdentifier absoluteTableIdentifier) {
    this.absoluteTableIdentifier = absoluteTableIdentifier;
    segmentCache = (SegmentTaskIndexStore) CacheProvider.getInstance()
        .createCache(CacheType.DRIVER_BTREE, absoluteTableIdentifier.getStorePath());
  }

  /**
   * The method returns the SegmentTaskIndexWrapper from the segments cache
   *
   * @param tableSegmentUniqueIdentifier
   * @return
   * @throws CarbonUtilException
   */
  public SegmentTaskIndexWrapper getSegmentTaskIndexWrapper(
      TableSegmentUniqueIdentifier tableSegmentUniqueIdentifier) throws CarbonUtilException {
    SegmentTaskIndexWrapper segmentTaskIndexWrapper;
    if (null == tableSegmentUniqueIdentifier.getSegmentToTableBlocksInfos()) {
      segmentTaskIndexWrapper = segmentCache.getIfPresent(tableSegmentUniqueIdentifier);
    } else {
      segmentTaskIndexWrapper = segmentCache.get(tableSegmentUniqueIdentifier);
    }
    if (null != segmentTaskIndexWrapper) {
      segmentList.add(tableSegmentUniqueIdentifier);
    }
    return segmentTaskIndexWrapper;
  }

  /**
   * the method is used to clear access count of the unused segments cacheable object
   */
  public void close() {
    segmentCache.clear(segmentList);
    segmentCache =null;
  }

  /**
   * The method removes invalid segments from the segment level cache
   *
   * @param invalidSegments
   */
  public void removeInvalidSegments(List<String> invalidSegments) {
    segmentCache.removeSegments(invalidSegments, absoluteTableIdentifier);
  }
}
