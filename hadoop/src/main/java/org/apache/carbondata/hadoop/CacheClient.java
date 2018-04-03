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
package org.apache.carbondata.hadoop;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.CacheProvider;
import org.apache.carbondata.core.cache.CacheType;
import org.apache.carbondata.core.datastore.SegmentTaskIndexStore;
import org.apache.carbondata.core.datastore.TableSegmentUniqueIdentifier;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.block.SegmentTaskIndexWrapper;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;

/**
 * CacheClient : Holds all the Cache access clients for Btree, Dictionary
 */
public class CacheClient {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CacheClient.class.getName());

  // segment access client for driver LRU cache
  private CacheAccessClient<TableSegmentUniqueIdentifier, SegmentTaskIndexWrapper>
      segmentAccessClient;

  private static Map<SegmentTaskIndexStore.SegmentPropertiesWrapper, SegmentProperties>
      segmentProperties =
      new HashMap<SegmentTaskIndexStore.SegmentPropertiesWrapper, SegmentProperties>();

  public CacheClient() {
    Cache<TableSegmentUniqueIdentifier, SegmentTaskIndexWrapper> segmentCache =
        CacheProvider.getInstance().createCache(CacheType.DRIVER_BTREE);
    segmentAccessClient = new CacheAccessClient<>(segmentCache);
  }

  public CacheAccessClient<TableSegmentUniqueIdentifier, SegmentTaskIndexWrapper>
      getSegmentAccessClient() {
    return segmentAccessClient;
  }

  public void close() {
    segmentAccessClient.close();
  }

  /**
   * Method to get the segment properties and avoid construction of new segment properties until
   * the schema is not modified
   *
   * @param tableIdentifier
   * @param columnsInTable
   * @param columnCardinality
   */
  public SegmentProperties getSegmentProperties(AbsoluteTableIdentifier tableIdentifier,
      List<ColumnSchema> columnsInTable, int[] columnCardinality) {
    SegmentTaskIndexStore.SegmentPropertiesWrapper segmentPropertiesWrapper =
        new SegmentTaskIndexStore.SegmentPropertiesWrapper(tableIdentifier, columnsInTable,
            columnCardinality);
    SegmentProperties segmentProperties = this.segmentProperties.get(segmentPropertiesWrapper);
    if (null == segmentProperties) {
      // create a metadata details
      // this will be useful in query handling
      // all the data file metadata will have common segment properties we
      // can use first one to get create the segment properties
      LOGGER.info("Constructing new SegmentProperties");
      segmentProperties = new SegmentProperties(columnsInTable, columnCardinality);
      this.segmentProperties.put(segmentPropertiesWrapper, segmentProperties);
    }
    return segmentProperties;
  }
}
