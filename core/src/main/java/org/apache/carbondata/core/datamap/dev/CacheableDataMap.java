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

package org.apache.carbondata.core.datamap.dev;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.carbondata.core.datamap.DataMapDistributable;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datamap.dev.expr.DataMapExprWrapper;
import org.apache.carbondata.core.indexstore.BlockletDataMapIndexWrapper;
import org.apache.carbondata.core.indexstore.TableBlockIndexUniqueIdentifier;
import org.apache.carbondata.core.indexstore.TableBlockIndexUniqueIdentifierWrapper;

/**
 * Interface for data map caching
 */
public interface CacheableDataMap {

  /**
   * Add the blockletDataMapIndexWrapper to cache for key tableBlockIndexUniqueIdentifier
   *
   * @param tableBlockIndexUniqueIdentifierWrapper
   * @param blockletDataMapIndexWrapper
   */
  void cache(TableBlockIndexUniqueIdentifierWrapper tableBlockIndexUniqueIdentifierWrapper,
      BlockletDataMapIndexWrapper blockletDataMapIndexWrapper) throws IOException;

  /**
   * Get all the uncached distributables from the list.
   *
   * @param distributables
   * @return
   */
  List<DataMapDistributable> getAllUncachedDistributables(List<DataMapDistributable> distributables)
      throws IOException;

  List<DataMapDistributable> getAllUncachedDistributables(
      List<Segment> segments, DataMapExprWrapper dataMapExprWrapper) throws IOException;

  void updateSegmentDataMap(
      Map<String, Set<TableBlockIndexUniqueIdentifier>> indexUniqueIdentifier);
}