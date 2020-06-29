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

package org.apache.carbondata.core.index.dev;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.carbondata.core.index.IndexInputSplit;
import org.apache.carbondata.core.index.Segment;
import org.apache.carbondata.core.index.dev.expr.IndexExprWrapper;
import org.apache.carbondata.core.indexstore.BlockletIndexWrapper;
import org.apache.carbondata.core.indexstore.TableBlockIndexUniqueIdentifier;
import org.apache.carbondata.core.indexstore.TableBlockIndexUniqueIdentifierWrapper;

/**
 * Interface for index caching
 */
public interface CacheableIndex {

  /**
   * Add the blockletIndexWrapper to cache for key tableBlockIndexUniqueIdentifier
   *
   * @param tableBlockIndexUniqueIdentifierWrapper
   * @param blockletIndexWrapper
   */
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2557
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2472
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2570
  void cache(TableBlockIndexUniqueIdentifierWrapper tableBlockIndexUniqueIdentifierWrapper,
      BlockletIndexWrapper blockletIndexWrapper) throws IOException;

  /**
   * Get all the uncached distributables from the list.
   *
   * @param distributables
   * @return
   */
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3704
  List<IndexInputSplit> getAllUncachedDistributables(List<IndexInputSplit> distributables)
      throws IOException;

  List<IndexInputSplit> getAllUncachedDistributables(
      List<Segment> segments, IndexExprWrapper indexExprWrapper) throws IOException;

//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765
  void updateSegmentIndex(
      Map<String, Set<TableBlockIndexUniqueIdentifier>> indexUniqueIdentifier);
}
