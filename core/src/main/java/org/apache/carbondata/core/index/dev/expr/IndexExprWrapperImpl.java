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

package org.apache.carbondata.core.index.dev.expr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.index.IndexDistributable;
import org.apache.carbondata.core.index.IndexFilter;
import org.apache.carbondata.core.index.IndexLevel;
import org.apache.carbondata.core.index.TableIndex;
import org.apache.carbondata.core.index.dev.Index;
import org.apache.carbondata.core.indexstore.ExtendedBlocklet;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;

public class IndexExprWrapperImpl extends IndexExprWrapper {

  private static final long serialVersionUID = -6240385328696074171L;

  private transient TableIndex dataMap;

  private FilterResolverIntf expression;

  private String uniqueId;

  public IndexExprWrapperImpl(TableIndex dataMap, FilterResolverIntf expression) {
    this.dataMap = dataMap;
    this.expression = expression;
    this.uniqueId = UUID.randomUUID().toString();
  }

  @Override
  public List<ExtendedBlocklet> prune(List<Segment> segments, List<PartitionSpec> partitionsToPrune)
      throws IOException {
    return dataMap.prune(segments, new IndexFilter(expression), partitionsToPrune);
  }

  public List<ExtendedBlocklet> prune(IndexDistributable distributable,
      List<PartitionSpec> partitionsToPrune)
      throws IOException {
    List<Index> indices = dataMap.getTableIndexes(distributable);
    return dataMap.prune(indices, distributable, expression, partitionsToPrune);
  }

  @Override
  public List<ExtendedBlocklet> pruneBlocklets(List<ExtendedBlocklet> blocklets) {
    List<ExtendedBlocklet> blockletList = new ArrayList<>();
    for (ExtendedBlocklet blocklet: blocklets) {
      if (blocklet.getDataMapUniqueId().equals(uniqueId)) {
        blockletList.add(blocklet);
      }
    }
    return blockletList;
  }

  @Override
  public FilterResolverIntf getFilterResolverIntf(String uniqueId) {
    if (this.uniqueId.equals(uniqueId)) {
      return expression;
    }
    return null;
  }

  @Override
  public List<IndexDistributableWrapper> toDistributable(List<Segment> segments) {
    List<IndexDistributable> indexDistributables = dataMap.toDistributable(segments);
    List<IndexDistributableWrapper> wrappers = new ArrayList<>();
    for (IndexDistributable distributable : indexDistributables) {
      wrappers.add(new IndexDistributableWrapper(uniqueId, distributable));
    }
    return wrappers;
  }

  @Override
  public IndexLevel getIndexLevel() {
    return dataMap.getIndexFactory().getIndexLevel();
  }

  public DataMapSchema getDataMapSchema() {
    return dataMap.getDataMapSchema();
  }

  @Override
  public IndexExprWrapper getLeftIndexWrapper() {
    return null;
  }

  @Override
  public IndexExprWrapper getRightIndexWrapper() {
    return null;
  }

  /**
   * Convert segment to distributable object.
   */
  public IndexDistributableWrapper toDistributableSegment(Segment segment)
      throws IOException {
    return dataMap.toDistributableSegment(segment, uniqueId);
  }
}
