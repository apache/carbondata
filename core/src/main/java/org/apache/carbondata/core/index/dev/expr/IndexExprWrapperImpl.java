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

import org.apache.carbondata.core.index.IndexFilter;
import org.apache.carbondata.core.index.IndexInputSplit;
import org.apache.carbondata.core.index.IndexLevel;
import org.apache.carbondata.core.index.Segment;
import org.apache.carbondata.core.index.TableIndex;
import org.apache.carbondata.core.index.dev.Index;
import org.apache.carbondata.core.indexstore.ExtendedBlocklet;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.metadata.schema.table.IndexSchema;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;

public class IndexExprWrapperImpl extends IndexExprWrapper {

  private static final long serialVersionUID = -6240385328696074171L;

  private transient TableIndex index;

  private FilterResolverIntf expression;

  private String uniqueId;

  public IndexExprWrapperImpl(TableIndex index, FilterResolverIntf expression) {
    this.index = index;
    this.expression = expression;
    this.uniqueId = UUID.randomUUID().toString();
  }

  @Override
  public List<ExtendedBlocklet> prune(List<Segment> segments, List<PartitionSpec> partitionsToPrune)
      throws IOException {
    return index.prune(segments, new IndexFilter(expression), partitionsToPrune);
  }

  public List<ExtendedBlocklet> prune(IndexInputSplit distributable,
      List<PartitionSpec> partitionsToPrune)
      throws IOException {
    List<Index> indices = index.getTableIndexes(distributable);
    return index.prune(indices, distributable, expression, partitionsToPrune);
  }

  @Override
  public List<ExtendedBlocklet> pruneBlocklets(List<ExtendedBlocklet> blocklets) {
    List<ExtendedBlocklet> blockletList = new ArrayList<>();
    for (ExtendedBlocklet blocklet: blocklets) {
      if (blocklet.getIndexUniqueId().equals(uniqueId)) {
        blockletList.add(blocklet);
      }
    }
    return blockletList;
  }

  @Override
  public FilterResolverIntf getFilterResolverIntf() {
    return expression;
  }

  @Override
  public FilterResolverIntf getFilterResolverIntf(String uniqueId) {
    if (this.uniqueId.equals(uniqueId)) {
      return expression;
    }
    return null;
  }

  @Override
  public List<IndexInputSplitWrapper> toDistributable(List<Segment> segments) {
    List<IndexInputSplit> indexInputSplits = index.toDistributable(segments);
    List<IndexInputSplitWrapper> wrappers = new ArrayList<>();
    for (IndexInputSplit distributable : indexInputSplits) {
      wrappers.add(new IndexInputSplitWrapper(uniqueId, distributable));
    }
    return wrappers;
  }

  @Override
  public IndexLevel getIndexLevel() {
    return index.getIndexFactory().getIndexLevel();
  }

  public IndexSchema getIndexSchema() {
    return index.getIndexSchema();
  }

  @Override
  public IndexExprWrapper getLeftIndexWrapper() {
    return null;
  }

  @Override
  public IndexExprWrapper getRightIndexWrapprt() {
    return null;
  }

  /**
   * Convert segment to distributable object.
   */
  public IndexInputSplitWrapper toDistributableSegment(Segment segment)
      throws IOException {
    return index.toDistributableSegment(segment, uniqueId);
  }
}
