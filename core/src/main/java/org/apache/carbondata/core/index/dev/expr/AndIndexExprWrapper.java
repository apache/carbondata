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

import org.apache.carbondata.core.index.IndexInputSplit;
import org.apache.carbondata.core.index.IndexLevel;
import org.apache.carbondata.core.index.Segment;
import org.apache.carbondata.core.indexstore.ExtendedBlocklet;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;

/**
 * And expression for indexes
 */
public class AndIndexExprWrapper extends IndexExprWrapper {

  private IndexExprWrapper left;

  private IndexExprWrapper right;

  private FilterResolverIntf resolverIntf;

  public AndIndexExprWrapper(IndexExprWrapper left, IndexExprWrapper right,
      FilterResolverIntf resolverIntf) {
    this.left = left;
    this.right = right;
    this.resolverIntf = resolverIntf;
  }

  @Override
  public List<ExtendedBlocklet> prune(List<Segment> segments, List<PartitionSpec> partitionsToPrune)
      throws IOException {
    List<ExtendedBlocklet> leftPrune = left.prune(segments, partitionsToPrune);
    List<ExtendedBlocklet> rightPrune = right.prune(segments, partitionsToPrune);
    List<ExtendedBlocklet> andBlocklets = new ArrayList<>();
    for (ExtendedBlocklet blocklet : leftPrune) {
      if (rightPrune.contains(blocklet)) {
        andBlocklets.add(blocklet);
      }
    }
    return andBlocklets;
  }

  @Override
  public List<ExtendedBlocklet> prune(IndexInputSplit distributable,
      List<PartitionSpec> partitionsToPrune)
          throws IOException {
    List<ExtendedBlocklet> leftPrune = left.prune(distributable, partitionsToPrune);
    List<ExtendedBlocklet> rightPrune = right.prune(distributable, partitionsToPrune);
    List<ExtendedBlocklet> andBlocklets = new ArrayList<>();
    for (ExtendedBlocklet blocklet : leftPrune) {
      if (rightPrune.contains(blocklet)) {
        andBlocklets.add(blocklet);
      }
    }
    return andBlocklets;
  }

  @Override
  public List<ExtendedBlocklet> pruneBlocklets(List<ExtendedBlocklet> blocklets)
      throws IOException {
    List<ExtendedBlocklet> leftPrune = left.pruneBlocklets(blocklets);
    List<ExtendedBlocklet> rightPrune = right.pruneBlocklets(blocklets);
    List<ExtendedBlocklet> andBlocklets = new ArrayList<>();
    for (ExtendedBlocklet blocklet : leftPrune) {
      if (rightPrune.contains(blocklet)) {
        andBlocklets.add(blocklet);
      }
    }
    return andBlocklets;
  }

  @Override
  public FilterResolverIntf getFilterResolverIntf() {
    return resolverIntf;
  }

  @Override
  public FilterResolverIntf getFilterResolverIntf(String uniqueId) {
    FilterResolverIntf leftExp = left.getFilterResolverIntf(uniqueId);
    FilterResolverIntf rightExp = right.getFilterResolverIntf(uniqueId);
    if (leftExp != null) {
      return leftExp;
    } else if (rightExp != null) {
      return rightExp;
    }
    return null;
  }

  @Override
  public List<IndexInputSplitWrapper> toDistributable(List<Segment> segments)
      throws IOException {
    List<IndexInputSplitWrapper> wrappers = new ArrayList<>();
    wrappers.addAll(left.toDistributable(segments));
    wrappers.addAll(right.toDistributable(segments));
    return wrappers;
  }

  @Override
  public IndexLevel getIndexLevel() {
    return left.getIndexLevel();
  }

  @Override
  public IndexExprWrapper getLeftIndexWrapper() {
    return left;
  }

  @Override
  public IndexExprWrapper getRightIndexWrapprt() {
    return right;
  }
}
