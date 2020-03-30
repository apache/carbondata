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
import java.io.Serializable;
import java.util.List;

import org.apache.carbondata.core.index.IndexInputSplit;
import org.apache.carbondata.core.index.IndexLevel;
import org.apache.carbondata.core.index.Segment;
import org.apache.carbondata.core.indexstore.ExtendedBlocklet;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;

/**
 * It is the wrapper around index and related filter expression. By using it user can apply
 * indexes in expression style.
 */
public abstract class IndexExprWrapper implements Serializable {

  /**
   * It get the blocklets from each leaf node index and apply expressions on the blocklets
   * using list of segments, it is used in case on non distributable index.
   */
  public abstract List<ExtendedBlocklet> prune(List<Segment> segments,
      List<PartitionSpec> partitionsToPrune) throws IOException;

  /**
   * prune blocklet according distributable
   *
   * @param distributable     distributable
   * @param partitionsToPrune partitions to prune
   * @return the pruned ExtendedBlocklet list
   * @throws IOException
   */
  public abstract List<ExtendedBlocklet> prune(IndexInputSplit distributable,
      List<PartitionSpec> partitionsToPrune) throws IOException;

  /**
   * It is used in case on distributable index. First using job it gets all blockets from all
   * related indexes. These blocklets are passed to this method to apply expression.
   *
   * @param blocklets
   * @return
   * @throws IOException
   */
  public abstract List<ExtendedBlocklet> pruneBlocklets(List<ExtendedBlocklet> blocklets)
      throws IOException;

  /**
   * Get the underlying filter expression.
   * @return
   */
  public abstract FilterResolverIntf getFilterResolverIntf();

  /**
   * Convert to distributable objects for executing job.
   * @param segments
   * @return
   * @throws IOException
   */
  public abstract List<IndexInputSplitWrapper> toDistributable(List<Segment> segments)
      throws IOException;

  /**
   * Each leaf node is identified by uniqueid, so if user wants the underlying filter expression for
   * any leaf node then this method can be used.
   * @param uniqueId
   * @return
   */
  public abstract FilterResolverIntf getFilterResolverIntf(String uniqueId);

  /**
   * Get the index level.
   */
  public abstract IndexLevel getIndexLevel();

  /**
   * get the left index wrapper
   */
  public abstract IndexExprWrapper getLeftIndexWrapper();

  /**
   * get the right index wrapper
   */
  public abstract IndexExprWrapper getRightIndexWrapprt();

  /**
   * Convert segment to distributable object.
   */
  public IndexInputSplitWrapper toDistributableSegment(Segment segment)
      throws IOException {
    return null;
  }
}
