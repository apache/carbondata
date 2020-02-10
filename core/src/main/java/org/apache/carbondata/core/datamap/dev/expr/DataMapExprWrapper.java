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

package org.apache.carbondata.core.datamap.dev.expr;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.carbondata.core.datamap.DataMapDistributable;
import org.apache.carbondata.core.datamap.DataMapLevel;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.indexstore.ExtendedBlocklet;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;

/**
 * It is the wrapper around datamap and related filter expression. By using it user can apply
 * datamaps in expression style.
 */
public abstract class DataMapExprWrapper implements Serializable {

  /**
   * It get the blocklets from each leaf node datamap and apply expressions on the blocklets
   * using list of segments, it is used in case on non distributable datamap.
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
  public abstract List<ExtendedBlocklet> prune(DataMapDistributable distributable,
      List<PartitionSpec> partitionsToPrune) throws IOException;

  /**
   * It is used in case on distributable datamap. First using job it gets all blockets from all
   * related datamaps. These blocklets are passed to this method to apply expression.
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
  public abstract List<DataMapDistributableWrapper> toDistributable(List<Segment> segments)
      throws IOException;

  /**
   * Each leaf node is identified by uniqueid, so if user wants the underlying filter expression for
   * any leaf node then this method can be used.
   * @param uniqueId
   * @return
   */
  public abstract FilterResolverIntf getFilterResolverIntf(String uniqueId);

  /**
   * Get the datamap level.
   */
  public abstract DataMapLevel getDataMapLevel();

  /**
   * get the left datamap wrapper
   */
  public abstract DataMapExprWrapper getLeftDataMapWrapper();

  /**
   * get the right datamap wrapper
   */
  public abstract DataMapExprWrapper getRightDataMapWrapprt();

  /**
   * Convert segment to distributable object.
   */
  public DataMapDistributableWrapper toDistributableSegment(Segment segment)
      throws IOException {
    return null;
  }
}
