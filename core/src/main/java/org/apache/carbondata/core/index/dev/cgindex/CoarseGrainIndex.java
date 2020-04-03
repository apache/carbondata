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

package org.apache.carbondata.core.index.dev.cgindex;

import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.index.Segment;
import org.apache.carbondata.core.index.dev.Index;
import org.apache.carbondata.core.indexstore.Blocklet;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.filter.executer.FilterExecuter;

/**
 * Index for Coarse Grain level, see {@link org.apache.carbondata.core.index.IndexLevel#CG}
 */
@InterfaceAudience.Developer("Index")
@InterfaceStability.Evolving
public abstract class CoarseGrainIndex implements Index<Blocklet> {

  @Override
  public List<Blocklet> prune(Expression expression, SegmentProperties segmentProperties,
      List<PartitionSpec> partitions, CarbonTable carbonTable, FilterExecuter filterExecuter) {
    throw new UnsupportedOperationException("Filter expression not supported");
  }

  @Override
  public long getRowCount(Segment segment, List<PartitionSpec> partitions) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override
  public Map<String, Long> getRowCountForEachBlock(Segment segment, List<PartitionSpec> partitions,
      Map<String, Long> blockletToRowCountMap) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override
  public int getNumberOfEntries() {
    // keep default, one record in one index
    return 1;
  }
}
