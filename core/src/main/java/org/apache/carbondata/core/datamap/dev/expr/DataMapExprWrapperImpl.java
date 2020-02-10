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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.carbondata.core.datamap.DataMapDistributable;
import org.apache.carbondata.core.datamap.DataMapFilter;
import org.apache.carbondata.core.datamap.DataMapLevel;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datamap.TableDataMap;
import org.apache.carbondata.core.datamap.dev.DataMap;
import org.apache.carbondata.core.indexstore.ExtendedBlocklet;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;

public class DataMapExprWrapperImpl extends DataMapExprWrapper {

  private static final long serialVersionUID = -6240385328696074171L;

  private transient TableDataMap dataMap;

  private FilterResolverIntf expression;

  private String uniqueId;

  public DataMapExprWrapperImpl(TableDataMap dataMap, FilterResolverIntf expression) {
    this.dataMap = dataMap;
    this.expression = expression;
    this.uniqueId = UUID.randomUUID().toString();
  }

  @Override
  public List<ExtendedBlocklet> prune(List<Segment> segments, List<PartitionSpec> partitionsToPrune)
      throws IOException {
    return dataMap.prune(segments, new DataMapFilter(expression), partitionsToPrune);
  }

  public List<ExtendedBlocklet> prune(DataMapDistributable distributable,
      List<PartitionSpec> partitionsToPrune)
      throws IOException {
    List<DataMap> dataMaps = dataMap.getTableDataMaps(distributable);
    return dataMap.prune(dataMaps, distributable, expression, partitionsToPrune);
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
  public List<DataMapDistributableWrapper> toDistributable(List<Segment> segments) {
    List<DataMapDistributable> dataMapDistributables = dataMap.toDistributable(segments);
    List<DataMapDistributableWrapper> wrappers = new ArrayList<>();
    for (DataMapDistributable distributable : dataMapDistributables) {
      wrappers.add(new DataMapDistributableWrapper(uniqueId, distributable));
    }
    return wrappers;
  }

  @Override
  public DataMapLevel getDataMapLevel() {
    return dataMap.getDataMapFactory().getDataMapLevel();
  }

  public DataMapSchema getDataMapSchema() {
    return dataMap.getDataMapSchema();
  }

  @Override
  public DataMapExprWrapper getLeftDataMapWrapper() {
    return null;
  }

  @Override
  public DataMapExprWrapper getRightDataMapWrapprt() {
    return null;
  }

  /**
   * Convert segment to distributable object.
   */
  public DataMapDistributableWrapper toDistributableSegment(Segment segment)
      throws IOException {
    return dataMap.toDistributableSegment(segment, uniqueId);
  }
}
