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

package org.apache.carbondata.datamap.minmax;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.datamap.dev.DataMapModel;
import org.apache.carbondata.core.datamap.dev.cgdatamap.CoarseGrainDataMap;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.indexstore.Blocklet;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.scan.filter.FilterUtil;
import org.apache.carbondata.core.scan.filter.executer.FilterExecuter;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;

import org.apache.hadoop.fs.Path;

/**
 * Datamap implementation for min max blocklet.
 */
@InterfaceAudience.Internal
public class MinMaxIndexDataMap extends CoarseGrainDataMap {
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(MinMaxIndexDataMap.class.getName());
  private Cache<MinMaxDataMapCacheKeyValue.Key, MinMaxDataMapCacheKeyValue.Value> cache;
  private MinMaxDataMapCacheKeyValue.Key cacheKey;
  private List<CarbonColumn> indexColumns;
  private String shardName;

  @Override
  public void init(DataMapModel model) throws IOException {
    Path shardPath = FileFactory.getPath(model.getFilePath());
    this.shardName = shardPath.getName();
    assert model instanceof MinMaxDataMapModel;
    this.cache = ((MinMaxDataMapModel) model).getCache();
    this.cacheKey = new MinMaxDataMapCacheKeyValue.Key(model.getFilePath());
  }

  /**
   * init field converters for index columns
   */
  public void initOthers(CarbonTable carbonTable, List<CarbonColumn> indexedColumn)
      throws IOException {
    this.indexColumns = indexedColumn;
  }

  /**
   * Block Prunning logic for Min Max DataMap. It will reuse the pruning procedure.
   */
  @Override
  public List<Blocklet> prune(FilterResolverIntf filterExp, SegmentProperties segmentProperties,
      List<PartitionSpec> partitions) throws IOException {
    Objects.requireNonNull(filterExp);
    List<Blocklet> hitBlocklets = new ArrayList<>();
    FilterExecuter filterExecuter =
        FilterUtil.getFilterExecuterTree(filterExp, segmentProperties, null, indexColumns);
    List<MinMaxIndexHolder> minMaxIndexHolders = cache.get(cacheKey).getMinMaxIndexHolders();
    for (int i = 0; i < minMaxIndexHolders.size(); i++) {
      byte[][] minValues = minMaxIndexHolders.get(i).getMinValues();
      byte[][] maxValues = minMaxIndexHolders.get(i).getMaxValues();

      BitSet bitSet = filterExecuter.isScanRequired(maxValues, minValues);
      if (!bitSet.isEmpty()) {
        LOGGER.debug(String.format("MinMaxDataMap: Need to scan -> blocklet#%s",
            minMaxIndexHolders.get(i).getBlockletId()));
        Blocklet blocklet =
            new Blocklet(shardName, String.valueOf(minMaxIndexHolders.get(i).getBlockletId()));
        hitBlocklets.add(blocklet);
      } else {
        LOGGER.debug(String.format("MinMaxDataMap: Skip scan -> blocklet#%s",
            minMaxIndexHolders.get(i).getBlockletId()));
      }
    }
    return hitBlocklets;
  }

  @Override
  public boolean isScanRequired(FilterResolverIntf filterExp) {
    return true;
  }

  @Override
  public void clear() {
  }

  public static String getIndexFile(String shardPath, String combineColumn) {
    return shardPath.concat(File.separator).concat(combineColumn)
        .concat(MinMaxIndexHolder.MINMAX_INDEX_SUFFIX);
  }

  @Override
  public void finish() {

  }
}
