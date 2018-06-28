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

package org.apache.carbondata.datamap.bloom;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.LiteralExpression;
import org.apache.carbondata.core.scan.expression.conditional.EqualToExpression;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.util.CarbonUtil;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.bloom.CarbonBloomFilter;
import org.apache.hadoop.util.bloom.Key;

/**
 * BloomDataCoarseGrainMap is constructed in blocklet level. For each indexed column,
 * a bloom filter is constructed to indicate whether a value belongs to this blocklet.
 * More information of the index file can be found in the corresponding datamap writer.
 */
@InterfaceAudience.Internal
public class BloomCoarseGrainDataMap extends CoarseGrainDataMap {
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(BloomCoarseGrainDataMap.class.getName());
  public static final String BLOOM_INDEX_SUFFIX = ".bloomindex";
  private Set<String> indexedColumn;
  private Cache<BloomCacheKeyValue.CacheKey, BloomCacheKeyValue.CacheValue> cache;
  private String shardName;
  private Path indexPath;

  @Override
  public void init(DataMapModel dataMapModel) throws IOException {
    this.indexPath = FileFactory.getPath(dataMapModel.getFilePath());
    this.shardName = indexPath.getName();
    if (dataMapModel instanceof BloomDataMapModel) {
      BloomDataMapModel model = (BloomDataMapModel) dataMapModel;
      this.cache = model.getCache();
      this.indexedColumn = model.getIndexedColumnNames();
    }
  }

  @Override
  public List<Blocklet> prune(FilterResolverIntf filterExp, SegmentProperties segmentProperties,
      List<PartitionSpec> partitions) throws IOException {
    Set<Blocklet> hitBlocklets = new HashSet<>();
    if (filterExp == null) {
      // null is different from empty here. Empty means after pruning, no blocklet need to scan.
      return null;
    }

    List<BloomQueryModel> bloomQueryModels = getQueryValue(filterExp.getFilterExpression());
    for (BloomQueryModel bloomQueryModel : bloomQueryModels) {
      LOGGER.debug("prune blocklet for query: " + bloomQueryModel);
      BloomCacheKeyValue.CacheKey cacheKey = new BloomCacheKeyValue.CacheKey(
          this.indexPath.toString(), bloomQueryModel.columnName);
      BloomCacheKeyValue.CacheValue cacheValue = cache.get(cacheKey);
      List<CarbonBloomFilter> bloomIndexList = cacheValue.getBloomFilters();
      for (CarbonBloomFilter bloomFilter : bloomIndexList) {
        boolean scanRequired = bloomFilter.membershipTest(new Key(
            convertValueToBytes(bloomQueryModel.dataType, bloomQueryModel.filterValue)));
        if (scanRequired) {
          LOGGER.debug(String.format("BloomCoarseGrainDataMap: Need to scan -> blocklet#%s",
              String.valueOf(bloomFilter.getBlockletNo())));
          Blocklet blocklet = new Blocklet(shardName, String.valueOf(bloomFilter.getBlockletNo()));
          hitBlocklets.add(blocklet);
        } else {
          LOGGER.debug(String.format("BloomCoarseGrainDataMap: Skip scan -> blocklet#%s",
              String.valueOf(bloomFilter.getBlockletNo())));
        }
      }
    }
    return new ArrayList<>(hitBlocklets);
  }

  private byte[] convertValueToBytes(DataType dataType, Object value) {
    try {
      if (dataType == DataTypes.STRING) {
        if (value instanceof byte[]) {
          return (byte[]) value;
        } else {
          return String.valueOf(value).getBytes("utf-8");
        }
      } else {
        return CarbonUtil.getValueAsBytes(dataType, value);
      }
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException("Error occurs while converting " + value + " to " + dataType, e);
    }
  }

  private List<BloomQueryModel> getQueryValue(Expression expression) {
    List<BloomQueryModel> queryModels = new ArrayList<BloomQueryModel>();
    if (expression instanceof EqualToExpression) {
      Expression left = ((EqualToExpression) expression).getLeft();
      Expression right = ((EqualToExpression) expression).getRight();
      String column;
      DataType dataType;
      Object value;
      if (left instanceof ColumnExpression && right instanceof LiteralExpression) {
        column = ((ColumnExpression) left).getColumnName();
        if (indexedColumn.contains(column)) {
          dataType = ((ColumnExpression) left).getDataType();
          value = ((LiteralExpression) right).getLiteralExpValue();
          BloomQueryModel bloomQueryModel = new BloomQueryModel(column, dataType, value);
          queryModels.add(bloomQueryModel);
        }
        return queryModels;
      } else if (left instanceof LiteralExpression && right instanceof ColumnExpression) {
        column = ((ColumnExpression) right).getColumnName();
        if (indexedColumn.contains(column)) {
          dataType = ((ColumnExpression) right).getDataType();
          value = ((LiteralExpression) left).getLiteralExpValue();
          BloomQueryModel bloomQueryModel = new BloomQueryModel(column, dataType, value);
          queryModels.add(bloomQueryModel);
        }
        return queryModels;
      }
    }

    for (Expression child : expression.getChildren()) {
      queryModels.addAll(getQueryValue(child));
    }
    return queryModels;
  }

  @Override
  public boolean isScanRequired(FilterResolverIntf filterExp) {
    return true;
  }

  @Override
  public void clear() {
  }

  /**
   * get bloom index file
   * @param shardPath path for the shard
   * @param colName index column name
   */
  public static String getBloomIndexFile(String shardPath, String colName) {
    return shardPath.concat(File.separator).concat(colName).concat(BLOOM_INDEX_SUFFIX);
  }
  static class BloomQueryModel {
    private String columnName;
    private DataType dataType;
    private Object filterValue;

    private BloomQueryModel(String columnName, DataType dataType, Object filterValue) {
      this.columnName = columnName;
      this.dataType = dataType;
      this.filterValue = filterValue;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("BloomQueryModel{");
      sb.append("columnName='").append(columnName).append('\'');
      sb.append(", dataType=").append(dataType);
      sb.append(", filterValue=").append(filterValue);
      sb.append('}');
      return sb.toString();
    }
  }

  @Override
  public void finish() {

  }
}
