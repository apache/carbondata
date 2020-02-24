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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.dev.DataMapModel;
import org.apache.carbondata.core.datamap.dev.cgdatamap.CoarseGrainDataMap;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.datastore.page.encoding.bool.BooleanConvert;
import org.apache.carbondata.core.indexstore.Blocklet;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.LiteralExpression;
import org.apache.carbondata.core.scan.expression.conditional.EqualToExpression;
import org.apache.carbondata.core.scan.expression.conditional.InExpression;
import org.apache.carbondata.core.scan.expression.conditional.ListExpression;
import org.apache.carbondata.core.scan.expression.exception.FilterIllegalMemberException;
import org.apache.carbondata.core.scan.expression.logical.AndExpression;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.processing.loading.DataField;
import org.apache.carbondata.processing.loading.converter.BadRecordLogHolder;
import org.apache.carbondata.processing.loading.converter.FieldConverter;
import org.apache.carbondata.processing.loading.converter.impl.FieldEncoderFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.bloom.CarbonBloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.log4j.Logger;

/**
 * BloomDataCoarseGrainMap is constructed in blocklet level. For each indexed column,
 * a bloom filter is constructed to indicate whether a value belongs to this blocklet.
 * More information of the index file can be found in the corresponding datamap writer.
 */
@InterfaceAudience.Internal
public class BloomCoarseGrainDataMap extends CoarseGrainDataMap {
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(BloomCoarseGrainDataMap.class.getName());
  private Map<String, CarbonColumn> name2Col;
  private Cache<BloomCacheKeyValue.CacheKey, BloomCacheKeyValue.CacheValue> cache;
  private String shardName;
  private Path indexPath;
  private Set<String> filteredShard;
  private boolean needShardPrune;
  /**
   * This is used to convert literal filter value to internal carbon value
   */
  private Map<String, FieldConverter> name2Converters;
  private BadRecordLogHolder badRecordLogHolder;

  @Override
  public void init(DataMapModel dataMapModel) {
    this.indexPath = FileFactory.getPath(dataMapModel.getFilePath());
    this.shardName = indexPath.getName();
    if (dataMapModel instanceof BloomDataMapModel) {
      BloomDataMapModel model = (BloomDataMapModel) dataMapModel;
      this.cache = model.getCache();
    }
  }

  public void setFilteredShard(Set<String> filteredShard) {
    this.filteredShard = filteredShard;
    // do shard prune when pruning only if bloom index files are merged
    this.needShardPrune = filteredShard != null &&
            shardName.equals(BloomIndexFileStore.MERGE_BLOOM_INDEX_SHARD_NAME);
  }

  /**
   * init field converters for index columns
   */
  public void initIndexColumnConverters(CarbonTable carbonTable, List<CarbonColumn> indexedColumn) {
    this.name2Col = new HashMap<>(indexedColumn.size());
    for (CarbonColumn col : indexedColumn) {
      this.name2Col.put(col.getColName(), col);
    }

    this.name2Converters = new HashMap<>(indexedColumn.size());
    String nullFormat = "\\N";

    for (int i = 0; i < indexedColumn.size(); i++) {
      DataField dataField = new DataField(indexedColumn.get(i));
      String dateFormat = CarbonProperties.getInstance().getProperty(
          CarbonCommonConstants.CARBON_DATE_FORMAT,
          CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT);
      dataField.setDateFormat(dateFormat);
      String tsFormat = CarbonProperties.getInstance().getProperty(
          CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
          CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT);
      dataField.setTimestampFormat(tsFormat);
      FieldConverter fieldConverter = FieldEncoderFactory.getInstance()
          .createFieldEncoder(dataField, i, nullFormat, false,
              false, carbonTable.getTablePath(), null);
      this.name2Converters.put(indexedColumn.get(i).getColName(), fieldConverter);
    }
    this.badRecordLogHolder = new BadRecordLogHolder();
    this.badRecordLogHolder.setLogged(false);
  }

  @Override
  public List<Blocklet> prune(FilterResolverIntf filterExp, SegmentProperties segmentProperties,
      List<PartitionSpec> partitions) {
    Set<Blocklet> hitBlocklets = null;
    if (filterExp == null) {
      // null is different from empty here. Empty means after pruning, no blocklet need to scan.
      return null;
    }
    if (filteredShard.isEmpty()) {
      LOGGER.info("Bloom filtered shards is empty");
      return new ArrayList<>();
    }

    List<BloomQueryModel> bloomQueryModels;
    bloomQueryModels = createQueryModel(filterExp.getFilterExpression());
    for (BloomQueryModel bloomQueryModel : bloomQueryModels) {
      Set<Blocklet> tempHitBlockletsResult = new HashSet<>();
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("prune blocklet for query: " + bloomQueryModel);
      }
      BloomCacheKeyValue.CacheKey cacheKey = new BloomCacheKeyValue.CacheKey(
          this.indexPath.toString(), bloomQueryModel.columnName);
      BloomCacheKeyValue.CacheValue cacheValue = cache.get(cacheKey);
      List<CarbonBloomFilter> bloomIndexList = cacheValue.getBloomFilters();
      for (CarbonBloomFilter bloomFilter : bloomIndexList) {
        if (needShardPrune && !filteredShard.contains(bloomFilter.getShardName())) {
          // skip shard which has been pruned in Main datamap
          continue;
        }
        boolean scanRequired = false;
        for (byte[] value: bloomQueryModel.filterValues) {
          scanRequired = bloomFilter.membershipTest(new Key(value));
          if (scanRequired) {
            // if any filter value hit this bloomfilter
            // no need to check other filter values
            break;
          }
        }
        if (scanRequired) {
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(String.format("BloomCoarseGrainDataMap: Need to scan -> blocklet#%s",
                String.valueOf(bloomFilter.getBlockletNo())));
          }
          Blocklet blocklet = new Blocklet(bloomFilter.getShardName(),
              String.valueOf(bloomFilter.getBlockletNo()));
          tempHitBlockletsResult.add(blocklet);
        } else if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(String.format("BloomCoarseGrainDataMap: Skip scan -> blocklet#%s",
              String.valueOf(bloomFilter.getBlockletNo())));
        }
        // get intersect result between query models
        // pre-condition: only And/In/EqualTo expression exists in single bloom datamap
        if (null == hitBlocklets) {
          hitBlocklets = tempHitBlockletsResult;
        } else {
          hitBlocklets.retainAll(tempHitBlockletsResult);
        }
      }
    }
    if (hitBlocklets == null) {
      LOGGER.warn(String.format("HitBlocklets is empty in bloom filter prune method. " +
              "bloomQueryModels size is %d, filterShards size if %d",
              bloomQueryModels.size(), filteredShard.size()));
      return new ArrayList<>();
    }
    return new ArrayList<>(hitBlocklets);
  }

  private List<BloomQueryModel> createQueryModel(Expression expression) {
    List<BloomQueryModel> queryModels = new ArrayList<BloomQueryModel>();
    // bloomdatamap only support equalTo and In operators now
    if (expression instanceof EqualToExpression) {
      Expression left = ((EqualToExpression) expression).getLeft();
      Expression right = ((EqualToExpression) expression).getRight();
      String column;
      if (left instanceof ColumnExpression && right instanceof LiteralExpression) {
        column = ((ColumnExpression) left).getColumnName();
        if (this.name2Col.containsKey(column)) {
          BloomQueryModel bloomQueryModel =
              buildQueryModelForEqual((ColumnExpression) left, (LiteralExpression) right);
          queryModels.add(bloomQueryModel);
        }
        return queryModels;
      } else if (left instanceof LiteralExpression && right instanceof ColumnExpression) {
        column = ((ColumnExpression) right).getColumnName();
        if (this.name2Col.containsKey(column)) {
          BloomQueryModel bloomQueryModel =
              buildQueryModelForEqual((ColumnExpression) right, (LiteralExpression) left);
          queryModels.add(bloomQueryModel);
        }
        return queryModels;
      } else {
        String errorMsg = "BloomFilter can only support the 'equal' filter like 'Col = PlainValue'";
        LOGGER.warn(errorMsg);
        throw new RuntimeException(errorMsg);
      }
    } else if (expression instanceof InExpression) {
      Expression left = ((InExpression) expression).getLeft();
      Expression right = ((InExpression) expression).getRight();
      String column;
      if (left instanceof ColumnExpression && right instanceof ListExpression) {
        column = ((ColumnExpression) left).getColumnName();
        if (this.name2Col.containsKey(column)) {
          BloomQueryModel bloomQueryModel =
              buildQueryModelForIn((ColumnExpression) left, (ListExpression) right);
          queryModels.add(bloomQueryModel);
        }
        return queryModels;
      } else if (left instanceof ListExpression && right instanceof ColumnExpression) {
        column = ((ColumnExpression) right).getColumnName();
        if (this.name2Col.containsKey(column)) {
          BloomQueryModel bloomQueryModel =
              buildQueryModelForIn((ColumnExpression) right, (ListExpression) left);
          queryModels.add(bloomQueryModel);
        }
        return queryModels;
      } else {
        String errorMsg = "BloomFilter can only support the 'in' filter like 'Col in PlainValue'";
        LOGGER.warn(errorMsg);
        throw new RuntimeException(errorMsg);
      }
    }  else if (expression instanceof AndExpression) {
      queryModels.addAll(createQueryModel(((AndExpression) expression).getLeft()));
      queryModels.addAll(createQueryModel(((AndExpression) expression).getRight()));
      return queryModels;
    }

    return queryModels;
  }

  private BloomQueryModel buildQueryModelForEqual(ColumnExpression ce, LiteralExpression le) {
    List<byte[]> filterValues = new ArrayList<>();
    byte[] internalFilterValue = getInternalFilterValue(this.name2Col.get(ce.getColumnName()), le);
    filterValues.add(internalFilterValue);
    return new BloomQueryModel(ce.getColumnName(), filterValues);
  }

  /**
   * Note that `in` operator needs at least one match not exactly match. since while doing pruning,
   * we collect all the blocklets that will match the querymodel, this will not be a problem.
   */
  private BloomQueryModel buildQueryModelForIn(ColumnExpression ce, ListExpression le) {
    List<byte[]> filterValues = new ArrayList<>();
    for (Expression child : le.getChildren()) {
      byte[] internalFilterValue = getInternalFilterValue(
          this.name2Col.get(ce.getColumnName()), (LiteralExpression) child);
      filterValues.add(internalFilterValue);
    }
    return new BloomQueryModel(ce.getColumnName(), filterValues);
  }

  private byte[] getInternalFilterValue(CarbonColumn carbonColumn, LiteralExpression le) {
    // convert the filter value to string and apply converters on it to get carbon internal value
    String strFilterValue = null;
    try {
      strFilterValue = le.getExpressionResult().getString();
    } catch (FilterIllegalMemberException e) {
      throw new RuntimeException("Error while resolving filter expression", e);
    }

    Object convertedValue = this.name2Converters.get(carbonColumn.getColName()).convert(
        strFilterValue, badRecordLogHolder);

    byte[] internalFilterValue;
    if (carbonColumn.isMeasure()) {
      // for measures, the value is already the type, just convert it to bytes.
      if (convertedValue == null) {
        convertedValue = DataConvertUtil.getNullValueForMeasure(carbonColumn.getDataType(),
            carbonColumn.getColumnSchema().getScale());
      }
      // Carbon stores boolean as byte. Here we convert it for `getValueAsBytes`
      if (carbonColumn.getDataType().equals(DataTypes.BOOLEAN)) {
        convertedValue = BooleanConvert.boolean2Byte((Boolean)convertedValue);
      }
      internalFilterValue = CarbonUtil.getValueAsBytes(carbonColumn.getDataType(), convertedValue);
    } else if (carbonColumn.getDataType() == DataTypes.DATE) {
      // for dictionary/date columns, convert the surrogate key to bytes
      internalFilterValue = CarbonUtil.getValueAsBytes(DataTypes.INT, convertedValue);
    } else {
      // for non dictionary dimensions, numeric columns will be of original data,
      // so convert the data to bytes
      if (DataTypeUtil.isPrimitiveColumn(carbonColumn.getDataType())) {
        if (convertedValue == null) {
          convertedValue = DataConvertUtil.getNullValueForMeasure(carbonColumn.getDataType(),
              carbonColumn.getColumnSchema().getScale());
        }
        internalFilterValue =
            CarbonUtil.getValueAsBytes(carbonColumn.getDataType(), convertedValue);
      } else {
        internalFilterValue = (byte[]) convertedValue;
      }
    }
    if (internalFilterValue.length == 0) {
      internalFilterValue = CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY;
    }
    return internalFilterValue;
  }

  @Override
  public boolean isScanRequired(FilterResolverIntf filterExp) {
    return true;
  }

  @Override
  public void clear() {
  }

  static class BloomQueryModel {
    private String columnName;
    private List<byte[]> filterValues;

    /**
     * represent an query model will be applyied on bloom index
     *
     * @param columnName bloom index column
     * @param filterValues key for the bloom index,
     *                   this value is converted from user specified filter value in query
     */
    private BloomQueryModel(String columnName, List<byte[]> filterValues) {
      this.columnName = columnName;
      this.filterValues = filterValues;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("BloomQueryModel{");
      sb.append("columnName='").append(columnName).append('\'');
      sb.append(", filterValues=");
      for (byte[] value: filterValues) {
        sb.append(Arrays.toString(value));
      }
      sb.append('}');
      return sb.toString();
    }
  }

  @Override
  public void finish() {

  }
}
