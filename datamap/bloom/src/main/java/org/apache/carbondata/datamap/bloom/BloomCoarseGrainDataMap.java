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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.dev.DataMapModel;
import org.apache.carbondata.core.datamap.dev.cgdatamap.CoarseGrainDataMap;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.datastore.page.encoding.bool.BooleanConvert;
import org.apache.carbondata.core.devapi.DictionaryGenerationException;
import org.apache.carbondata.core.indexstore.Blocklet;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.RelationIdentifier;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.LiteralExpression;
import org.apache.carbondata.core.scan.expression.conditional.EqualToExpression;
import org.apache.carbondata.core.scan.expression.conditional.InExpression;
import org.apache.carbondata.core.scan.expression.conditional.ListExpression;
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

/**
 * BloomDataCoarseGrainMap is constructed in blocklet level. For each indexed column,
 * a bloom filter is constructed to indicate whether a value belongs to this blocklet.
 * More information of the index file can be found in the corresponding datamap writer.
 */
@InterfaceAudience.Internal
public class BloomCoarseGrainDataMap extends CoarseGrainDataMap {
  private static final LogService LOGGER =
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
  public void init(DataMapModel dataMapModel) throws IOException {
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
    String parentTablePath = getAncestorTablePath(carbonTable);

    try {
      this.name2Converters = new HashMap<>(indexedColumn.size());
      AbsoluteTableIdentifier absoluteTableIdentifier = AbsoluteTableIdentifier
          .from(carbonTable.getTablePath(), carbonTable.getCarbonTableIdentifier());
      String nullFormat = "\\N";
      Map<Object, Integer>[] localCaches = new Map[indexedColumn.size()];

      for (int i = 0; i < indexedColumn.size(); i++) {
        localCaches[i] = new ConcurrentHashMap<>();
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
            .createFieldEncoder(dataField, absoluteTableIdentifier, i, nullFormat, null, false,
                localCaches[i], false, parentTablePath, false);
        this.name2Converters.put(indexedColumn.get(i).getColName(), fieldConverter);
      }
    } catch (IOException e) {
      LOGGER.error(e, "Exception occurs while init index columns");
      throw new RuntimeException(e);
    }
    this.badRecordLogHolder = new BadRecordLogHolder();
    this.badRecordLogHolder.setLogged(false);
  }

  /**
   * recursively find the ancestor's table path. This is used for dictionary scenario
   * where preagg will use the dictionary of the parent table.
   */
  private String getAncestorTablePath(CarbonTable currentTable) {
    if (!currentTable.isChildDataMap()) {
      return currentTable.getTablePath();
    }

    RelationIdentifier parentIdentifier =
        currentTable.getTableInfo().getParentRelationIdentifiers().get(0);
    CarbonTable parentTable = CarbonMetadata.getInstance().getCarbonTable(
        parentIdentifier.getDatabaseName(), parentIdentifier.getTableName());
    return getAncestorTablePath(parentTable);
  }

  @Override
  public List<Blocklet> prune(FilterResolverIntf filterExp, SegmentProperties segmentProperties,
      List<PartitionSpec> partitions) throws IOException {
    Set<Blocklet> hitBlocklets = new HashSet<>();
    if (filterExp == null) {
      // null is different from empty here. Empty means after pruning, no blocklet need to scan.
      return null;
    }

    List<BloomQueryModel> bloomQueryModels;
    try {
      bloomQueryModels = createQueryModel(filterExp.getFilterExpression());
    } catch (DictionaryGenerationException | UnsupportedEncodingException e) {
      LOGGER.error(e, "Exception occurs while creating query model");
      throw new RuntimeException(e);
    }
    for (BloomQueryModel bloomQueryModel : bloomQueryModels) {
      LOGGER.debug("prune blocklet for query: " + bloomQueryModel);
      BloomCacheKeyValue.CacheKey cacheKey = new BloomCacheKeyValue.CacheKey(
          this.indexPath.toString(), bloomQueryModel.columnName);
      BloomCacheKeyValue.CacheValue cacheValue = cache.get(cacheKey);
      List<CarbonBloomFilter> bloomIndexList = cacheValue.getBloomFilters();
      for (CarbonBloomFilter bloomFilter : bloomIndexList) {
        if (needShardPrune && !filteredShard.contains(bloomFilter.getShardName())) {
          // skip shard which has been pruned in Main datamap
          continue;
        }
        boolean scanRequired = bloomFilter.membershipTest(new Key(bloomQueryModel.filterValue));
        if (scanRequired) {
          LOGGER.debug(String.format("BloomCoarseGrainDataMap: Need to scan -> blocklet#%s",
              String.valueOf(bloomFilter.getBlockletNo())));
          Blocklet blocklet = new Blocklet(bloomFilter.getShardName(),
                  String.valueOf(bloomFilter.getBlockletNo()));
          hitBlocklets.add(blocklet);
        } else {
          LOGGER.debug(String.format("BloomCoarseGrainDataMap: Skip scan -> blocklet#%s",
              String.valueOf(bloomFilter.getBlockletNo())));
        }
      }
    }
    return new ArrayList<>(hitBlocklets);
  }

  private List<BloomQueryModel> createQueryModel(Expression expression)
      throws DictionaryGenerationException, UnsupportedEncodingException {
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
          List<BloomQueryModel> models =
              buildQueryModelForIn((ColumnExpression) left, (ListExpression) right);
          queryModels.addAll(models);
        }
        return queryModels;
      } else if (left instanceof ListExpression && right instanceof ColumnExpression) {
        column = ((ColumnExpression) right).getColumnName();
        if (this.name2Col.containsKey(column)) {
          List<BloomQueryModel> models =
              buildQueryModelForIn((ColumnExpression) right, (ListExpression) left);
          queryModels.addAll(models);
        }
        return queryModels;
      } else {
        String errorMsg = "BloomFilter can only support the 'in' filter like 'Col in PlainValue'";
        LOGGER.warn(errorMsg);
        throw new RuntimeException(errorMsg);
      }
    }

    for (Expression child : expression.getChildren()) {
      queryModels.addAll(createQueryModel(child));
    }
    return queryModels;
  }

  private BloomQueryModel buildQueryModelForEqual(ColumnExpression ce,
      LiteralExpression le) throws DictionaryGenerationException, UnsupportedEncodingException {
    String columnName = ce.getColumnName();
    DataType dataType = ce.getDataType();
    Object expressionValue = le.getLiteralExpValue();
    Object literalValue;
    // note that if the datatype is date/timestamp, the expressionValue is long type.
    if (null == expressionValue) {
      literalValue = null;
    } else if (le.getLiteralExpDataType() == DataTypes.DATE) {
      DateFormat format = new SimpleDateFormat(CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT);
      // the below settings are set statically according to DateDirectDirectionaryGenerator
      format.setLenient(false);
      format.setTimeZone(TimeZone.getTimeZone("GMT"));

      literalValue = format.format(new Date((long) expressionValue / 1000));
    } else if (le.getLiteralExpDataType() == DataTypes.TIMESTAMP) {
      DateFormat format =
          new SimpleDateFormat(CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT);
      // the below settings are set statically according to TimeStampDirectDirectionaryGenerator
      format.setLenient(false);
      literalValue = format.format(new Date((long) expressionValue / 1000));
    } else {
      literalValue = expressionValue;
    }

    return buildQueryModelInternal(this.name2Col.get(columnName), literalValue, dataType);
  }

  /**
   * for `in` expressions, we use `equal` to handle it.
   * Note that `in` operator needs at least one match not exactly match. since while doing pruning,
   * we collect all the blocklets that will match the querymodel, this will not be a problem.
   */
  private List<BloomQueryModel> buildQueryModelForIn(ColumnExpression ce, ListExpression le)
      throws DictionaryGenerationException, UnsupportedEncodingException {
    List<BloomQueryModel> queryModels = new ArrayList<>();
    for (Expression child : le.getChildren()) {
      queryModels.add(buildQueryModelForEqual(ce, (LiteralExpression) child));
    }
    return queryModels;
  }

  private BloomQueryModel buildQueryModelInternal(CarbonColumn carbonColumn,
      Object filterLiteralValue, DataType filterValueDataType) throws
      DictionaryGenerationException, UnsupportedEncodingException {
    // convert the filter value to string and apply convertes on it to get carbon internal value
    String strFilterValue = null;
    if (null != filterLiteralValue) {
      strFilterValue = String.valueOf(filterLiteralValue);
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
    } else if (carbonColumn.hasEncoding(Encoding.DIRECT_DICTIONARY) ||
        carbonColumn.hasEncoding(Encoding.DICTIONARY)) {
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
    return new BloomQueryModel(carbonColumn.getColName(), internalFilterValue);
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
    private byte[] filterValue;

    /**
     * represent an query model will be applyied on bloom index
     *
     * @param columnName bloom index column
     * @param filterValue key for the bloom index,
     *                   this value is converted from user specified filter value in query
     */
    private BloomQueryModel(String columnName, byte[] filterValue) {
      this.columnName = columnName;
      this.filterValue = filterValue;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("BloomQueryModel{");
      sb.append("columnName='").append(columnName).append('\'');
      sb.append(", filterValue=").append(Arrays.toString(filterValue));
      sb.append('}');
      return sb.toString();
    }
  }

  @Override
  public void finish() {

  }
}
