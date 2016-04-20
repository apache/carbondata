/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.carbondata.query.executer.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.common.logging.impl.StandardLogService;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.iterator.CarbonIterator;
import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.core.keygenerator.columnar.impl.MultiDimKeyVarLengthVariableSplitGenerator;
import org.carbondata.core.metadata.CarbonMetadata;
import org.carbondata.core.metadata.CarbonMetadata.Dimension;
import org.carbondata.core.metadata.CarbonMetadata.Measure;
import org.carbondata.core.metadata.SliceMetaData;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.aggregator.dimension.DimensionAggregatorInfo;
import org.carbondata.query.cache.QueryExecutorUtil;
import org.carbondata.query.datastorage.InMemoryTable;
import org.carbondata.query.datastorage.Member;
import org.carbondata.query.datastorage.MemberStore;
import org.carbondata.query.executer.CarbonQueryExecutorModel;
import org.carbondata.query.executer.SliceExecuter;
import org.carbondata.query.executer.exception.QueryExecutionException;
import org.carbondata.query.executer.pagination.impl.QueryResult;
import org.carbondata.query.filters.measurefilter.util.FilterUtil;
import org.carbondata.query.result.ChunkResult;
import org.carbondata.query.result.RowResult;
import org.carbondata.query.result.iterator.ChunkBasedResultIterator;
import org.carbondata.query.result.iterator.ChunkRowIterator;
import org.carbondata.query.result.iterator.DetailQueryResultIterator;
import org.carbondata.query.result.iterator.MemoryBasedResultIterator;
import org.carbondata.query.scanner.impl.CarbonKey;
import org.carbondata.query.scanner.impl.CarbonValue;
import org.carbondata.query.schema.metadata.DimColumnFilterInfo;
import org.carbondata.query.schema.metadata.FilterEvaluatorInfo;
import org.carbondata.query.schema.metadata.SliceExecutionInfo;
import org.carbondata.query.scope.QueryScopeObject;
import org.carbondata.query.util.CarbonEngineLogEvent;
import org.carbondata.query.util.DataTypeConverter;
import org.carbondata.query.util.QueryExecutorUtility;
@Deprecated
public class QueryExecutorImpl extends AbstractQueryExecutor {
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(QueryExecutorImpl.class.getName());

  public QueryExecutorImpl(List<Dimension> dimList, String schemaName, String cubeName,
      QueryScopeObject queryScopeObject) {
    super(dimList, schemaName, cubeName, queryScopeObject);
  }

  @Override public CarbonIterator<RowResult> execute(CarbonQueryExecutorModel queryModel)
      throws QueryExecutionException {
    // setting the query current thread name
    //        Thread.currentThread().setName("Query Thread" + queryModel.getQueryId());

    StandardLogService
        .setThreadName(StandardLogService.getPartitionID(queryModel.getCube().getOnlyCubeName()),
            queryModel.getQueryId());
    LOGGER.info(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
        "Query will be executed on table: " + queryModel.getFactTable());

    LOGGER.info(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
        "Is detail Query: " + queryModel.isDetailQuery());
    // if empty slice then we can return empty result
    if (null == executerProperties.slices || executerProperties.slices.size() == 0
        || checkIfAllEmptySlices(queryModel.getFactTable())) {
      // if there are not slice present then set empty row
      return new ChunkRowIterator(
          new ChunkBasedResultIterator(new MemoryBasedResultIterator(new QueryResult()),
              executerProperties, queryModel));
    }
    // below method will be used to initialize all the properties required
    // for query execution
    initQuery(queryModel);

    // need to handle count(*)
    if (queryModel.isCountStarQuery() && null == queryModel.getFilterExpression()
        && queryModel.getDims().length < 1 && queryModel.getMsrs().size() < 2
        && queryModel.getDimensionAggInfo().size() < 1 && queryModel.getExpressions().size() == 0) {
      LOGGER.info(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
          "Count(*) query: " + queryModel.isCountStarQuery());
      return executeQueryForCountStar(queryModel);
    } else if (null == queryModel.getFilterExpression() && queryModel.getDims().length < 1
        && queryModel.getMsrs().size() == 0 && queryModel.getDimensionAggInfo().size() < 1
        && queryModel.getExpressions().size() == 0) {
      LOGGER.info(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
          "Count(*) query: " + queryModel.isCountStarQuery());
      executerProperties.isFunctionQuery = true;
      return executeQueryForCountStar(queryModel);
    }
    // create a execution info list
    List<SliceExecutionInfo> infos =
        new ArrayList<SliceExecutionInfo>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    SliceMetaData sliceMataData = null;
    SliceExecutionInfo info = null;
    // for each slice we need create a slice info which will be used to
    // execute the query

    int currentSliceIndex = -1;
    String sliceMetadataPath = null;
    CarbonFile carbonFile = null;
    for (InMemoryTable slice : executerProperties.slices) {
      // get the slice metadata for each slice
      currentSliceIndex++;

      if (slice.getTableName().equals(queryModel.getFactTable())) {
        sliceMetadataPath = slice.getRsStore().getSliceMetadataPath(queryModel.getFactTable());
        if (null == sliceMetadataPath) {
          continue;
        }
        carbonFile = FileFactory
            .getCarbonFile(sliceMetadataPath, FileFactory.getFileType(sliceMetadataPath));
        sliceMataData = CarbonUtil.readSliceMetaDataFile(carbonFile);
        if (null == sliceMataData) {
          continue;
        }
        slice.getRsStore().setSliceMetaCache(sliceMataData, queryModel.getFactTable());
        // sliceMataData =
        // slice.getRsStore().getSliceMetaCache(queryModel.getFactTable());
      } else {
        continue;
      }
      // get the slice execution for slice
      info = getSliceExecutionInfo(queryModel, slice, sliceMataData, currentSliceIndex);
      infos.add(info);
    }
    CarbonIterator<QueryResult> queryResultIterator = null;
    if (infos.size() > 0) {
      if (!queryModel.isDetailQuery() || (queryModel.isDetailQuery() && null != queryModel
          .getSortOrder() && queryModel.getSortOrder().length > 0)) {
        queryResultIterator = submitExecutorDetailQuery(infos);
      } else {
        LOGGER.info(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, "Memory based detail query: ");
        infos.get(infos.size() - 1).setFileBasedQuery(false);
        return new ChunkRowIterator(
            new DetailQueryResultIterator(infos, executerProperties, queryModel));
      }
    } else {
      return new ChunkRowIterator(
          new ChunkBasedResultIterator(new MemoryBasedResultIterator(new QueryResult()),
              executerProperties, queryModel));
    }
    return new ChunkRowIterator(
        new ChunkBasedResultIterator(queryResultIterator, executerProperties, queryModel));
  }

  /**
   * Below method will be used for quick filter query execution
   *
   * @param queryModel
   * @return CarbonIterator<RowResult>
   */
  @Override public CarbonIterator<RowResult> executeDimension(CarbonQueryExecutorModel queryModel)
      throws QueryExecutionException {
    StandardLogService
        .setThreadName(StandardLogService.getPartitionID(queryModel.getCube().getOnlyCubeName()),
            queryModel.getQueryId());
    LOGGER.info(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
        "Query will be executed on level file : " + queryModel.getDims()[0].getDimName());

    if (null == executerProperties.slices || executerProperties.slices.size() == 0
        || checkIfAllEmptySlices(queryModel.getFactTable())) {
      // if there are not slice present then set empty row
      return new ChunkRowIterator(
          new ChunkBasedResultIterator(new MemoryBasedResultIterator(new QueryResult()),
              executerProperties, queryModel));
    }

    String memString = null;
    List<CarbonKey> carbonKeys =
        new ArrayList<CarbonKey>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    List<CarbonValue> carbonValues =
        new ArrayList<CarbonValue>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    Dimension dim = queryModel.getDims()[0];
    Object[] row = null;
    boolean dummyRow = false;
    int limit = queryModel.getLimit() == -1 ? Integer.MAX_VALUE : queryModel.getLimit();

    for (InMemoryTable slice : executerProperties.slices) {
      // Fetching member values from level file
      MemberStore ms = slice.getMemberCache(
          dim.getTableName() + "_" + dim.getColName() + "_" + dim.getDimName() + "_" + dim
              .getHierName());

      if (ms != null) {
        Member[][] members = ms.getAllMembers();

        if (members != null) {
          if (queryModel.getLimit() != -1 && null != queryModel.getSortOrder()
              && queryModel.getSortOrder().length > 0) {
            return getSortedMemberData(members, carbonKeys, carbonValues, dim, limit,
                queryModel.getSortOrder());
          }

          for (int j = 0; j < members.length && carbonValues.size() < limit; j++) {
            for (int k = 0; k < members[j].length && carbonValues.size() < limit; k++) {
              row = new Object[1];
              memString = members[j][k].toString();
              if (!memString.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL)) {
                row[0] = DataTypeConverter.getDataBasedOnDataType(memString, dim.getDataType());
                carbonKeys.add(new CarbonKey(row));
                carbonValues.add(new CarbonValue(new MeasureAggregator[0]));
                dummyRow = false;
              } else {
                dummyRow = true;
                if (k > 0) {
                  row[0] = null;
                  carbonKeys.add(new CarbonKey(row));
                  carbonValues.add(new CarbonValue(new MeasureAggregator[0]));
                  dummyRow = false;

                }
              }
            }

          }
        }
      }
    }
    if (dummyRow) {
      row[0] = null;
      carbonKeys.add(new CarbonKey(row));
      carbonValues.add(new CarbonValue(new MeasureAggregator[0]));
    }

    ChunkResult chunkResult = new ChunkResult();
    chunkResult.setKeys(carbonKeys);
    chunkResult.setValues(carbonValues);
    return chunkResult;
  }

  private boolean checkIfAllEmptySlices(String factTable) {
    for (InMemoryTable slice : executerProperties.slices) {
      if (null != slice.getDataCache(factTable)) {
        return false;
      }
    }
    return true;
  }

  private CarbonIterator<RowResult> executeQueryForCountStar(CarbonQueryExecutorModel queryModel)
      throws QueryExecutionException {
    SliceExecuter sliceExec = new ColumnarCountStartExecuter(executerProperties.slices,
        queryModel.getCube().getFactTableName());
    try {
      return new ChunkRowIterator(
          new ChunkBasedResultIterator(sliceExec.executeSlices(null, null), executerProperties,
              queryModel));
    } catch (QueryExecutionException e) {
      LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, e,
          "Error happend on executing slices parallely");
      throw e;
    }
  }

  /**
   * Below method will be used to set the property require for query execution
   *
   * @param queryModel query model
   * @param slice      slice
   * @param metaData   slice meta data
   * @return execution info for slice
   */
  private SliceExecutionInfo getSliceExecutionInfo(CarbonQueryExecutorModel queryModel,
      InMemoryTable slice, SliceMetaData sliceMataData, int currentSliceIndex)
      throws QueryExecutionException {
    // below part of the code is to handle restructure scenario
    // Rest
    List<Dimension> currentDimList =
        new ArrayList<CarbonMetadata.Dimension>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    RestructureHolder holder = new RestructureHolder();

    Dimension[] currentDimTables = RestructureUtil
        .updateRestructureHolder(queryModel.getDims(), sliceMataData, currentDimList, holder,
            executerProperties);
    Dimension[] queryDimensions =
        getSelectedQueryDimensions(queryModel.getDims(), currentDimTables);
    Dimension[] sortDims =
        getSelectedQueryDimensions(queryModel.getSortedDimensions(), queryDimensions);
    holder.metaData = sliceMataData;
    holder.setKeyGenerator(slice.getKeyGenerator(queryModel.getFactTable()));
    holder.setQueryDimsCount(queryDimensions.length);
    if (!executerProperties.globalKeyGenerator
        .equals(slice.getKeyGenerator(queryModel.getFactTable()))) {
      holder.updateRequired = true;
    }
    Measure[] measures = queryModel.getMsrs().toArray(new Measure[queryModel.getMsrs().size()]);
    int[] measureOrdinal = new int[measures.length];
    boolean[] msrExists = new boolean[measures.length];
    Object[] newMsrsDftVal = new Object[measures.length];
    RestructureUtil
        .updateMeasureInfo(sliceMataData, measures, measureOrdinal, msrExists, newMsrsDftVal);

    Object[] sliceUniqueValues = null;
    if (null != slice.getDataCache(queryModel.getFactTable())) {
      sliceUniqueValues = slice.getDataCache(queryModel.getFactTable()).getUniqueValue();
    }
    SliceExecutionInfo info = new SliceExecutionInfo();

    FilterEvaluatorInfo filterInfo = getFilterInfo(queryModel, currentSliceIndex, sliceMataData);
    filterInfo.setComplexTypesWithBlockStartIndex(QueryExecutorUtility
        .getAllComplexTypesBlockStartIndex(executerProperties.complexDimensionsMap));
    QueryExecutorUtility.getComplexDimensionsKeySize(executerProperties.complexDimensionsMap,
        executerProperties.slices.get(currentSliceIndex).getDimensionCardinality());
    filterInfo.setDimensions(executerProperties.dimTables);
    if (null != queryModel.getFilterExpression() && null != slice
        .getDataCache(queryModel.getFactTable())) {
//      info.setFilterEvaluatorTree(
//          //FilterUtil.getFilterEvaluator(queryModel.getFilterExpression(), filterInfo));
    }
    info.setFileBasedQuery(queryModel.isDetailQuery());
    info.setHybridStoreMeta(slice.getHybridStoreModel());
    info.setExecutionRequired(null != slice.getDataCache(queryModel.getFactTable()));
    info.setCustomExpressions(queryModel.getExpressions());
    info.setCustomMeasure(queryModel.isAggTable());
    info.setTableName(queryModel.getFactTable());
    info.setKeyGenerator(slice.getKeyGenerator(queryModel.getFactTable()));
    info.setQueryDimensions(queryDimensions);
    info.setDimensions(currentDimTables);
    info.setCurrentDimTables(currentDimTables);
    info.setMeasureOrdinal(measureOrdinal);
    info.setCubeName(executerProperties.cubeName);
    info.setPartitionId(queryModel.getPartitionId());
    info.setSchemaName(executerProperties.schemaName);
    info.setQueryId(queryModel.getQueryId());
    info.setDetailQuery(queryModel.isDetailQuery());
    //hybrid store related changes
    info.setQueryDimOrdinal(QueryExecutorUtility
        .getSelectedDimensionStoreIndex(queryDimensions, info.getHybridStoreMeta()));
    info.setAllSelectedDimensions(QueryExecutorUtility
        .getAllSelectedDiemnsionStoreIndex(queryDimensions, queryModel.getDimensionAggInfo(),
            executerProperties.aggExpDimensions, info.getHybridStoreMeta()));
    int[] maskedByteRanges = QueryExecutorUtil
        .getMaskedByte(queryDimensions, slice.getKeyGenerator(queryModel.getFactTable()),
            executerProperties.hybridStoreModel);
    int[][] maskedByteRangeForSorting = QueryExecutorUtility
        .getMaskedByteRangeForSorting(sortDims, executerProperties.globalKeyGenerator,
            executerProperties.maskByteRanges);
    info.setMaskedKeyByteSize(maskedByteRanges.length);
    int[] maskedBytesLocal =
        new int[slice.getKeyGenerator(queryModel.getFactTable()).getKeySizeInBytes()];
    QueryExecutorUtil.updateMaskedKeyRanges(maskedBytesLocal, maskedByteRanges);
    holder.maskedByteRanges = maskedBytesLocal;
    info.setActalMaskedByteRanges(executerProperties.maskByteRanges);
    info.setMaskedBytePositions(executerProperties.maskedBytes);
    info.setActualMaskedKeyByteSize(executerProperties.byteCount);
    info.setActualMaxKeyBasedOnDimensions(executerProperties.maxKey);
    info.setActualKeyGenerator(executerProperties.globalKeyGenerator);
    info.setRestructureHolder(holder);
    info.setSlice(slice);
    info.setSlices(executerProperties.slices);
    info.setAvgIndexes(executerProperties.avgIndexes);
    info.setCountMsrsIndex(executerProperties.countMsrIndex);
    info.setDimensionSortOrder(executerProperties.dimSortOrder);
    info.setDimensionSortOrder(queryModel.getSortOrder());
    info.setUniqueValues(sliceUniqueValues);
    info.setOriginalDims(queryDimensions);

    info.setMaskedByteRangeForSorting(maskedByteRangeForSorting);
    executerProperties.sortDimIndexes =
        QueryExecutorUtility.fillSortedDimensions(sortDims, queryModel.getDims());
    info.setSortedDimensionsIndex(executerProperties.sortDimIndexes);
    info.setDimensionMaskKeys(QueryExecutorUtility
        .getMaksedKeyForSorting(sortDims, executerProperties.globalKeyGenerator,
            maskedByteRangeForSorting, executerProperties.maskByteRanges));
    if (slice.getDimensionCardinality().length > 0) {
      info.setColumnarSplitter(new MultiDimKeyVarLengthVariableSplitGenerator(CarbonUtil
          .getDimensionBitLength(slice.getHybridStoreModel().getHybridCardinality(),
              slice.getHybridStoreModel().getDimensionPartitioner()),
          slice.getHybridStoreModel().getColumnSplit()));
    }
    info.setLimit(queryModel.getLimit());
    info.setDetailQuery(queryModel.isDetailQuery());
    info.setTotalNumerOfDimColumns(
        queryModel.getCube().getDimensions(queryModel.getFactTable()).size());
    info.setTotalNumberOfMeasuresInTable(
        queryModel.getCube().getMeasures(queryModel.getFactTable()).size());
    //        info.setMsrSortModel(queryModel.getSortModel());
    long[] startKey = new long[currentDimTables.length];
    long[] endKey;// = new long[currentDimTables.length];
    Map<Dimension, List<DimColumnFilterInfo>> dimensionFilter =
        filterInfo.getInfo().getDimensionFilter();
    if (!dimensionFilter.isEmpty()) {
      getStartKey(dimensionFilter, slice.getKeyGenerator(queryModel.getFactTable()), startKey);
      endKey = getEndKey(currentDimTables, currentSliceIndex);
      getEndKeyWithFilter(dimensionFilter, slice.getKeyGenerator(queryModel.getFactTable()),
          endKey);
    } else {
      endKey = getEndKey(currentDimTables, currentSliceIndex);
    }
    info.setStartKey(startKey);
    info.setEndKey(endKey);
    int recordSize = CarbonCommonConstants.INMEMORY_REOCRD_SIZE_DEFAULT;
    String defaultInMemoryRecordsSize =
        CarbonProperties.getInstance().getProperty(CarbonCommonConstants.INMEMORY_REOCRD_SIZE);
    if (null != defaultInMemoryRecordsSize) {
      try {
        recordSize = Integer.parseInt(defaultInMemoryRecordsSize);
      } catch (NumberFormatException ne) {
        LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
            "Invalid inmemory records size. Using default value");
      }
    }

    LOGGER
        .info(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, "inmemory records size : " + recordSize);
    info.setNumberOfRecordsInMemory(recordSize);
    info.setOutLocation(queryModel.getOutLocation() == null ?
        CarbonUtil.getCarbonStorePath(executerProperties.schemaName, executerProperties.cubeName) :
        queryModel.getOutLocation());
    info.setDimAggInfo(queryModel.getDimensionAggInfo());
    if (null != queryModel.getDimensionAggInfo()) {
      RestructureUtil
          .updateDimensionAggInfo(queryModel.getDimensionAggInfo(), sliceMataData.getDimensions());
    }
    info.setComplexQueryDimensions(QueryExecutorUtility
        .getAllComplexTypesBlockStartIndex(executerProperties.complexDimensionsMap));
    info.setDimensions(executerProperties.dimTables);
    getApplicableDataBlocksForAggDims(queryModel.getDimensionAggInfo(), currentDimTables);
    info.setCurrentSliceIndex(currentSliceIndex);
    info.setMsrMinValue(executerProperties.msrMinValue);
    info.setAggType(executerProperties.aggTypes);
    info.setNoDictionaryType(executerProperties.isNoDictionary);
    info.setAllSelectedMeasures(QueryExecutorUtility
        .getAllSelectedMeasureOrdinals(measures, executerProperties.aggExpMeasures,
            sliceMataData.getMeasures()));
    info.setMeasureStartIndex(executerProperties.measureStartIndex);
    info.setExpressionStartIndex(executerProperties.aggExpressionStartIndex);
    info.setIsMeasureExistis(msrExists);
    info.setMsrDefaultValue(newMsrsDftVal);
    info.setDataTypes(executerProperties.dataTypes);
    info.setMeasureOrdinalMap(executerProperties.measureOrdinalMap);
    return info;
  }

  private void getApplicableDataBlocksForAggDims(List<DimensionAggregatorInfo> dimensionAggInfo,
      Dimension[] currentDimTables) {
    List<Dimension> selectedQueryDimensions =
        new ArrayList<Dimension>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    for (int i = 0; i < dimensionAggInfo.size(); i++) {
      Dimension dim = dimensionAggInfo.get(i).getDim();
      for (int j = 0; j < currentDimTables.length; j++) {
        if (dim.equals(currentDimTables[j])) {
          dim.setDataBlockIndex(currentDimTables[j].getDataBlockIndex());
          dim.setAllApplicableDataBlockIndexs(
              currentDimTables[j].getAllApplicableDataBlockIndexs());
          selectedQueryDimensions.add(dim);
          break;
        }
      }
    }
  }

  private Dimension[] getSelectedQueryDimensions(Dimension[] dims, Dimension[] currentDimTables) {
    List<Dimension> selectedQueryDimensions =
        new ArrayList<Dimension>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    Set<String> dimensionGroup = new LinkedHashSet<String>();
    for (int i = 0; i < dims.length; i++) {
      dimensionGroup.add(dims[i].getHierName());
      for (int j = 0; j < currentDimTables.length; j++) {
        if (dims[i].getTableName().equals(currentDimTables[j].getTableName()) && dims[i]
            .getDimName().equals(currentDimTables[j].getDimName())) {
          dims[i].setDataBlockIndex(currentDimTables[j].getDataBlockIndex());
          dims[i].setAllApplicableDataBlockIndexs(
              currentDimTables[j].getAllApplicableDataBlockIndexs());
          selectedQueryDimensions.add(dims[i]);
          break;
        }
      }
    }

    return selectedQueryDimensions.toArray(new Dimension[selectedQueryDimensions.size()]);
  }

  private CarbonIterator<RowResult> getSortedMemberData(final Member[][] members,
      List<CarbonKey> carbonKeys, List<CarbonValue> carbonValues, Dimension dim, int limit,
      byte[] sortOrder) {
    String memString = "";
    Object[] row = null;
    List<String> vals = new ArrayList<String>();
    for (int j = 0; j < members.length; j++) {
      for (int k = 0; k < members[j].length; k++) {
        memString = members[j][k].toString();
        if (!memString.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL)) {
          vals.add(memString);
        }

      }
    }

    byte sortType = sortOrder[0];
    if (sortType == 1) {
      Collections.sort(vals, Collections.reverseOrder());
    } else {
      Collections.sort(vals);
    }

    for (int i = 0; i < vals.size() && carbonValues.size() < limit; i++) {
      row = new Object[1];
      row[0] = DataTypeConverter.getDataBasedOnDataType(vals.get(i), dim.getDataType());
      carbonKeys.add(new CarbonKey(row));
      carbonValues.add(new CarbonValue(new MeasureAggregator[0]));

    }
    ChunkResult chunkResult = new ChunkResult();
    chunkResult.setKeys(carbonKeys);
    chunkResult.setValues(carbonValues);
    return chunkResult;
  }

  private void getEndKeyWithFilter(Map<Dimension, List<DimColumnFilterInfo>> dimensionFilter,
      KeyGenerator keyGenerator, long[] endKey) {
    for (Entry<Dimension, List<DimColumnFilterInfo>> entry : dimensionFilter.entrySet()) {
      List<DimColumnFilterInfo> values = entry.getValue();
      if (null == values) {
        continue;
      }
      boolean isExcludeFilterPresent = false;
      for (DimColumnFilterInfo info : values) {
        if (!info.isIncludeFilter()) {
          isExcludeFilterPresent = true;
        }
      }
      if (isExcludeFilterPresent) {
        continue;
      }

      for (DimColumnFilterInfo info : values) {
        if (endKey[entry.getKey().getOrdinal()] > info.getFilterList()
            .get(info.getFilterList().size() - 1)) {
          endKey[entry.getKey().getOrdinal()] =
              info.getFilterList().get(info.getFilterList().size() - 1);
        }
      }
    }
  }

  private void getStartKey(Map<Dimension, List<DimColumnFilterInfo>> dimensionFilter,
      KeyGenerator keyGenerator, long[] startKey) {
    for (Entry<Dimension, List<DimColumnFilterInfo>> entry : dimensionFilter.entrySet()) {
      List<DimColumnFilterInfo> values = entry.getValue();
      if (null == values) {
        continue;
      }
      boolean isExcludePresent = false;
      for (DimColumnFilterInfo info : values) {
        if (!info.isIncludeFilter()) {
          isExcludePresent = true;
        }
      }
      if (isExcludePresent) {
        continue;
      }
      for (DimColumnFilterInfo info : values) {
        if (startKey[entry.getKey().getOrdinal()] < info.getFilterList().get(0)) {
          startKey[entry.getKey().getOrdinal()] = info.getFilterList().get(0);
        }
      }
    }
  }

  private FilterEvaluatorInfo getFilterInfo(CarbonQueryExecutorModel queryModel,
      int currentSliceIndex, SliceMetaData sliceMetaData) {
    FilterEvaluatorInfo info = new FilterEvaluatorInfo();
    info.setCurrentSliceIndex(currentSliceIndex);
    info.setFactTableName(queryModel.getFactTable());
    info.setKeyGenerator(executerProperties.slices.get(currentSliceIndex)
        .getKeyGenerator(queryModel.getFactTable()));
    info.setSlices(executerProperties.slices);
    info.setInfo(new QueryFilterInfo());
    info.setNewDimension(sliceMetaData.getNewDimensions());
    info.setNewMeasures(sliceMetaData.getNewMeasures());
    info.setNewDefaultValues(sliceMetaData.getNewMsrDfts());
    info.setNewDimensionDefaultValue(sliceMetaData.getNewDimsDefVals());
    info.setNewDimensionSurrogates(sliceMetaData.getNewDimsSurrogateKeys());
    info.setHybridStoreModel(executerProperties.hybridStoreModel);
    return info;
  }

  private CarbonIterator<QueryResult> submitExecutorDetailQuery(List<SliceExecutionInfo> infos)
      throws QueryExecutionException {
    SliceExecuter sliceExec;
    sliceExec = new ColumnarParallelSliceExecutor();
    try {
      return sliceExec.executeSlices(infos, null);
    } catch (QueryExecutionException e) {
      LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, e,
          "Error happend on executing slices parallely");
      throw e;
    } finally {
      sliceExec = null;
    }
  }
}
