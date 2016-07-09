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
package org.carbondata.query.carbon.executor.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.common.logging.impl.StandardLogService;
import org.carbondata.core.carbon.datastore.BlockIndexStore;
import org.carbondata.core.carbon.datastore.IndexKey;
import org.carbondata.core.carbon.datastore.block.AbstractIndex;
import org.carbondata.core.carbon.datastore.block.SegmentProperties;
import org.carbondata.core.carbon.datastore.exception.IndexBuilderException;
import org.carbondata.core.carbon.metadata.datatype.DataType;
import org.carbondata.core.carbon.metadata.encoder.Encoding;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonMeasure;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.keygenerator.KeyGenException;
import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.query.aggregator.util.MeasureAggregatorFactory;
import org.carbondata.query.carbon.executor.QueryExecutor;
import org.carbondata.query.carbon.executor.exception.QueryExecutionException;
import org.carbondata.query.carbon.executor.infos.AggregatorInfo;
import org.carbondata.query.carbon.executor.infos.BlockExecutionInfo;
import org.carbondata.query.carbon.executor.infos.KeyStructureInfo;
import org.carbondata.query.carbon.executor.infos.SortInfo;
import org.carbondata.query.carbon.executor.util.QueryUtil;
import org.carbondata.query.carbon.executor.util.RestructureUtil;
import org.carbondata.query.carbon.model.DimensionAggregatorInfo;
import org.carbondata.query.carbon.model.QueryDimension;
import org.carbondata.query.carbon.model.QueryMeasure;
import org.carbondata.query.carbon.model.QueryModel;
import org.carbondata.query.filters.measurefilter.util.FilterUtil;

import org.apache.commons.lang3.ArrayUtils;

/**
 * This class provides a skeletal implementation of the {@link QueryExecutor}
 * interface to minimize the effort required to implement this interface. This
 * will be used to prepare all the properties required for query execution
 */
public abstract class AbstractQueryExecutor<E> implements QueryExecutor<E> {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(AbstractQueryExecutor.class.getName());
  /**
   * holder for query properties which will be used to execute the query
   */
  protected QueryExecutorProperties queryProperties;

  public AbstractQueryExecutor() {
    queryProperties = new QueryExecutorProperties();
  }

  /**
   * Below method will be used to fill the executor properties based on query
   * model it will parse the query model and get the detail and fill it in
   * query properties
   *
   * @param queryModel
   */
  protected void initQuery(QueryModel queryModel) throws QueryExecutionException {
    StandardLogService.setThreadName(StandardLogService.getPartitionID(
        queryModel.getAbsoluteTableIdentifier().getCarbonTableIdentifier().getTableName()),
        queryModel.getQueryId());
    LOGGER.info("Query will be executed on table: " + queryModel.getAbsoluteTableIdentifier()
        .getCarbonTableIdentifier().getTableName());

    QueryUtil.resolveQueryModel(queryModel);

    // get the table blocks
    try {
      queryProperties.dataBlocks = BlockIndexStore.getInstance()
          .loadAndGetBlocks(queryModel.getTableBlockInfos(),
              queryModel.getAbsoluteTableIdentifier());
    } catch (IndexBuilderException e) {
      throw new QueryExecutionException(e);
    }
    //
    // // updating the restructuring infos for the query
    queryProperties.keyStructureInfo = getKeyStructureInfo(queryModel,
        queryProperties.dataBlocks.get(queryProperties.dataBlocks.size() - 1).getSegmentProperties()
            .getDimensionKeyGenerator());

    // calculating the total number of aggeragted columns
    int aggTypeCount = queryModel.getQueryMeasures().size() + queryModel.getExpressions().size();

    // as in one dimension multiple aggregator can be selected , so we need
    // to select all the aggregator function
    Iterator<DimensionAggregatorInfo> iterator = queryModel.getDimAggregationInfo().iterator();
    while (iterator.hasNext()) {
      aggTypeCount += iterator.next().getAggList().size();
    }
    int currentIndex = 0;
    String[] aggTypes = new String[aggTypeCount];
    DataType[] dataTypes = new DataType[aggTypeCount];

    // adding query dimension selected in aggregation info
    for (DimensionAggregatorInfo dimensionAggregationInfo : queryModel.getDimAggregationInfo()) {
      for (int i = 0; i < dimensionAggregationInfo.getAggList().size(); i++) {
        aggTypes[currentIndex] = dimensionAggregationInfo.getAggList().get(i);
        dataTypes[currentIndex] = dimensionAggregationInfo.getDim().getDataType();
        currentIndex++;
      }
    }
    // filling the query expression data type and its aggregator
    // in this case both will be custom
    for (int i = 0; i < queryModel.getExpressions().size(); i++) {
      aggTypes[currentIndex] = CarbonCommonConstants.CUSTOM;
      dataTypes[currentIndex] = DataType.STRING;
      currentIndex++;
    }

    for (QueryMeasure carbonMeasure : queryModel.getQueryMeasures()) {
      // adding the data type and aggregation type of all the measure this
      // can be used
      // to select the aggregator
      aggTypes[currentIndex] = carbonMeasure.getAggregateFunction();
      dataTypes[currentIndex] = carbonMeasure.getMeasure().getDataType();
      currentIndex++;
    }
    queryProperties.measureDataTypes = dataTypes;
    queryProperties.measureAggregators = MeasureAggregatorFactory
        .getMeassureAggregator(aggTypes, dataTypes, queryModel.getExpressions());
    // as aggregation will be executed in following order
    // 1.aggregate dimension expression
    // 2. expression
    // 3. query measure
    // so calculating the index of the expression start index
    // and measure column start index
    queryProperties.aggExpressionStartIndex =
        aggTypes.length - queryModel.getExpressions().size() - queryModel.getQueryMeasures().size();
    queryProperties.measureStartIndex = aggTypes.length - queryModel.getQueryMeasures().size();

    // dictionary column unique column id to dictionary mapping
    // which will be used to get column actual data
    queryProperties.columnToDictionayMapping = QueryUtil
        .getDimensionDictionaryDetail(queryModel.getQueryDimension(),
            queryModel.getDimAggregationInfo(), queryModel.getExpressions(),
            queryModel.getAbsoluteTableIdentifier());
    queryModel.setColumnToDictionaryMapping(queryProperties.columnToDictionayMapping);
    // setting the sort dimension index. as it will be updated while getting the sort info
    // so currently setting it to default 0 means sort is not present in any dimension
    queryProperties.sortDimIndexes = new byte[queryModel.getQueryDimension().size()];
  }

  /**
   * Below method will be used to get the key structure info for the uqery
   *
   * @param queryModel   query model
   * @param keyGenerator
   * @return key structure info
   */
  private KeyStructureInfo getKeyStructureInfo(QueryModel queryModel, KeyGenerator keyGenerator) {
    // getting the masked byte range for dictionary column
    int[] maskByteRanges =
        QueryUtil.getMaskedByteRange(queryModel.getQueryDimension(), keyGenerator);

    // getting the masked bytes for query dimension dictionary column
    int[] maskedBytes = QueryUtil.getMaskedByte(keyGenerator.getKeySizeInBytes(), maskByteRanges);

    // max key for the dictionary dimension present in the query
    byte[] maxKey = null;
    try {
      // getting the max key which will be used to masked and get the
      // masked key
      maxKey = QueryUtil.getMaxKeyBasedOnDimensions(queryModel.getQueryDimension(), keyGenerator);
    } catch (KeyGenException e) {
      LOGGER.error(e, "problem while getting the max key");
    }

    KeyStructureInfo restructureInfos = new KeyStructureInfo();
    restructureInfos.setKeyGenerator(keyGenerator);
    restructureInfos.setMaskByteRanges(maskByteRanges);
    restructureInfos.setMaskedBytes(maskedBytes);
    restructureInfos.setMaxKey(maxKey);
    return restructureInfos;
  }

  protected List<BlockExecutionInfo> getBlockExecutionInfos(QueryModel queryModel)
      throws QueryExecutionException {
    initQuery(queryModel);
    List<BlockExecutionInfo> blockExecutionInfoList = new ArrayList<BlockExecutionInfo>();
    // fill all the block execution infos for all the blocks selected in
    // query
    // and query will be executed based on that infos
    for (int i = 0; i < queryProperties.dataBlocks.size(); i++) {
      blockExecutionInfoList
          .add(getBlockExecutionInfoForBlock(queryModel, queryProperties.dataBlocks.get(i)));
    }
    return blockExecutionInfoList;
  }

  /**
   * Below method will be used to get the block execution info which is
   * required to execute any block  based on query model
   *
   * @param queryModel query model from user query
   * @param blockIndex block index
   * @return block execution info
   * @throws QueryExecutionException any failure during block info creation
   */
  protected BlockExecutionInfo getBlockExecutionInfoForBlock(QueryModel queryModel,
      AbstractIndex blockIndex) throws QueryExecutionException {
    BlockExecutionInfo blockExecutionInfo = new BlockExecutionInfo();
    SegmentProperties segmentProperties = blockIndex.getSegmentProperties();
    List<CarbonDimension> tableBlockDimensions = segmentProperties.getDimensions();
    KeyGenerator blockKeyGenerator = segmentProperties.getDimensionKeyGenerator();

    // below is to get only those dimension in query which is present in the
    // table block
    List<QueryDimension> updatedQueryDimension = RestructureUtil
        .getUpdatedQueryDimension(queryModel.getQueryDimension(), tableBlockDimensions);
    // TODO add complex dimension children
    int[] maskByteRangesForBlock =
        QueryUtil.getMaskedByteRange(updatedQueryDimension, blockKeyGenerator);
    int[] maksedByte =
        QueryUtil.getMaskedByte(blockKeyGenerator.getKeySizeInBytes(), maskByteRangesForBlock);
    blockExecutionInfo.setDimensionsExistInQuery(updatedQueryDimension.size() > 0);
    blockExecutionInfo.setDataBlock(blockIndex);
    blockExecutionInfo.setBlockKeyGenerator(blockKeyGenerator);
    // adding aggregation info for query
    blockExecutionInfo.setAggregatorInfo(getAggregatorInfoForBlock(queryModel, blockIndex));
    // adding custom aggregate expression of query
    blockExecutionInfo.setCustomAggregateExpressions(queryModel.getExpressions());

    // setting the limit
    blockExecutionInfo.setLimit(queryModel.getLimit());
    // setting whether detail query or not
    blockExecutionInfo.setDetailQuery(queryModel.isDetailQuery());
    // setting whether raw record query or not
    blockExecutionInfo.setRawRecordDetailQuery(queryModel.isForcedDetailRawQuery());
    // setting the masked byte of the block which will be
    // used to update the unpack the older block keys
    blockExecutionInfo.setMaskedByteForBlock(maksedByte);
    // total number dimension
    blockExecutionInfo
        .setTotalNumberDimensionBlock(segmentProperties.getDimensionOrdinalToBlockMapping().size());
    blockExecutionInfo
        .setTotalNumberOfMeasureBlock(segmentProperties.getMeasuresOrdinalToBlockMapping().size());
    // to check whether older block key update is required or not
    blockExecutionInfo.setFixedKeyUpdateRequired(
        !blockKeyGenerator.equals(queryProperties.keyStructureInfo.getKeyGenerator()));
    IndexKey startIndexKey = null;
    IndexKey endIndexKey = null;
    if (null != queryModel.getFilterExpressionResolverTree()) {
      // loading the filter executer tree for filter evaluation
      blockExecutionInfo.setFilterExecuterTree(FilterUtil
          .getFilterExecuterTree(queryModel.getFilterExpressionResolverTree(), segmentProperties));
      List<IndexKey> listOfStartEndKeys = new ArrayList<IndexKey>(2);
      FilterUtil.traverseResolverTreeAndGetStartAndEndKey(segmentProperties,
          queryModel.getAbsoluteTableIdentifier(), queryModel.getFilterExpressionResolverTree(),
          listOfStartEndKeys);
      startIndexKey = listOfStartEndKeys.get(0);
      endIndexKey = listOfStartEndKeys.get(1);
    } else {
      try {
        startIndexKey = FilterUtil.prepareDefaultStartIndexKey(segmentProperties);
        endIndexKey = FilterUtil.prepareDefaultEndIndexKey(segmentProperties);
      } catch (KeyGenException e) {
        throw new QueryExecutionException(e);
      }
    }
    blockExecutionInfo.setFileType(
        FileFactory.getFileType(queryModel.getAbsoluteTableIdentifier().getStorePath()));
    //setting the start index key of the block node
    blockExecutionInfo.setStartKey(startIndexKey);
    //setting the end index key of the block node
    blockExecutionInfo.setEndKey(endIndexKey);
    // expression dimensions
    List<CarbonDimension> expressionDimensions =
        new ArrayList<CarbonDimension>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    // expression measure
    List<CarbonMeasure> expressionMeasures =
        new ArrayList<CarbonMeasure>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    // to get all the dimension and measure which required to get the chunk
    // indexes to be read from file
    QueryUtil.extractDimensionsAndMeasuresFromExpression(queryModel.getExpressions(),
        expressionDimensions, expressionMeasures);
    // setting all the dimension chunk indexes to be read from file
    blockExecutionInfo.setAllSelectedDimensionBlocksIndexes(QueryUtil
        .getDimensionsBlockIndexes(updatedQueryDimension,
            segmentProperties.getDimensionOrdinalToBlockMapping(),
            queryModel.getDimAggregationInfo(), expressionDimensions));
    // setting all the measure chunk indexes to be read from file
    blockExecutionInfo.setAllSelectedMeasureBlocksIndexes(QueryUtil
        .getMeasureBlockIndexes(queryModel.getQueryMeasures(), expressionMeasures,
            segmentProperties.getMeasuresOrdinalToBlockMapping()));
    // setting the key structure info which will be required
    // to update the older block key with new key generator
    blockExecutionInfo.setKeyStructureInfo(queryProperties.keyStructureInfo);
    // setting the size of fixed key column (dictionary column)
    blockExecutionInfo.setFixedLengthKeySize(getKeySize(updatedQueryDimension, segmentProperties));
    Set<Integer> dictionaryColumnBlockIndex = new HashSet<Integer>();
    List<Integer> noDictionaryColumnBlockIndex = new ArrayList<Integer>();
    // get the block index to be read from file for query dimension
    // for both dictionary columns and no dictionary columns
    QueryUtil.fillQueryDimensionsBlockIndexes(updatedQueryDimension,
        segmentProperties.getDimensionOrdinalToBlockMapping(), dictionaryColumnBlockIndex,
        noDictionaryColumnBlockIndex);
    int[] queryDictionaruColumnBlockIndexes = ArrayUtils.toPrimitive(
        dictionaryColumnBlockIndex.toArray(new Integer[dictionaryColumnBlockIndex.size()]));
    // need to sort the dictionary column as for all dimension
    // column key will be filled based on key order
    Arrays.sort(queryDictionaruColumnBlockIndexes);
    blockExecutionInfo.setDictionaryColumnBlockIndex(queryDictionaruColumnBlockIndexes);
    // setting the no dictionary column block indexes
    blockExecutionInfo.setNoDictionaryBlockIndexes(ArrayUtils.toPrimitive(
        noDictionaryColumnBlockIndex.toArray(new Integer[noDictionaryColumnBlockIndex.size()])));
    // setting column id to dictionary mapping
    blockExecutionInfo.setColumnIdToDcitionaryMapping(queryProperties.columnToDictionayMapping);
    // setting each column value size
    blockExecutionInfo.setEachColumnValueSize(segmentProperties.getEachDimColumnValueSize());
    blockExecutionInfo.setDimensionAggregator(QueryUtil
        .getDimensionDataAggregatorList1(queryModel.getDimAggregationInfo(),
            segmentProperties.getDimensionOrdinalToBlockMapping(),
            segmentProperties.getColumnGroupAndItsKeygenartor(),
            queryProperties.columnToDictionayMapping));
    try {
      // to set column group and its key structure info which will be used
      // to
      // for getting the column group column data in case of final row
      // and in case of dimension aggregation
      blockExecutionInfo.setColumnGroupToKeyStructureInfo(
          QueryUtil.getColumnGroupKeyStructureInfo(updatedQueryDimension, segmentProperties));
    } catch (KeyGenException e) {
      throw new QueryExecutionException(e);
    }
    return blockExecutionInfo;
  }

  /**
   * This method will be used to get fixed key length size this will be used
   * to create a row from column chunk
   *
   * @param queryDimension    query dimension
   * @param blockMetadataInfo block metadata info
   * @return key size
   */
  private int getKeySize(List<QueryDimension> queryDimension, SegmentProperties blockMetadataInfo) {
    List<Integer> fixedLengthDimensionOrdinal =
        new ArrayList<Integer>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    int counter = 0;
    while (counter < queryDimension.size()) {
      if (queryDimension.get(counter).getDimension().numberOfChild() > 0) {
        counter += queryDimension.get(counter).getDimension().numberOfChild();
        continue;
      } else if (!CarbonUtil.hasEncoding(queryDimension.get(counter).getDimension().getEncoder(),
          Encoding.DICTIONARY)) {
        counter++;
      } else {
        fixedLengthDimensionOrdinal.add(queryDimension.get(counter).getDimension().getKeyOrdinal());
        counter++;
      }
    }
    int[] dictioanryColumnOrdinal = ArrayUtils.toPrimitive(
        fixedLengthDimensionOrdinal.toArray(new Integer[fixedLengthDimensionOrdinal.size()]));
    if (dictioanryColumnOrdinal.length > 0) {
      return blockMetadataInfo.getFixedLengthKeySplitter()
          .getKeySizeByBlock(dictioanryColumnOrdinal);
    }
    return 0;
  }

  /**
   * Below method will be used to get the sort information which will be
   * required during sorting the data on dimension column
   *
   * @param queryModel query model
   * @return Sort infos
   * @throws QueryExecutionException if problem while
   */
  protected SortInfo getSortInfos(QueryModel queryModel) throws QueryExecutionException {

    // get the masked by range for order by dimension
    int[][] maskedByteRangeForSorting = QueryUtil
        .getMaskedByteRangeForSorting(queryModel.getSortDimension(),
            queryProperties.keyStructureInfo.getKeyGenerator(),
            queryProperties.keyStructureInfo.getMaskByteRanges());
    // get masked key for sorting
    byte[][] maksedKeyForSorting = QueryUtil.getMaksedKeyForSorting(queryModel.getSortDimension(),
        queryProperties.keyStructureInfo.getKeyGenerator(), maskedByteRangeForSorting,
        queryProperties.keyStructureInfo.getMaskByteRanges());
    // fill sort dimension indexes
    queryProperties.sortDimIndexes = QueryUtil
        .getSortDimensionIndexes(queryModel.getSortDimension(), queryModel.getQueryDimension());
    SortInfo sortInfos = new SortInfo();
    sortInfos.setDimensionMaskKeyForSorting(maksedKeyForSorting);
    sortInfos.setDimensionSortOrder(queryModel.getSortOrder());
    sortInfos.setMaskedByteRangeForSorting(maskedByteRangeForSorting);
    sortInfos.setSortDimensionIndex(queryProperties.sortDimIndexes);
    sortInfos.setSortDimension(queryModel.getSortDimension());
    return sortInfos;
  }

  /**
   * Below method will be used to get the aggrgator info for the query
   *
   * @param queryModel query model
   * @param tableBlock table block
   * @return aggregator info
   */
  private AggregatorInfo getAggregatorInfoForBlock(QueryModel queryModel,
      AbstractIndex tableBlock) {
    // getting the aggregate infos which will be used during aggregation
    AggregatorInfo aggregatorInfos = RestructureUtil
        .getAggregatorInfos(queryModel.getQueryMeasures(),
            tableBlock.getSegmentProperties().getMeasures());
    // setting the index of expression in measure aggregators
    aggregatorInfos.setExpressionAggregatorStartIndex(queryProperties.aggExpressionStartIndex);
    // setting the index of measure columns in measure aggregators
    aggregatorInfos.setMeasureAggregatorStartIndex(queryProperties.measureStartIndex);
    // setting the measure aggregator for all aggregation function selected
    // in query
    aggregatorInfos.setMeasuresAggreagators(queryProperties.measureAggregators);
    aggregatorInfos.setMeasureDataTypes(queryProperties.measureDataTypes);
    return aggregatorInfos;
  }

}
