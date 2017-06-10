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
package org.apache.carbondata.core.scan.executor.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.common.logging.impl.StandardLogService;
import org.apache.carbondata.core.cache.CacheProvider;
import org.apache.carbondata.core.cache.CacheType;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.constants.CarbonV3DataFormatConstants;
import org.apache.carbondata.core.datastore.BlockIndexStore;
import org.apache.carbondata.core.datastore.IndexKey;
import org.apache.carbondata.core.datastore.block.AbstractIndex;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.datastore.block.TableBlockUniqueIdentifier;
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.core.keygenerator.KeyGenerator;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.scan.executor.QueryExecutor;
import org.apache.carbondata.core.scan.executor.exception.QueryExecutionException;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.executor.util.QueryUtil;
import org.apache.carbondata.core.scan.executor.util.RestructureUtil;
import org.apache.carbondata.core.scan.filter.FilterUtil;
import org.apache.carbondata.core.scan.model.QueryDimension;
import org.apache.carbondata.core.scan.model.QueryMeasure;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.stats.QueryStatistic;
import org.apache.carbondata.core.stats.QueryStatisticsConstants;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonTimeStatisticsFactory;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonStorePath;

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

  /**
   * query result iterator which will execute the query
   * and give the result
   */
  protected CarbonIterator queryIterator;

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
  protected void initQuery(QueryModel queryModel) throws IOException {
    StandardLogService.setThreadName(StandardLogService.getPartitionID(
        queryModel.getAbsoluteTableIdentifier().getCarbonTableIdentifier().getTableName()),
        queryModel.getQueryId());
    LOGGER.info("Query will be executed on table: " + queryModel.getAbsoluteTableIdentifier()
        .getCarbonTableIdentifier().getTableName());
    // add executor service for query execution
    queryProperties.executorService = Executors.newCachedThreadPool();
    // Initializing statistics list to record the query statistics
    // creating copy on write to handle concurrent scenario
    queryProperties.queryStatisticsRecorder =
        CarbonTimeStatisticsFactory.createExecutorRecorder(queryModel.getQueryId());
    queryModel.setStatisticsRecorder(queryProperties.queryStatisticsRecorder);
    QueryUtil.resolveQueryModel(queryModel);
    QueryStatistic queryStatistic = new QueryStatistic();
    // sort the block info
    // so block will be loaded in sorted order this will be required for
    // query execution
    Collections.sort(queryModel.getTableBlockInfos());
    // get the table blocks
    CacheProvider cacheProvider = CacheProvider.getInstance();
    BlockIndexStore<TableBlockUniqueIdentifier, AbstractIndex> cache =
        (BlockIndexStore) cacheProvider
            .createCache(CacheType.EXECUTOR_BTREE, queryModel.getTable().getStorePath());
    // remove the invalid table blocks, block which is deleted or compacted
    cache.removeTableBlocks(queryModel.getInvalidSegmentIds(),
        queryModel.getAbsoluteTableIdentifier());
    List<TableBlockUniqueIdentifier> tableBlockUniqueIdentifiers =
        prepareTableBlockUniqueIdentifier(queryModel.getTableBlockInfos(),
            queryModel.getAbsoluteTableIdentifier());
    cache.removeTableBlocksIfHorizontalCompactionDone(queryModel);
    queryProperties.dataBlocks = cache.getAll(tableBlockUniqueIdentifiers);
    queryStatistic
        .addStatistics(QueryStatisticsConstants.LOAD_BLOCKS_EXECUTOR, System.currentTimeMillis());
    queryProperties.queryStatisticsRecorder.recordStatistics(queryStatistic);

    // calculating the total number of aggeragted columns
    int aggTypeCount = queryModel.getQueryMeasures().size();

    int currentIndex = 0;
    DataType[] dataTypes = new DataType[aggTypeCount];

    for (QueryMeasure carbonMeasure : queryModel.getQueryMeasures()) {
      // adding the data type and aggregation type of all the measure this
      // can be used
      // to select the aggregator
      dataTypes[currentIndex] = carbonMeasure.getMeasure().getDataType();
      currentIndex++;
    }
    queryProperties.measureDataTypes = dataTypes;
    // as aggregation will be executed in following order
    // 1.aggregate dimension expression
    // 2. expression
    // 3. query measure
    // so calculating the index of the expression start index
    // and measure column start index
    queryProperties.filterMeasures = new HashSet<>();
    queryProperties.complexFilterDimension = new HashSet<>();
    QueryUtil.getAllFilterDimensions(queryModel.getFilterExpressionResolverTree(),
        queryProperties.complexFilterDimension, queryProperties.filterMeasures);

    queryStatistic = new QueryStatistic();
    // dictionary column unique column id to dictionary mapping
    // which will be used to get column actual data
    queryProperties.columnToDictionayMapping = QueryUtil
        .getDimensionDictionaryDetail(queryModel.getQueryDimension(),
            queryProperties.complexFilterDimension, queryModel.getAbsoluteTableIdentifier());
    queryStatistic
        .addStatistics(QueryStatisticsConstants.LOAD_DICTIONARY, System.currentTimeMillis());
    queryProperties.queryStatisticsRecorder.recordStatistics(queryStatistic);
    queryModel.setColumnToDictionaryMapping(queryProperties.columnToDictionayMapping);
  }

  private List<TableBlockUniqueIdentifier> prepareTableBlockUniqueIdentifier(
      List<TableBlockInfo> tableBlockInfos, AbsoluteTableIdentifier absoluteTableIdentifier) {
    List<TableBlockUniqueIdentifier> tableBlockUniqueIdentifiers =
        new ArrayList<>(tableBlockInfos.size());
    for (TableBlockInfo blockInfo : tableBlockInfos) {
      tableBlockUniqueIdentifiers
          .add(new TableBlockUniqueIdentifier(absoluteTableIdentifier, blockInfo));
    }
    return tableBlockUniqueIdentifiers;
  }

  protected List<BlockExecutionInfo> getBlockExecutionInfos(QueryModel queryModel)
      throws IOException, QueryExecutionException {
    initQuery(queryModel);
    List<BlockExecutionInfo> blockExecutionInfoList = new ArrayList<BlockExecutionInfo>();
    // fill all the block execution infos for all the blocks selected in
    // query
    // and query will be executed based on that infos
    for (int i = 0; i < queryProperties.dataBlocks.size(); i++) {
      blockExecutionInfoList.add(
          getBlockExecutionInfoForBlock(queryModel, queryProperties.dataBlocks.get(i),
              queryModel.getTableBlockInfos().get(i).getBlockletInfos().getStartBlockletNumber(),
              queryModel.getTableBlockInfos().get(i).getBlockletInfos().getNumberOfBlockletToScan(),
              queryModel.getTableBlockInfos().get(i).getFilePath()));
    }
    if (null != queryModel.getStatisticsRecorder()) {
      QueryStatistic queryStatistic = new QueryStatistic();
      queryStatistic.addCountStatistic(QueryStatisticsConstants.SCAN_BLOCKS_NUM,
          blockExecutionInfoList.size());
      queryModel.getStatisticsRecorder().recordStatistics(queryStatistic);
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
      AbstractIndex blockIndex, int startBlockletIndex, int numberOfBlockletToScan, String filePath)
      throws QueryExecutionException {
    BlockExecutionInfo blockExecutionInfo = new BlockExecutionInfo();
    SegmentProperties segmentProperties = blockIndex.getSegmentProperties();
    List<CarbonDimension> tableBlockDimensions = segmentProperties.getDimensions();
    KeyGenerator blockKeyGenerator = segmentProperties.getDimensionKeyGenerator();

    // below is to get only those dimension in query which is present in the
    // table block
    List<QueryDimension> currentBlockQueryDimensions = RestructureUtil
        .createDimensionInfoAndGetCurrentBlockQueryDimension(blockExecutionInfo,
            queryModel.getQueryDimension(), tableBlockDimensions,
            segmentProperties.getComplexDimensions());
    int tableFactPathLength = CarbonStorePath
        .getCarbonTablePath(queryModel.getAbsoluteTableIdentifier().getStorePath(),
            queryModel.getAbsoluteTableIdentifier().getCarbonTableIdentifier()).getFactDir()
        .length() + 1;
    blockExecutionInfo.setBlockId(filePath.substring(tableFactPathLength));
    blockExecutionInfo.setStartBlockletIndex(startBlockletIndex);
    blockExecutionInfo.setNumberOfBlockletToScan(numberOfBlockletToScan);
    blockExecutionInfo.setQueryDimensions(currentBlockQueryDimensions
        .toArray(new QueryDimension[currentBlockQueryDimensions.size()]));
    // get measures present in the current block
    List<QueryMeasure> currentBlockQueryMeasures =
        getCurrentBlockQueryMeasures(blockExecutionInfo, queryModel, blockIndex);
    blockExecutionInfo.setQueryMeasures(
        currentBlockQueryMeasures.toArray(new QueryMeasure[currentBlockQueryMeasures.size()]));
    blockExecutionInfo.setDataBlock(blockIndex);
    blockExecutionInfo.setBlockKeyGenerator(blockKeyGenerator);
    // setting whether raw record query or not
    blockExecutionInfo.setRawRecordDetailQuery(queryModel.isForcedDetailRawQuery());
    // total number dimension
    blockExecutionInfo
        .setTotalNumberDimensionBlock(segmentProperties.getDimensionOrdinalToBlockMapping().size());
    blockExecutionInfo
        .setTotalNumberOfMeasureBlock(segmentProperties.getMeasuresOrdinalToBlockMapping().size());
    blockExecutionInfo.setAbsoluteTableIdentifier(queryModel.getAbsoluteTableIdentifier());
    blockExecutionInfo.setComplexDimensionInfoMap(QueryUtil
        .getComplexDimensionsMap(currentBlockQueryDimensions,
            segmentProperties.getDimensionOrdinalToBlockMapping(),
            segmentProperties.getEachComplexDimColumnValueSize(),
            queryProperties.columnToDictionayMapping, queryProperties.complexFilterDimension));
    IndexKey startIndexKey = null;
    IndexKey endIndexKey = null;
    if (null != queryModel.getFilterExpressionResolverTree()) {
      // loading the filter executer tree for filter evaluation
      blockExecutionInfo.setFilterExecuterTree(FilterUtil
          .getFilterExecuterTree(queryModel.getFilterExpressionResolverTree(), segmentProperties,
              blockExecutionInfo.getComlexDimensionInfoMap()));
      List<IndexKey> listOfStartEndKeys = new ArrayList<IndexKey>(2);
      FilterUtil.traverseResolverTreeAndGetStartAndEndKey(segmentProperties,
          queryModel.getFilterExpressionResolverTree(), listOfStartEndKeys);
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
    // setting all the dimension chunk indexes to be read from file
    int numberOfElementToConsider = 0;
    // list of dimensions to be projected
    Set<Integer> allProjectionListDimensionIdexes = new LinkedHashSet<>();
    // create a list of filter dimensions present in the current block
    Set<CarbonDimension> currentBlockFilterDimensions =
        getCurrentBlockFilterDimensions(queryProperties.complexFilterDimension, segmentProperties);
    int[] dimensionsBlockIndexes = QueryUtil.getDimensionsBlockIndexes(currentBlockQueryDimensions,
        segmentProperties.getDimensionOrdinalToBlockMapping(), expressionDimensions,
        currentBlockFilterDimensions, allProjectionListDimensionIdexes);
    int numberOfColumnToBeReadInOneIO = Integer.parseInt(CarbonProperties.getInstance()
        .getProperty(CarbonV3DataFormatConstants.NUMBER_OF_COLUMN_TO_READ_IN_IO,
            CarbonV3DataFormatConstants.NUMBER_OF_COLUMN_TO_READ_IN_IO_DEFAULTVALUE));

    if (dimensionsBlockIndexes.length > 0) {
      numberOfElementToConsider = dimensionsBlockIndexes[dimensionsBlockIndexes.length - 1]
          == segmentProperties.getBlockTodimensionOrdinalMapping().size() - 1 ?
          dimensionsBlockIndexes.length - 1 :
          dimensionsBlockIndexes.length;
      blockExecutionInfo.setAllSelectedDimensionBlocksIndexes(CarbonUtil
          .getRangeIndex(dimensionsBlockIndexes, numberOfElementToConsider,
              numberOfColumnToBeReadInOneIO));
    } else {
      blockExecutionInfo.setAllSelectedDimensionBlocksIndexes(new int[0][0]);
    }
    // get the list of updated filter measures present in the current block
    Set<CarbonMeasure> currentBlockFilterMeasures =
        getCurrentBlockFilterMeasures(queryProperties.filterMeasures, segmentProperties);
    // list of measures to be projected
    List<Integer> allProjectionListMeasureIndexes = new ArrayList<>();
    int[] measureBlockIndexes = QueryUtil
        .getMeasureBlockIndexes(currentBlockQueryMeasures, expressionMeasures,
            segmentProperties.getMeasuresOrdinalToBlockMapping(), currentBlockFilterMeasures,
            allProjectionListMeasureIndexes);
    if (measureBlockIndexes.length > 0) {

      numberOfElementToConsider = measureBlockIndexes[measureBlockIndexes.length - 1]
          == segmentProperties.getMeasures().size() - 1 ?
          measureBlockIndexes.length - 1 :
          measureBlockIndexes.length;
      // setting all the measure chunk indexes to be read from file
      blockExecutionInfo.setAllSelectedMeasureBlocksIndexes(CarbonUtil
          .getRangeIndex(measureBlockIndexes, numberOfElementToConsider,
              numberOfColumnToBeReadInOneIO));
    } else {
      blockExecutionInfo.setAllSelectedMeasureBlocksIndexes(new int[0][0]);
    }
    // setting the indexes of list of dimension in projection list
    blockExecutionInfo.setProjectionListDimensionIndexes(ArrayUtils.toPrimitive(
        allProjectionListDimensionIdexes
            .toArray(new Integer[allProjectionListDimensionIdexes.size()])));
    // setting the indexes of list of measures in projection list
    blockExecutionInfo.setProjectionListMeasureIndexes(ArrayUtils.toPrimitive(
        allProjectionListMeasureIndexes
            .toArray(new Integer[allProjectionListMeasureIndexes.size()])));
    // setting the size of fixed key column (dictionary column)
    blockExecutionInfo
        .setFixedLengthKeySize(getKeySize(currentBlockQueryDimensions, segmentProperties));
    Set<Integer> dictionaryColumnBlockIndex = new HashSet<Integer>();
    List<Integer> noDictionaryColumnBlockIndex = new ArrayList<Integer>();
    // get the block index to be read from file for query dimension
    // for both dictionary columns and no dictionary columns
    QueryUtil.fillQueryDimensionsBlockIndexes(currentBlockQueryDimensions,
        segmentProperties.getDimensionOrdinalToBlockMapping(), dictionaryColumnBlockIndex,
        noDictionaryColumnBlockIndex);
    int[] queryDictionaryColumnBlockIndexes = ArrayUtils.toPrimitive(
        dictionaryColumnBlockIndex.toArray(new Integer[dictionaryColumnBlockIndex.size()]));
    // need to sort the dictionary column as for all dimension
    // column key will be filled based on key order
    Arrays.sort(queryDictionaryColumnBlockIndexes);
    blockExecutionInfo.setDictionaryColumnBlockIndex(queryDictionaryColumnBlockIndexes);
    // setting the no dictionary column block indexes
    blockExecutionInfo.setNoDictionaryBlockIndexes(ArrayUtils.toPrimitive(
        noDictionaryColumnBlockIndex.toArray(new Integer[noDictionaryColumnBlockIndex.size()])));
    // setting column id to dictionary mapping
    blockExecutionInfo.setColumnIdToDcitionaryMapping(queryProperties.columnToDictionayMapping);
    // setting each column value size
    blockExecutionInfo.setEachColumnValueSize(segmentProperties.getEachDimColumnValueSize());
    blockExecutionInfo.setComplexColumnParentBlockIndexes(
        getComplexDimensionParentBlockIndexes(currentBlockQueryDimensions));
    blockExecutionInfo.setVectorBatchCollector(queryModel.isVectorReader());
    try {
      // to set column group and its key structure info which will be used
      // to
      // for getting the column group column data in case of final row
      // and in case of dimension aggregation
      blockExecutionInfo.setColumnGroupToKeyStructureInfo(
          QueryUtil.getColumnGroupKeyStructureInfo(currentBlockQueryDimensions, segmentProperties));
    } catch (KeyGenException e) {
      throw new QueryExecutionException(e);
    }
    // set actual query dimensions and measures. It may differ in case of restructure scenarios
    blockExecutionInfo.setActualQueryDimensions(queryModel.getQueryDimension()
        .toArray(new QueryDimension[queryModel.getQueryDimension().size()]));
    blockExecutionInfo.setActualQueryMeasures(queryModel.getQueryMeasures()
        .toArray(new QueryMeasure[queryModel.getQueryMeasures().size()]));
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
  private int getKeySize(List<QueryDimension> queryDimension,
      SegmentProperties blockMetadataInfo) {
    // add the dimension block ordinal for each dictionary column
    // existing in the current block dimensions. Set is used because in case of column groups
    // ordinal of columns in a column group will be same
    Set<Integer> fixedLengthDimensionOrdinal =
        new HashSet<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    int counter = 0;
    while (counter < queryDimension.size()) {
      if (queryDimension.get(counter).getDimension().getNumberOfChild() > 0) {
        counter += queryDimension.get(counter).getDimension().getNumberOfChild();
        continue;
      } else if (!CarbonUtil.hasEncoding(queryDimension.get(counter).getDimension().getEncoder(),
          Encoding.DICTIONARY)) {
        counter++;
      } else {
        fixedLengthDimensionOrdinal.add(blockMetadataInfo.getDimensionOrdinalToBlockMapping()
            .get(queryDimension.get(counter).getDimension().getOrdinal()));
        counter++;
      }
    }
    int[] dictionaryColumnOrdinal = ArrayUtils.toPrimitive(
        fixedLengthDimensionOrdinal.toArray(new Integer[fixedLengthDimensionOrdinal.size()]));
    // calculate the size of existing query dictionary columns in this block
    if (dictionaryColumnOrdinal.length > 0) {
      int[] eachColumnValueSize = blockMetadataInfo.getEachDimColumnValueSize();
      int keySize = 0;
      for (int i = 0; i < dictionaryColumnOrdinal.length; i++) {
        keySize += eachColumnValueSize[dictionaryColumnOrdinal[i]];
      }
      return keySize;
    }
    return 0;
  }

  /**
   * Below method will be used to get the measures present in the current block
   *
   * @param blockExecutionInfo
   * @param queryModel         query model
   * @param tableBlock         table block
   * @return
   */
  private List<QueryMeasure> getCurrentBlockQueryMeasures(BlockExecutionInfo blockExecutionInfo,
      QueryModel queryModel, AbstractIndex tableBlock) throws QueryExecutionException {
    // getting the measure info which will be used while filling up measure data
    List<QueryMeasure> updatedQueryMeasures = RestructureUtil
        .createMeasureInfoAndGetCurrentBlockQueryMeasures(blockExecutionInfo,
            queryModel.getQueryMeasures(), tableBlock.getSegmentProperties().getMeasures());
    // setting the measure aggregator for all aggregation function selected
    // in query
    blockExecutionInfo.getMeasureInfo().setMeasureDataTypes(queryProperties.measureDataTypes);
    return updatedQueryMeasures;
  }

  private int[] getComplexDimensionParentBlockIndexes(List<QueryDimension> queryDimensions) {
    List<Integer> parentBlockIndexList = new ArrayList<Integer>();
    for (QueryDimension queryDimension : queryDimensions) {
      if (CarbonUtil.hasDataType(queryDimension.getDimension().getDataType(),
          new DataType[] { DataType.ARRAY, DataType.STRUCT, DataType.MAP })) {
        parentBlockIndexList.add(queryDimension.getDimension().getOrdinal());
      }
    }
    return ArrayUtils
        .toPrimitive(parentBlockIndexList.toArray(new Integer[parentBlockIndexList.size()]));
  }

  /**
   * This method will create the updated list of filter measures present in the current block
   *
   * @param queryFilterMeasures
   * @param segmentProperties
   * @return
   */
  private Set<CarbonMeasure> getCurrentBlockFilterMeasures(Set<CarbonMeasure> queryFilterMeasures,
      SegmentProperties segmentProperties) {
    if (!queryFilterMeasures.isEmpty()) {
      Set<CarbonMeasure> updatedFilterMeasures = new HashSet<>(queryFilterMeasures.size());
      for (CarbonMeasure queryMeasure : queryFilterMeasures) {
        CarbonMeasure measureFromCurrentBlock =
            segmentProperties.getMeasureFromCurrentBlock(queryMeasure.getColumnId());
        if (null != measureFromCurrentBlock) {
          updatedFilterMeasures.add(measureFromCurrentBlock);
        }
      }
      return updatedFilterMeasures;
    } else {
      return queryFilterMeasures;
    }
  }

  /**
   * This method will create the updated list of filter dimensions present in the current block
   *
   * @param queryFilterDimensions
   * @param segmentProperties
   * @return
   */
  private Set<CarbonDimension> getCurrentBlockFilterDimensions(
      Set<CarbonDimension> queryFilterDimensions, SegmentProperties segmentProperties) {
    if (!queryFilterDimensions.isEmpty()) {
      Set<CarbonDimension> updatedFilterDimensions = new HashSet<>(queryFilterDimensions.size());
      for (CarbonDimension queryDimension : queryFilterDimensions) {
        CarbonDimension dimensionFromCurrentBlock =
            segmentProperties.getDimensionFromCurrentBlock(queryDimension);
        if (null != dimensionFromCurrentBlock) {
          updatedFilterDimensions.add(dimensionFromCurrentBlock);
        }
      }
      return updatedFilterDimensions;
    } else {
      return queryFilterDimensions;
    }
  }

  /**
   * Below method will be used to finish the execution
   *
   * @throws QueryExecutionException
   */
  @Override public void finish() throws QueryExecutionException {
    CarbonUtil.clearBlockCache(queryProperties.dataBlocks);
    if (null != queryIterator) {
      queryIterator.close();
    }
    if (null != queryProperties.executorService) {
      queryProperties.executorService.shutdown();
      try {
        queryProperties.executorService.awaitTermination(1, TimeUnit.HOURS);
      } catch (InterruptedException e) {
        throw new QueryExecutionException(e);
      }
    }
  }

}
