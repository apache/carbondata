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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;

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
import org.apache.carbondata.core.indexstore.BlockletDetailInfo;
import org.apache.carbondata.core.indexstore.blockletindex.BlockletDataRefNode;
import org.apache.carbondata.core.indexstore.blockletindex.IndexWrapper;
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.core.memory.UnsafeMemoryManager;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.blocklet.BlockletInfo;
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.scan.executor.QueryExecutor;
import org.apache.carbondata.core.scan.executor.exception.QueryExecutionException;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.executor.util.QueryUtil;
import org.apache.carbondata.core.scan.executor.util.RestructureUtil;
import org.apache.carbondata.core.scan.filter.FilterUtil;
import org.apache.carbondata.core.scan.filter.SingleTableProvider;
import org.apache.carbondata.core.scan.filter.TableProvider;
import org.apache.carbondata.core.scan.model.ProjectionDimension;
import org.apache.carbondata.core.scan.model.ProjectionMeasure;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.stats.QueryStatistic;
import org.apache.carbondata.core.stats.QueryStatisticsConstants;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonTimeStatisticsFactory;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.core.util.ThreadLocalTaskInfo;
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
    QueryStatistic queryStatistic = new QueryStatistic();
    // sort the block info
    // so block will be loaded in sorted order this will be required for
    // query execution
    Collections.sort(queryModel.getTableBlockInfos());

    if (queryModel.getTableBlockInfos().get(0).getDetailInfo() != null) {
      List<AbstractIndex> indexList = new ArrayList<>();
      Map<String, List<TableBlockInfo>> listMap = new LinkedHashMap<>();
      for (TableBlockInfo blockInfo: queryModel.getTableBlockInfos()) {
        List<TableBlockInfo> tableBlockInfos = listMap.get(blockInfo.getFilePath());
        if (tableBlockInfos == null) {
          tableBlockInfos = new ArrayList<>();
          listMap.put(blockInfo.getFilePath(), tableBlockInfos);
        }
        BlockletDetailInfo blockletDetailInfo = blockInfo.getDetailInfo();
        // This is the case of old stores where blocklet information is not available so read
        // the blocklet information from block file
        if (blockletDetailInfo.getBlockletInfo() == null) {
          readAndFillBlockletInfo(blockInfo, tableBlockInfos, blockletDetailInfo);
        } else {
          tableBlockInfos.add(blockInfo);
        }
      }
      for (List<TableBlockInfo> tableBlockInfos: listMap.values()) {
        indexList.add(new IndexWrapper(tableBlockInfos));
      }
      queryProperties.dataBlocks = indexList;
    } else {
      // get the table blocks
      CacheProvider cacheProvider = CacheProvider.getInstance();
      BlockIndexStore<TableBlockUniqueIdentifier, AbstractIndex> cache =
          (BlockIndexStore) cacheProvider.createCache(CacheType.EXECUTOR_BTREE);
      // remove the invalid table blocks, block which is deleted or compacted
      cache.removeTableBlocks(queryModel.getInvalidSegmentIds(),
          queryModel.getAbsoluteTableIdentifier());
      List<TableBlockUniqueIdentifier> tableBlockUniqueIdentifiers =
          prepareTableBlockUniqueIdentifier(queryModel.getTableBlockInfos(),
              queryModel.getAbsoluteTableIdentifier());
      cache.removeTableBlocksIfHorizontalCompactionDone(queryModel);
      queryProperties.dataBlocks = cache.getAll(tableBlockUniqueIdentifiers);
    }
    queryStatistic
        .addStatistics(QueryStatisticsConstants.LOAD_BLOCKS_EXECUTOR, System.currentTimeMillis());
    queryProperties.queryStatisticsRecorder.recordStatistics(queryStatistic);
    // calculating the total number of aggeragted columns
    int measureCount = queryModel.getProjectionMeasures().size();

    int currentIndex = 0;
    DataType[] dataTypes = new DataType[measureCount];

    for (ProjectionMeasure carbonMeasure : queryModel.getProjectionMeasures()) {
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

    CarbonTable carbonTable = queryModel.getTable();
    TableProvider tableProvider = new SingleTableProvider(carbonTable);

    queryStatistic = new QueryStatistic();
    // dictionary column unique column id to dictionary mapping
    // which will be used to get column actual data
    queryProperties.columnToDictionayMapping =
        QueryUtil.getDimensionDictionaryDetail(
            queryModel.getProjectionDimensions(),
            queryProperties.complexFilterDimension,
            queryModel.getAbsoluteTableIdentifier(),
            tableProvider);
    queryStatistic
        .addStatistics(QueryStatisticsConstants.LOAD_DICTIONARY, System.currentTimeMillis());
    queryProperties.queryStatisticsRecorder.recordStatistics(queryStatistic);
    queryModel.setColumnToDictionaryMapping(queryProperties.columnToDictionayMapping);
  }

  /**
   * Read the file footer of block file and get the blocklets to query
   */
  private void readAndFillBlockletInfo(TableBlockInfo blockInfo,
      List<TableBlockInfo> tableBlockInfos, BlockletDetailInfo blockletDetailInfo)
      throws IOException {
    blockInfo.setBlockOffset(blockletDetailInfo.getBlockFooterOffset());
    blockInfo.setDetailInfo(null);
    DataFileFooter fileFooter = CarbonUtil.readMetadatFile(blockInfo);
    blockInfo.setDetailInfo(blockletDetailInfo);
    List<BlockletInfo> blockletList = fileFooter.getBlockletList();
    short count = 0;
    for (BlockletInfo blockletInfo: blockletList) {
      TableBlockInfo info = blockInfo.copy();
      BlockletDetailInfo detailInfo = info.getDetailInfo();
      detailInfo.setRowCount(blockletInfo.getNumberOfRows());
      detailInfo.setBlockletInfo(blockletInfo);
      detailInfo.setPagesCount((short) blockletInfo.getNumberOfPages());
      detailInfo.setBlockletId(count);
      tableBlockInfos.add(info);
      count++;
    }
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
      AbstractIndex abstractIndex = queryProperties.dataBlocks.get(i);
      BlockletDataRefNode dataRefNode =
          (BlockletDataRefNode) abstractIndex.getDataRefNode();
      blockExecutionInfoList.add(getBlockExecutionInfoForBlock(queryModel, abstractIndex,
          dataRefNode.getBlockInfos().get(0).getBlockletInfos().getStartBlockletNumber(),
          dataRefNode.numberOfNodes(), dataRefNode.getBlockInfos().get(0).getFilePath(),
          dataRefNode.getBlockInfos().get(0).getDeletedDeltaFilePath()));
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
  private BlockExecutionInfo getBlockExecutionInfoForBlock(QueryModel queryModel,
      AbstractIndex blockIndex, int startBlockletIndex, int numberOfBlockletToScan, String filePath,
      String[] deleteDeltaFiles)
      throws QueryExecutionException {
    BlockExecutionInfo blockExecutionInfo = new BlockExecutionInfo();
    SegmentProperties segmentProperties = blockIndex.getSegmentProperties();
    List<CarbonDimension> tableBlockDimensions = segmentProperties.getDimensions();

    // below is to get only those dimension in query which is present in the
    // table block
    List<ProjectionDimension> projectDimensions = RestructureUtil
        .createDimensionInfoAndGetCurrentBlockQueryDimension(blockExecutionInfo,
            queryModel.getProjectionDimensions(), tableBlockDimensions,
            segmentProperties.getComplexDimensions());
    int tableFactPathLength = CarbonStorePath
        .getCarbonTablePath(queryModel.getAbsoluteTableIdentifier().getTablePath(),
            queryModel.getAbsoluteTableIdentifier().getCarbonTableIdentifier()).getFactDir()
        .length() + 1;
    blockExecutionInfo.setBlockId(filePath.substring(tableFactPathLength));
    blockExecutionInfo.setDeleteDeltaFilePath(deleteDeltaFiles);
    blockExecutionInfo.setStartBlockletIndex(startBlockletIndex);
    blockExecutionInfo.setNumberOfBlockletToScan(numberOfBlockletToScan);
    blockExecutionInfo.setProjectionDimensions(projectDimensions
        .toArray(new ProjectionDimension[projectDimensions.size()]));
    // get measures present in the current block
    List<ProjectionMeasure> currentBlockQueryMeasures =
        getCurrentBlockQueryMeasures(blockExecutionInfo, queryModel, blockIndex);
    blockExecutionInfo.setProjectionMeasures(
        currentBlockQueryMeasures.toArray(new ProjectionMeasure[currentBlockQueryMeasures.size()]));
    blockExecutionInfo.setDataBlock(blockIndex);
    // setting whether raw record query or not
    blockExecutionInfo.setRawRecordDetailQuery(queryModel.isForcedDetailRawQuery());
    // total number dimension
    blockExecutionInfo
        .setTotalNumberDimensionToRead(
            segmentProperties.getDimensionOrdinalToChunkMapping().size());
    blockExecutionInfo.setPrefetchBlocklet(!queryModel.isReadPageByPage());
    blockExecutionInfo
        .setTotalNumberOfMeasureToRead(segmentProperties.getMeasuresOrdinalToChunkMapping().size());
    blockExecutionInfo.setComplexDimensionInfoMap(QueryUtil
        .getComplexDimensionsMap(projectDimensions,
            segmentProperties.getDimensionOrdinalToChunkMapping(),
            segmentProperties.getEachComplexDimColumnValueSize(),
            queryProperties.columnToDictionayMapping, queryProperties.complexFilterDimension));
    IndexKey startIndexKey = null;
    IndexKey endIndexKey = null;
    if (null != queryModel.getFilterExpressionResolverTree()) {
      // loading the filter executor tree for filter evaluation
      blockExecutionInfo.setFilterExecuterTree(FilterUtil
          .getFilterExecuterTree(queryModel.getFilterExpressionResolverTree(), segmentProperties,
              blockExecutionInfo.getComlexDimensionInfoMap()));
    }
    try {
      startIndexKey = FilterUtil.prepareDefaultStartIndexKey(segmentProperties);
      endIndexKey = FilterUtil.prepareDefaultEndIndexKey(segmentProperties);
    } catch (KeyGenException e) {
      throw new QueryExecutionException(e);
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
    int[] dimensionChunkIndexes = QueryUtil.getDimensionChunkIndexes(
        projectDimensions, segmentProperties.getDimensionOrdinalToChunkMapping(),
        expressionDimensions, currentBlockFilterDimensions, allProjectionListDimensionIdexes);
    int numberOfColumnToBeReadInOneIO = Integer.parseInt(CarbonProperties.getInstance()
        .getProperty(CarbonV3DataFormatConstants.NUMBER_OF_COLUMN_TO_READ_IN_IO,
            CarbonV3DataFormatConstants.NUMBER_OF_COLUMN_TO_READ_IN_IO_DEFAULTVALUE));

    if (dimensionChunkIndexes.length > 0) {
      numberOfElementToConsider = dimensionChunkIndexes[dimensionChunkIndexes.length - 1]
          == segmentProperties.getBlockTodimensionOrdinalMapping().size() - 1 ?
          dimensionChunkIndexes.length - 1 :
          dimensionChunkIndexes.length;
      blockExecutionInfo.setAllSelectedDimensionColumnIndexRange(
          CarbonUtil.getRangeIndex(dimensionChunkIndexes, numberOfElementToConsider,
              numberOfColumnToBeReadInOneIO));
    } else {
      blockExecutionInfo.setAllSelectedDimensionColumnIndexRange(new int[0][0]);
    }
    // get the list of updated filter measures present in the current block
    Set<CarbonMeasure> filterMeasures =
        getCurrentBlockFilterMeasures(queryProperties.filterMeasures, segmentProperties);
    // list of measures to be projected
    List<Integer> allProjectionListMeasureIndexes = new ArrayList<>();
    int[] measureChunkIndexes = QueryUtil.getMeasureChunkIndexes(
        currentBlockQueryMeasures, expressionMeasures,
        segmentProperties.getMeasuresOrdinalToChunkMapping(), filterMeasures,
        allProjectionListMeasureIndexes);
    if (measureChunkIndexes.length > 0) {

      numberOfElementToConsider = measureChunkIndexes[measureChunkIndexes.length - 1]
          == segmentProperties.getMeasures().size() - 1 ?
          measureChunkIndexes.length - 1 :
          measureChunkIndexes.length;
      // setting all the measure chunk indexes to be read from file
      blockExecutionInfo.setAllSelectedMeasureIndexRange(
          CarbonUtil.getRangeIndex(
              measureChunkIndexes, numberOfElementToConsider,
              numberOfColumnToBeReadInOneIO));
    } else {
      blockExecutionInfo.setAllSelectedMeasureIndexRange(new int[0][0]);
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
        .setFixedLengthKeySize(getKeySize(projectDimensions, segmentProperties));
    Set<Integer> dictionaryColumnChunkIndex = new HashSet<Integer>();
    List<Integer> noDictionaryColumnChunkIndex = new ArrayList<Integer>();
    // get the block index to be read from file for query dimension
    // for both dictionary columns and no dictionary columns
    QueryUtil.fillQueryDimensionChunkIndexes(projectDimensions,
        segmentProperties.getDimensionOrdinalToChunkMapping(), dictionaryColumnChunkIndex,
        noDictionaryColumnChunkIndex);
    int[] queryDictionaryColumnChunkIndexes = ArrayUtils.toPrimitive(
        dictionaryColumnChunkIndex.toArray(new Integer[dictionaryColumnChunkIndex.size()]));
    // need to sort the dictionary column as for all dimension
    // column key will be filled based on key order
    Arrays.sort(queryDictionaryColumnChunkIndexes);
    blockExecutionInfo.setDictionaryColumnChunkIndex(queryDictionaryColumnChunkIndexes);
    // setting the no dictionary column block indexes
    blockExecutionInfo.setNoDictionaryColumnChunkIndexes(ArrayUtils.toPrimitive(
        noDictionaryColumnChunkIndex.toArray(new Integer[noDictionaryColumnChunkIndex.size()])));
    // setting each column value size
    blockExecutionInfo.setEachColumnValueSize(segmentProperties.getEachDimColumnValueSize());
    blockExecutionInfo.setComplexColumnParentBlockIndexes(
        getComplexDimensionParentBlockIndexes(projectDimensions));
    blockExecutionInfo.setVectorBatchCollector(queryModel.isVectorReader());
    try {
      // to set column group and its key structure info which will be used
      // to
      // for getting the column group column data in case of final row
      // and in case of dimension aggregation
      blockExecutionInfo.setColumnGroupToKeyStructureInfo(
          QueryUtil.getColumnGroupKeyStructureInfo(projectDimensions, segmentProperties));
    } catch (KeyGenException e) {
      throw new QueryExecutionException(e);
    }
    // set actual query dimensions and measures. It may differ in case of restructure scenarios
    blockExecutionInfo.setActualQueryDimensions(queryModel.getProjectionDimensions()
        .toArray(new ProjectionDimension[queryModel.getProjectionDimensions().size()]));
    blockExecutionInfo.setActualQueryMeasures(queryModel.getProjectionMeasures()
        .toArray(new ProjectionMeasure[queryModel.getProjectionMeasures().size()]));
    DataTypeUtil.setDataTypeConverter(queryModel.getConverter());
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
  private int getKeySize(List<ProjectionDimension> queryDimension,
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
      } else if (!CarbonUtil.hasEncoding(queryDimension.get(counter).getDimension().getEncoder(),
          Encoding.DICTIONARY)) {
        counter++;
      } else {
        fixedLengthDimensionOrdinal.add(blockMetadataInfo.getDimensionOrdinalToChunkMapping()
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
   * @param executionInfo
   * @param queryModel         query model
   * @param tableBlock         table block
   * @return
   */
  private List<ProjectionMeasure> getCurrentBlockQueryMeasures(BlockExecutionInfo executionInfo,
      QueryModel queryModel, AbstractIndex tableBlock) throws QueryExecutionException {
    // getting the measure info which will be used while filling up measure data
    List<ProjectionMeasure> updatedQueryMeasures = RestructureUtil
        .createMeasureInfoAndGetCurrentBlockQueryMeasures(executionInfo,
            queryModel.getProjectionMeasures(), tableBlock.getSegmentProperties().getMeasures());
    // setting the measure aggregator for all aggregation function selected
    // in query
    executionInfo.getMeasureInfo().setMeasureDataTypes(queryProperties.measureDataTypes);
    return updatedQueryMeasures;
  }

  private int[] getComplexDimensionParentBlockIndexes(List<ProjectionDimension> queryDimensions) {
    List<Integer> parentBlockIndexList = new ArrayList<Integer>();
    for (ProjectionDimension queryDimension : queryDimensions) {
      if (queryDimension.getDimension().getDataType().isComplexType()) {
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
    UnsafeMemoryManager.INSTANCE.freeMemoryAll(ThreadLocalTaskInfo.getCarbonTaskInfo().getTaskId());
    if (null != queryProperties.executorService) {
      // In case of limit query when number of limit records is already found so executors
      // must stop all the running execution otherwise it will keep running and will hit
      // the query performance.
      queryProperties.executorService.shutdownNow();
    }
  }

}
