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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.constants.CarbonV3DataFormatConstants;
import org.apache.carbondata.core.datastore.ReusableDataBuffer;
import org.apache.carbondata.core.datastore.block.AbstractIndex;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.index.IndexFilter;
import org.apache.carbondata.core.index.Segment;
import org.apache.carbondata.core.indexstore.BlockletDetailInfo;
import org.apache.carbondata.core.indexstore.blockletindex.BlockletDataRefNode;
import org.apache.carbondata.core.indexstore.blockletindex.IndexWrapper;
import org.apache.carbondata.core.memory.UnsafeMemoryManager;
import org.apache.carbondata.core.metadata.blocklet.BlockletInfo;
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.scan.executor.QueryExecutor;
import org.apache.carbondata.core.scan.executor.exception.QueryExecutionException;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.executor.util.QueryUtil;
import org.apache.carbondata.core.scan.executor.util.RestructureUtil;
import org.apache.carbondata.core.scan.filter.FilterUtil;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.scan.model.ProjectionDimension;
import org.apache.carbondata.core.scan.model.ProjectionMeasure;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.stats.QueryStatistic;
import org.apache.carbondata.core.stats.QueryStatisticsConstants;
import org.apache.carbondata.core.util.BlockletIndexUtil;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonTimeStatisticsFactory;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.core.util.ThreadLocalSessionInfo;
import org.apache.carbondata.core.util.ThreadLocalTaskInfo;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

/**
 * This class provides a skeletal implementation of the {@link QueryExecutor}
 * interface to minimize the effort required to implement this interface. This
 * will be used to prepare all the properties required for query execution
 */
public abstract class AbstractQueryExecutor<E> implements QueryExecutor<E> {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(AbstractQueryExecutor.class.getName());
  /**
   * holder for query properties which will be used to execute the query
   */
  protected QueryExecutorProperties queryProperties;

  // whether to clear/free unsafe memory or not
  private boolean freeUnsafeMemory;

  /**
   * query result iterator which will execute the query
   * and give the result
   */
  protected CarbonIterator queryIterator;

  // denotes the size of the ReusableDataBuffer based on the number of projection columns
  protected int dataBufferSize = 0;

  public AbstractQueryExecutor(Configuration configuration) {
    ThreadLocalSessionInfo.setConfigurationToCurrentThread(configuration);
    queryProperties = new QueryExecutorProperties();
  }

  public void setExecutorService(ExecutorService executorService) {
    // add executor service for query execution
    queryProperties.executorService = executorService;
  }

  /**
   * Below method will be used to fill the executor properties based on query
   * model it will parse the query model and get the detail and fill it in
   * query properties
   *
   * @param queryModel
   */
  protected void initQuery(QueryModel queryModel) throws IOException {
    LOGGER.info("Query will be executed on table: " + queryModel.getAbsoluteTableIdentifier()
        .getCarbonTableIdentifier().getTableName());
    this.freeUnsafeMemory = queryModel.isFreeUnsafeMemory();
    // Initializing statistics list to record the query statistics
    // creating copy on write to handle concurrent scenario
    queryProperties.queryStatisticsRecorder = queryModel.getStatisticsRecorder();
    if (null == queryProperties.queryStatisticsRecorder) {
      queryProperties.queryStatisticsRecorder =
          CarbonTimeStatisticsFactory.createExecutorRecorder(queryModel.getQueryId());
      queryModel.setStatisticsRecorder(queryProperties.queryStatisticsRecorder);
    }
    QueryStatistic queryStatistic = new QueryStatistic();
    // sort the block info
    // so block will be loaded in sorted order this will be required for
    // query execution
    Collections.sort(queryModel.getTableBlockInfos());
    queryProperties.dataBlocks = getDataBlocks(queryModel);
    queryStatistic
        .addStatistics(QueryStatisticsConstants.LOAD_BLOCKS_EXECUTOR, System.currentTimeMillis());
    queryProperties.queryStatisticsRecorder.recordStatistics(queryStatistic);

    // as aggregation will be executed in following order
    // 1.aggregate dimension expression
    // 2. expression
    // 3. query measure
    // so calculating the index of the expression start index
    // and measure column start index
    queryProperties.filterMeasures = new HashSet<>();
    queryProperties.complexFilterDimension = new HashSet<>();
    if (queryModel.getIndexFilter() != null) {
      QueryUtil.getAllFilterDimensionsAndMeasures(queryModel.getIndexFilter().getResolver(),
          queryProperties.complexFilterDimension, queryProperties.filterMeasures);
    }

    queryStatistic = new QueryStatistic();
    queryStatistic
        .addStatistics(QueryStatisticsConstants.LOAD_DICTIONARY, System.currentTimeMillis());
    queryProperties.queryStatisticsRecorder.recordStatistics(queryStatistic);

    for (int i = 0; i < queryModel.getProjectionColumns().length; i++) {
      dataBufferSize++;
      if (queryModel.getProjectionColumns()[i] instanceof CarbonDimension) {
        findDataBufferSize((CarbonDimension) queryModel.getProjectionColumns()[i]);
      }

    }
  }

  // Recursively determine the size needed for ReusableDataBuffer[]
  public void findDataBufferSize(CarbonDimension carbonDimension) {
    if (carbonDimension == null || carbonDimension.getListOfChildDimensions() == null) {
      return;
    }
    List<CarbonDimension> childDimension = carbonDimension.getListOfChildDimensions();
    dataBufferSize += childDimension.size();
    for (int i = 0; i < childDimension.size(); i++) {
      findDataBufferSize(childDimension.get(i));
    }
  }

  /**
   * Method returns the block(s) on which query will get executed
   *
   * @param queryModel
   * @return
   * @throws IOException
   */
  private List<AbstractIndex> getDataBlocks(QueryModel queryModel) throws IOException {
    Map<String, List<TableBlockInfo>> listMap = new LinkedHashMap<>();
    // this is introduced to handle the case when CACHE_LEVEL=BLOCK and there are few other indexes
    // like lucene, Bloom created on the table. In that case all the indexes will do blocklet
    // level pruning and blockInfo entries will be repeated with different blockletIds
    Map<String, DataFileFooter> filePathToFileFooterMapping = new HashMap<>();
    Map<String, SegmentProperties> filePathToSegmentPropertiesMap = new HashMap<>();
    for (TableBlockInfo blockInfo : queryModel.getTableBlockInfos()) {
      List<TableBlockInfo> tableBlockInfos = listMap.get(blockInfo.getFilePath());
      if (tableBlockInfos == null) {
        tableBlockInfos = new ArrayList<>();
        listMap.put(blockInfo.getFilePath(), tableBlockInfos);
      }
      SegmentProperties segmentProperties =
          filePathToSegmentPropertiesMap.get(blockInfo.getFilePath());
      BlockletDetailInfo blockletDetailInfo = blockInfo.getDetailInfo();
      // This case can come in 2 scenarios:
      // 1. old stores (1.1 or any prior version to 1.1) where blocklet information is not
      // available so read the blocklet information from block file
      // 2. CACHE_LEVEL is set to block
      // 3. CACHE_LEVEL is BLOCKLET but filter column min/max is not cached in driver
      if (null == blockletDetailInfo || blockletDetailInfo.getBlockletInfo() == null
          || blockletDetailInfo.isUseMinMaxForPruning()) {
        if (null != blockletDetailInfo) {
          blockInfo.setBlockOffset(blockletDetailInfo.getBlockFooterOffset());
        }
        DataFileFooter fileFooter = filePathToFileFooterMapping.get(blockInfo.getFilePath());
        if (null != blockInfo.getDataFileFooter()) {
          fileFooter = blockInfo.getDataFileFooter();
        }
        if (null == fileFooter) {
          blockInfo.setDetailInfo(null);
          fileFooter = CarbonUtil.readMetadataFile(blockInfo);
          // In case of non transactional table just set columnUniqueId as columnName to support
          // backward compatibility. non transactional tables column uniqueId is always equal to
          // columnName
          if (!queryModel.getTable().isTransactionalTable()) {
            QueryUtil.updateColumnUniqueIdForNonTransactionTable(fileFooter.getColumnInTable());
          }
          filePathToFileFooterMapping.put(blockInfo.getFilePath(), fileFooter);
          if (null == blockletDetailInfo) {
            blockletDetailInfo = QueryUtil.getBlockletDetailInfo(fileFooter, blockInfo);
          }
          blockInfo.setDetailInfo(blockletDetailInfo);
        }
        if (null == segmentProperties) {
          segmentProperties = new SegmentProperties(fileFooter.getColumnInTable());
          createFilterExpression(queryModel, segmentProperties);
          updateColumns(queryModel, fileFooter.getColumnInTable(), blockInfo.getFilePath());
          filePathToSegmentPropertiesMap.put(blockInfo.getFilePath(), segmentProperties);
        }
        readAndFillBlockletInfo(tableBlockInfos, blockInfo, blockletDetailInfo, fileFooter);
      } else {
        if (null == segmentProperties) {
          segmentProperties = new SegmentProperties(blockInfo.getDetailInfo().getColumnSchemas());
          createFilterExpression(queryModel, segmentProperties);
          updateColumns(queryModel, blockInfo.getDetailInfo().getColumnSchemas(),
              blockInfo.getFilePath());
          filePathToSegmentPropertiesMap.put(blockInfo.getFilePath(), segmentProperties);
        }
        tableBlockInfos.add(blockInfo);
      }
    }
    List<AbstractIndex> indexList = new ArrayList<>();
    for (List<TableBlockInfo> tableBlockInfos : listMap.values()) {
      indexList.add(new IndexWrapper(tableBlockInfos,
          filePathToSegmentPropertiesMap.get(tableBlockInfos.get(0).getFilePath())));
    }
    return indexList;
  }

  /**
   * It updates dimensions and measures of query model. In few scenarios like SDK user can configure
   * sort options per load, so if first load has c1 as integer column and configure as sort column
   * then carbon treat that as dimension.But in second load if user change the sort option then the
   * c1 become measure as by default integers are measures. So this method updates the measures to
   * dimensions and vice versa as per the index file schema.
   */
  private void updateColumns(QueryModel queryModel, List<ColumnSchema> columnsInTable,
      String filePath) throws IOException {
    if (queryModel.getTable().isTransactionalTable()) {
      return;
    }
    // First validate the schema of the carbondata file if the same column name have different
    // datatype
    boolean sameColumnSchemaList = BlockletIndexUtil
        .isSameColumnAndDifferentDatatypeInSchema(columnsInTable,
            queryModel.getTable().getTableInfo().getFactTable().getListOfColumns());
    if (!sameColumnSchemaList) {
      LOGGER.error("Datatype of the common columns present in " + filePath + " doesn't match with"
          + "the column's datatype in table schema");
      throw new IOException("All common columns present in the files doesn't have same datatype. "
          + "Unsupported operation on nonTransactional table. Check logs.");
    }
    List<ProjectionDimension> dimensions = queryModel.getProjectionDimensions();
    List<ProjectionMeasure> measures = queryModel.getProjectionMeasures();
    List<ProjectionDimension> updatedDims = new ArrayList<>();
    List<ProjectionMeasure> updatedMsrs = new ArrayList<>();

    // Check and update dimensions to measures if it is measure in index file schema
    for (ProjectionDimension dimension : dimensions) {
      int index = columnsInTable.indexOf(dimension.getDimension().getColumnSchema());
      if (index > -1) {
        if (!columnsInTable.get(index).isDimensionColumn()) {
          ProjectionMeasure measure = new ProjectionMeasure(
              new CarbonMeasure(columnsInTable.get(index), dimension.getDimension().getOrdinal(),
                  dimension.getDimension().getSchemaOrdinal()));
          measure.setOrdinal(dimension.getOrdinal());
          updatedMsrs.add(measure);
        } else {
          updatedDims.add(dimension);
        }
      } else {
        updatedDims.add(dimension);
      }
    }

    // Check and update measure to dimension if it is dimension in index file schema.
    for (ProjectionMeasure measure : measures) {
      int index = columnsInTable.indexOf(measure.getMeasure().getColumnSchema());
      if (index > -1) {
        if (columnsInTable.get(index).isDimensionColumn()) {
          ProjectionDimension dimension = new ProjectionDimension(
              new CarbonDimension(columnsInTable.get(index), measure.getMeasure().getOrdinal(), -1,
                  measure.getMeasure().getSchemaOrdinal()));
          dimension.setOrdinal(measure.getOrdinal());
          updatedDims.add(dimension);
        } else {
          updatedMsrs.add(measure);
        }
      } else {
        updatedMsrs.add(measure);
      }
    }
    // Clear and update the query model projections.
    dimensions.clear();
    dimensions.addAll(updatedDims);
    measures.clear();
    measures.addAll(updatedMsrs);
  }

  private void createFilterExpression(QueryModel queryModel, SegmentProperties properties) {
    if (queryModel.getIndexFilter() != null) {
      if (!queryModel.getIndexFilter().isResolvedOnSegment(properties)) {
        IndexFilter expression = new IndexFilter(properties, queryModel.getTable(),
            queryModel.getIndexFilter().getExpression());
        queryModel.setIndexFilter(expression);
      }
    }
  }

  /**
   * Read the file footer of block file and get the blocklets to query
   */
  private void readAndFillBlockletInfo(List<TableBlockInfo> tableBlockInfos,
      TableBlockInfo blockInfo, BlockletDetailInfo blockletDetailInfo, DataFileFooter fileFooter) {
    List<BlockletInfo> blockletList = fileFooter.getBlockletList();
    // cases when blockletID will be -1
    // 1. In case of legacy store
    // 2. In case CACHE_LEVEL is block and no other index apart from blocklet index is
    // created for a table
    // In all above cases entries will be according to the number of blocks and not according to
    // number of blocklets
    if (blockletDetailInfo.getBlockletId() != -1) {
      // fill the info only for given blockletId in detailInfo
      BlockletInfo blockletInfo = blockletList.get(blockletDetailInfo.getBlockletId());
      fillBlockletInfoToTableBlock(tableBlockInfos, blockInfo, fileFooter,
          blockletInfo, blockletDetailInfo.getBlockletId());
    } else {
      short count = 0;
      for (BlockletInfo blockletInfo : blockletList) {
        fillBlockletInfoToTableBlock(tableBlockInfos, blockInfo, fileFooter,
            blockletInfo, count);
        count++;
      }
    }
  }

  private void fillBlockletInfoToTableBlock(List<TableBlockInfo> tableBlockInfos,
      TableBlockInfo blockInfo, DataFileFooter fileFooter, BlockletInfo blockletInfo,
      short blockletId) {
    TableBlockInfo info = blockInfo.copy();
    BlockletDetailInfo detailInfo = info.getDetailInfo();
    // set column schema details
    detailInfo.setColumnSchemas(fileFooter.getColumnInTable());
    detailInfo.setRowCount(blockletInfo.getNumberOfRows());
    byte[][] maxValues = blockletInfo.getBlockletIndex().getMinMaxIndex().getMaxValues();
    byte[][] minValues = blockletInfo.getBlockletIndex().getMinMaxIndex().getMinValues();
    blockletInfo.getBlockletIndex().getMinMaxIndex().setMaxValues(maxValues);
    blockletInfo.getBlockletIndex().getMinMaxIndex().setMinValues(minValues);
    detailInfo.setBlockletInfo(blockletInfo);
    detailInfo.setBlockletId(blockletId);
    detailInfo.setPagesCount((short) blockletInfo.getNumberOfPages());
    tableBlockInfos.add(info);
  }

  protected List<BlockExecutionInfo> getBlockExecutionInfos(QueryModel queryModel)
      throws IOException {
    initQuery(queryModel);
    List<BlockExecutionInfo> blockExecutionInfoList = new ArrayList<BlockExecutionInfo>();
    // fill all the block execution infos for all the blocks selected in
    // query
    // and query will be executed based on that infos
    ReusableDataBuffer[] dimensionReusableDataBuffers = null;
    ReusableDataBuffer[] measureReusableDataBuffers = null;

    for (int i = 0; i < queryProperties.dataBlocks.size(); i++) {
      AbstractIndex abstractIndex = queryProperties.dataBlocks.get(i);
      BlockletDataRefNode dataRefNode =
          (BlockletDataRefNode) abstractIndex.getDataRefNode();
      final BlockExecutionInfo blockExecutionInfoForBlock =
          getBlockExecutionInfoForBlock(
              queryModel,
              abstractIndex,
              dataRefNode.numberOfNodes(),
              dataRefNode.getBlockInfos().get(0).getFilePath(),
              dataRefNode.getBlockInfos().get(0).getDeletedDeltaFilePath(),
              dataRefNode.getBlockInfos().get(0).getSegment());
      if (null == dimensionReusableDataBuffers || null == measureReusableDataBuffers) {
        dimensionReusableDataBuffers = blockExecutionInfoForBlock.getDimensionReusableDataBuffer();
        measureReusableDataBuffers = blockExecutionInfoForBlock.getMeasureReusableDataBuffer();
      } else {
        if (dimensionReusableDataBuffers.length == blockExecutionInfoForBlock
            .getDimensionReusableDataBuffer().length) {
          blockExecutionInfoForBlock.setDimensionReusableDataBuffer(dimensionReusableDataBuffers);
        }
        if (measureReusableDataBuffers.length == blockExecutionInfoForBlock
            .getMeasureReusableDataBuffer().length) {
          blockExecutionInfoForBlock.setMeasureReusableDataBuffer(measureReusableDataBuffers);
        }
      }
      blockExecutionInfoList.add(blockExecutionInfoForBlock);
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
   */
  private BlockExecutionInfo getBlockExecutionInfoForBlock(QueryModel queryModel,
      AbstractIndex blockIndex, int numberOfBlockletToScan, String filePath,
      String[] deleteDeltaFiles, Segment segment) {
    BlockExecutionInfo blockExecutionInfo = new BlockExecutionInfo();
    SegmentProperties segmentProperties = blockIndex.getSegmentProperties();
    // set actual query dimensions and measures. It may differ in case of restructure scenarios
    RestructureUtil.actualProjectionOfSegment(blockExecutionInfo, queryModel, segmentProperties);
    // below is to get only those dimension in query which is present in the
    // table block
    List<ProjectionDimension> projectDimensions = RestructureUtil
        .createDimensionInfoAndGetCurrentBlockQueryDimension(blockExecutionInfo,
            blockExecutionInfo.getActualQueryDimensions(), segmentProperties.getDimensions(),
            segmentProperties.getComplexDimensions(),
            blockExecutionInfo.getActualQueryMeasures().length,
            queryModel.getTable().getTableInfo().isTransactionalTable());
    boolean isStandardTable = CarbonUtil.isStandardCarbonTable(queryModel.getTable());
    String blockId = CarbonUtil
        .getBlockId(queryModel.getAbsoluteTableIdentifier(), filePath, segment.getSegmentNo(),
            queryModel.getTable().getTableInfo().isTransactionalTable(),
            isStandardTable, queryModel.getTable().isHivePartitionTable());
    if (!isStandardTable) {
      blockExecutionInfo.setBlockId(CarbonTablePath.getShortBlockIdForPartitionTable(blockId));
    } else {
      blockExecutionInfo.setBlockId(CarbonTablePath.getShortBlockId(blockId));
    }
    blockExecutionInfo.setDeleteDeltaFilePath(deleteDeltaFiles);
    blockExecutionInfo.setStartBlockletIndex(0);
    blockExecutionInfo.setNumberOfBlockletToScan(numberOfBlockletToScan);
    blockExecutionInfo.setProjectionDimensions(projectDimensions
        .toArray(new ProjectionDimension[0]));
    // get measures present in the current block
    List<ProjectionMeasure> projectionMeasures = RestructureUtil
        .createMeasureInfoAndGetCurrentBlockQueryMeasures(blockExecutionInfo,
            blockExecutionInfo.getActualQueryMeasures(), segmentProperties.getMeasures(),
            queryModel.getTable().getTableInfo().isTransactionalTable());
    blockExecutionInfo.setProjectionMeasures(
        projectionMeasures.toArray(new ProjectionMeasure[projectionMeasures.size()]));
    blockExecutionInfo.setDataBlock(blockIndex);
    // setting whether raw record query or not
    blockExecutionInfo.setRawRecordDetailQuery(queryModel.isForcedDetailRawQuery());
    // total number dimension
    blockExecutionInfo
        .setTotalNumberDimensionToRead(
            segmentProperties.getDimensionOrdinalToChunkMapping().size());
    blockExecutionInfo.setReadOnlyDelta(queryModel.isReadOnlyDelta());
    if (queryModel.isReadPageByPage()) {
      blockExecutionInfo.setPrefetchBlocklet(false);
      LOGGER.info("Query prefetch is: false, read page by page");
    } else {
      LOGGER.info("Query prefetch is: " + queryModel.isPreFetchData());
      blockExecutionInfo.setPrefetchBlocklet(queryModel.isPreFetchData());
    }
    // In case of fg index it should not go to direct fill.
    boolean fgIndexPathPresent = false;
    for (TableBlockInfo blockInfo : queryModel.getTableBlockInfos()) {
      fgIndexPathPresent = blockInfo.getIndexWriterPath() != null;
      if (fgIndexPathPresent) {
        queryModel.setDirectVectorFill(false);
        break;
      }
    }

    blockExecutionInfo.setDirectVectorFill(queryModel.isDirectVectorFill());
    blockExecutionInfo.setTotalNumberOfMeasureToRead(
        segmentProperties.getMeasuresOrdinalToChunkMapping().size());
    blockExecutionInfo.setComplexDimensionInfoMap(
        QueryUtil.getComplexDimensionsMap(
            projectDimensions,
            segmentProperties.getDimensionOrdinalToChunkMapping(),
            queryProperties.complexFilterDimension));
    if (null != queryModel.getIndexFilter()) {
      FilterResolverIntf filterResolverIntf;
      if (!filePath.startsWith(queryModel.getTable().getTablePath())) {
        filterResolverIntf = queryModel.getIndexFilter().getExternalSegmentResolver();
      } else {
        // loading the filter executor tree for filter evaluation
        filterResolverIntf = queryModel.getIndexFilter().getResolver();
      }
      blockExecutionInfo.setFilterExecutorTree(
          FilterUtil.getFilterExecutorTree(filterResolverIntf, segmentProperties,
              blockExecutionInfo.getComplexDimensionInfoMap(), false));
    }
    // expression measure
    List<CarbonMeasure> expressionMeasures =
        new ArrayList<CarbonMeasure>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    // setting all the dimension chunk indexes to be read from file
    int numberOfElementToConsider = 0;
    // list of dimensions to be projected
    Set<Integer> allProjectionListDimensionIndexes = new LinkedHashSet<>();
    // create a list of filter dimensions present in the current block
    Set<CarbonDimension> currentBlockFilterDimensions =
        getCurrentBlockFilterDimensions(queryProperties.complexFilterDimension, segmentProperties);
    int[] dimensionChunkIndexes = QueryUtil.getDimensionChunkIndexes(projectDimensions,
        segmentProperties.getDimensionOrdinalToChunkMapping(),
        currentBlockFilterDimensions, allProjectionListDimensionIndexes);
    ReusableDataBuffer[] dimensionBuffer = new ReusableDataBuffer[dataBufferSize];
    for (int i = 0; i < dimensionBuffer.length; i++) {
      dimensionBuffer[i] = new ReusableDataBuffer();
    }
    blockExecutionInfo.setDimensionReusableDataBuffer(dimensionBuffer);
    int numberOfColumnToBeReadInOneIO = Integer.parseInt(CarbonProperties.getInstance()
        .getProperty(CarbonV3DataFormatConstants.NUMBER_OF_COLUMN_TO_READ_IN_IO,
            CarbonV3DataFormatConstants.NUMBER_OF_COLUMN_TO_READ_IN_IO_DEFAULT_VALUE));

    if (dimensionChunkIndexes.length > 0) {
      numberOfElementToConsider = dimensionChunkIndexes[dimensionChunkIndexes.length - 1]
          == segmentProperties.getBlockToDimensionOrdinalMapping().size() - 1 ?
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
        projectionMeasures, expressionMeasures,
        segmentProperties.getMeasuresOrdinalToChunkMapping(), filterMeasures,
        allProjectionListMeasureIndexes);
    ReusableDataBuffer[] measureBuffer =
        new ReusableDataBuffer[allProjectionListMeasureIndexes.size()];
    for (int i = 0; i < measureBuffer.length; i++) {
      measureBuffer[i] = new ReusableDataBuffer();
    }
    blockExecutionInfo.setMeasureReusableDataBuffer(measureBuffer);
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
        allProjectionListDimensionIndexes
            .toArray(new Integer[allProjectionListDimensionIndexes.size()])));
    // setting the indexes of list of measures in projection list
    blockExecutionInfo.setProjectionListMeasureIndexes(ArrayUtils.toPrimitive(
        allProjectionListMeasureIndexes
            .toArray(new Integer[allProjectionListMeasureIndexes.size()])));
    // setting the size of fixed key column (dictionary column)
    blockExecutionInfo
        .setFixedLengthKeySize(getKeySize(projectDimensions, segmentProperties));
    List<Integer> dictionaryColumnChunkIndex = new ArrayList<Integer>();
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
    if (!queryModel.isForcedDetailRawQuery()) {
      Arrays.sort(queryDictionaryColumnChunkIndexes);
    }
    blockExecutionInfo.setDictionaryColumnChunkIndex(queryDictionaryColumnChunkIndexes);
    // setting the no dictionary column block indexes
    blockExecutionInfo.setNoDictionaryColumnChunkIndexes(ArrayUtils.toPrimitive(
        noDictionaryColumnChunkIndex.toArray(new Integer[noDictionaryColumnChunkIndex.size()])));
    blockExecutionInfo.setComplexColumnParentBlockIndexes(
        getComplexDimensionParentBlockIndexes(projectDimensions));
    blockExecutionInfo.setVectorBatchCollector(queryModel.isVectorReader());
    DataTypeUtil.setDataTypeConverter(queryModel.getConverter());
    blockExecutionInfo.setRequiredRowId(queryModel.isRequiredRowId());
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
      } else if (queryDimension.get(counter).getDimension().getDataType() != DataTypes.DATE) {
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
    if (blockMetadataInfo.getNumberOfDictDimensions() > 0) {
      int[] eachColumnValueSize = blockMetadataInfo.createDimColumnValueLength();
      int keySize = 0;
      for (int i = 0; i < dictionaryColumnOrdinal.length; i++) {
        keySize += eachColumnValueSize[dictionaryColumnOrdinal[i]];
      }
      return keySize;
    }
    return 0;
  }

  private int[] getComplexDimensionParentBlockIndexes(List<ProjectionDimension> queryDimensions) {
    List<Integer> parentBlockIndexList = new ArrayList<Integer>();
    for (ProjectionDimension queryDimension : queryDimensions) {
      if (queryDimension.getDimension().getDataType().isComplexType()) {
        if (null != queryDimension.getDimension().getComplexParentDimension()) {
          if (queryDimension.getDimension().isComplex()) {
            parentBlockIndexList.add(queryDimension.getDimension().getOrdinal());
          } else {
            parentBlockIndexList.add(queryDimension.getParentDimension().getOrdinal());
          }
        } else {
          parentBlockIndexList.add(queryDimension.getDimension().getOrdinal());
        }
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
            segmentProperties.getMeasureFromCurrentBlock(queryMeasure);
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
  @Override
  public void finish() throws QueryExecutionException {
    CarbonUtil.clearBlockCache(queryProperties.dataBlocks);
    Throwable exceptionOccurred = null;
    if (null != queryIterator) {
      // catch if there is any exception so that it can be rethrown after clearing all the resources
      // else if any exception is thrown from this point executor service will not be terminated
      try {
        queryIterator.close();
      } catch (Throwable e) {
        exceptionOccurred = e;
      }
    }
    // clear all the unsafe memory used for the given task ID only if it is necessary to be cleared
    if (freeUnsafeMemory) {
      UnsafeMemoryManager.INSTANCE
          .freeMemoryAll(ThreadLocalTaskInfo.getCarbonTaskInfo().getTaskId());
      ThreadLocalTaskInfo.clearCarbonTaskInfo();
    }
    if (null != queryProperties.executorService) {
      // In case of limit query when number of limit records is already found so executors
      // must stop all the running execution otherwise it will keep running and will hit
      // the query performance.
      queryProperties.executorService.shutdownNow();
    }
    // if there is any exception re throw the exception
    if (null != exceptionOccurred) {
      throw new QueryExecutionException(exceptionOccurred);
    }
    DataTypeUtil.clearFormatter();
  }

}
