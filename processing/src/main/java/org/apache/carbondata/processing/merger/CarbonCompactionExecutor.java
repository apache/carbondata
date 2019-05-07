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
package org.apache.carbondata.processing.merger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.datastore.block.TaskBlockInfo;
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.scan.executor.QueryExecutor;
import org.apache.carbondata.core.scan.executor.QueryExecutorFactory;
import org.apache.carbondata.core.scan.executor.exception.QueryExecutionException;
import org.apache.carbondata.core.scan.executor.util.RestructureUtil;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.model.QueryModelBuilder;
import org.apache.carbondata.core.scan.result.RowBatch;
import org.apache.carbondata.core.scan.result.iterator.ColumnDriftRawResultIterator;
import org.apache.carbondata.core.scan.result.iterator.RawResultIterator;
import org.apache.carbondata.core.scan.wrappers.IntArrayWrapper;
import org.apache.carbondata.core.stats.QueryStatistic;
import org.apache.carbondata.core.stats.QueryStatisticsConstants;
import org.apache.carbondata.core.stats.QueryStatisticsRecorder;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonTimeStatisticsFactory;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeConverter;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

/**
 * Executor class for executing the query on the selected segments to be merged.
 * This will fire a select * query and get the raw result.
 */
public class CarbonCompactionExecutor {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(CarbonCompactionExecutor.class.getName());
  private final Map<String, List<DataFileFooter>> dataFileMetadataSegMapping;
  private final SegmentProperties destinationSegProperties;
  private final Map<String, TaskBlockInfo> segmentMapping;
  private List<QueryExecutor> queryExecutorList;
  private List<QueryStatisticsRecorder> queryStatisticsRecorders =
      new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
  private CarbonTable carbonTable;
  private QueryModel queryModel;

  /**
   * flag to check whether any restructured block exists in the blocks selected for compaction.
   * Based on this decision will be taken whether complete data has to be sorted again
   */
  private boolean restructuredBlockExists;

  // converter for UTF8String and decimal conversion
  private DataTypeConverter dataTypeConverter;

  /**
   * Constructor
   *
   * @param segmentMapping
   * @param segmentProperties
   * @param carbonTable
   * @param dataFileMetadataSegMapping
   * @param restructuredBlockExists
   */
  public CarbonCompactionExecutor(Map<String, TaskBlockInfo> segmentMapping,
      SegmentProperties segmentProperties, CarbonTable carbonTable,
      Map<String, List<DataFileFooter>> dataFileMetadataSegMapping,
      boolean restructuredBlockExists, DataTypeConverter dataTypeConverter) {
    this.segmentMapping = segmentMapping;
    this.destinationSegProperties = segmentProperties;
    this.carbonTable = carbonTable;
    this.dataFileMetadataSegMapping = dataFileMetadataSegMapping;
    this.restructuredBlockExists = restructuredBlockExists;
    this.queryExecutorList = new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    this.dataTypeConverter = dataTypeConverter;
  }

  /**
   * For processing of the table blocks.
   *
   * @return Map of String with Carbon iterators
   * Map has 2 elements: UNSORTED and SORTED
   * Map(UNSORTED) = List of Iterators which yield sorted data
   * Map(Sorted) = List of Iterators which yield sorted data
   * In Range Column compaction we will have a Filter Expression to process
   */
  public Map<String, List<RawResultIterator>> processTableBlocks(Configuration configuration,
      Expression filterExpr) throws QueryExecutionException, IOException {

    Map<String, List<RawResultIterator>> resultList = new HashMap<>(2);
    resultList.put(CarbonCompactionUtil.UNSORTED_IDX,
        new ArrayList<RawResultIterator>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE));
    resultList.put(CarbonCompactionUtil.SORTED_IDX,
        new ArrayList<RawResultIterator>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE));

    List<TableBlockInfo> tableBlockInfos = null;
    QueryModelBuilder builder = null;
    if (null == filterExpr) {
      builder =
          new QueryModelBuilder(carbonTable).projectAllColumns().dataConverter(dataTypeConverter)
              .enableForcedDetailRawQuery();
    } else {
      builder = new QueryModelBuilder(carbonTable).projectAllColumns().filterExpression(filterExpr)
          .dataConverter(dataTypeConverter).enableForcedDetailRawQuery()
          .convertToRangeFilter(false);
    }
    if (enablePageLevelReaderForCompaction()) {
      builder.enableReadPageByPage();
    }
    queryModel = builder.build();
    // iterate each seg ID
    for (Map.Entry<String, TaskBlockInfo> taskMap : segmentMapping.entrySet()) {
      String segmentId = taskMap.getKey();
      List<DataFileFooter> listMetadata = dataFileMetadataSegMapping.get(segmentId);
      // for each segment get taskblock info
      TaskBlockInfo taskBlockInfo = taskMap.getValue();
      Set<String> taskBlockListMapping = taskBlockInfo.getTaskSet();
      // Check if block needs sorting or not
      boolean sortingRequired =
          !CarbonCompactionUtil.isSortedByCurrentSortColumns(carbonTable, listMetadata.get(0));
      for (String task : taskBlockListMapping) {
        tableBlockInfos = taskBlockInfo.getTableBlockInfoList(task);
        // during update there may be a chance that the cardinality may change within the segment
        // which may lead to failure while converting the row, so get all the blocks present in a
        // task and then split into multiple lists of same column values and create separate
        // RawResultIterator for each tableBlockInfo of same column values. If all the blocks have
        // same column values, then make a single RawResultIterator for all the blocks
        List<List<TableBlockInfo>> listOfTableBlocksBasedOnKeyLength =
            getListOfTableBlocksBasedOnColumnValueSize(tableBlockInfos);
        for (List<TableBlockInfo> tableBlockInfoList : listOfTableBlocksBasedOnKeyLength) {
          Collections.sort(tableBlockInfoList);
          LOGGER.info("for task -" + task + "- in segment id -" + segmentId + "- block size is -"
              + tableBlockInfos.size());
          queryModel.setTableBlockInfos(tableBlockInfoList);
          if (sortingRequired) {
            resultList.get(CarbonCompactionUtil.UNSORTED_IDX).add(
                getRawResultIterator(configuration, segmentId, task, tableBlockInfoList));
          } else {
            resultList.get(CarbonCompactionUtil.SORTED_IDX).add(
                getRawResultIterator(configuration, segmentId, task, tableBlockInfoList));
          }
        }
      }
    }
    return resultList;
  }

  private RawResultIterator getRawResultIterator(Configuration configuration, String segmentId,
      String task, List<TableBlockInfo> tableBlockInfoList)
      throws QueryExecutionException, IOException {
    SegmentProperties sourceSegmentProperties = getSourceSegmentProperties(
        Collections.singletonList(tableBlockInfoList.get(0).getDataFileFooter()));
    boolean hasColumnDrift = carbonTable.hasColumnDrift() &&
        RestructureUtil.hasColumnDriftOnSegment(carbonTable, sourceSegmentProperties);
    if (hasColumnDrift) {
      return new ColumnDriftRawResultIterator(
          executeBlockList(tableBlockInfoList, segmentId, task, configuration),
          sourceSegmentProperties, destinationSegProperties);
    } else {
      return new RawResultIterator(
          executeBlockList(tableBlockInfoList, segmentId, task, configuration),
          sourceSegmentProperties, destinationSegProperties, true);
    }
  }

  /**
   * This method returns the List of TableBlockInfoList, where each listOfTableBlockInfos will have
   * same columnvalues
   * @param tableBlockInfos List of tableBlockInfos present in each task
   */
  private List<List<TableBlockInfo>> getListOfTableBlocksBasedOnColumnValueSize(
      List<TableBlockInfo> tableBlockInfos) {
    List<List<TableBlockInfo>> listOfTableBlockInfoListOnColumnvaluesSize = new ArrayList<>();
    Map<IntArrayWrapper, List<TableBlockInfo>> columnvalueSizeToTableBlockInfoMap = new HashMap<>();
    for (TableBlockInfo tableBlock : tableBlockInfos) {
      // get the columnValueSize for the dataFileFooter
      IntArrayWrapper columnValueSize = new IntArrayWrapper(
          getSourceSegmentProperties(Collections.singletonList(tableBlock.getDataFileFooter()))
              .getColumnsValueSize());
      List<TableBlockInfo> tempBlockInfoList =
          columnvalueSizeToTableBlockInfoMap.get(columnValueSize);
      if (tempBlockInfoList == null) {
        tempBlockInfoList = new ArrayList<>();
        columnvalueSizeToTableBlockInfoMap.put(columnValueSize, tempBlockInfoList);
      }
      tempBlockInfoList.add(tableBlock);
    }
    for (Map.Entry<IntArrayWrapper, List<TableBlockInfo>> taskMap :
        columnvalueSizeToTableBlockInfoMap.entrySet()) {
      listOfTableBlockInfoListOnColumnvaluesSize.add(taskMap.getValue());
    }
    return listOfTableBlockInfoListOnColumnvaluesSize;
  }

  /**
   * This method will create the source segment properties based on restructured block existence
   *
   * @param listMetadata
   * @return
   */
  private SegmentProperties getSourceSegmentProperties(List<DataFileFooter> listMetadata) {
    SegmentProperties sourceSegProperties = null;
    if (restructuredBlockExists) {
      // update cardinality of source segment according to new schema
      Map<String, Integer> columnToCardinalityMap =
          new HashMap<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
      CarbonCompactionUtil
          .addColumnCardinalityToMap(columnToCardinalityMap, listMetadata.get(0).getColumnInTable(),
              listMetadata.get(0).getSegmentInfo().getColumnCardinality());
      List<ColumnSchema> updatedColumnSchemaList =
          new ArrayList<>(listMetadata.get(0).getColumnInTable().size());
      int[] updatedColumnCardinalities = CarbonCompactionUtil
          .updateColumnSchemaAndGetCardinality(columnToCardinalityMap, carbonTable,
              updatedColumnSchemaList);
      sourceSegProperties =
          new SegmentProperties(updatedColumnSchemaList, updatedColumnCardinalities);
    } else {
      sourceSegProperties = new SegmentProperties(listMetadata.get(0).getColumnInTable(),
          listMetadata.get(0).getSegmentInfo().getColumnCardinality());
    }
    return sourceSegProperties;
  }

  /**
   * get executor and execute the query model.
   *
   * @param blockList
   * @return
   */
  private CarbonIterator<RowBatch> executeBlockList(List<TableBlockInfo> blockList,
      String segmentId, String taskId, Configuration configuration)
      throws QueryExecutionException, IOException {
    queryModel.setTableBlockInfos(blockList);
    QueryStatisticsRecorder executorRecorder = CarbonTimeStatisticsFactory
        .createExecutorRecorder(queryModel.getQueryId() + "_" + segmentId + "_" + taskId);
    queryStatisticsRecorders.add(executorRecorder);
    queryModel.setStatisticsRecorder(executorRecorder);
    QueryExecutor queryExecutor = QueryExecutorFactory.getQueryExecutor(queryModel, configuration);
    queryExecutorList.add(queryExecutor);
    return queryExecutor.execute(queryModel);
  }

  /**
   * Below method will be used
   * for cleanup
   */
  public void close(List<RawResultIterator> rawResultIteratorList, long queryStartTime) {
    try {
      // close all the iterators. Iterators might not closed in case of compaction failure
      // or if process is killed
      if (null != rawResultIteratorList) {
        for (RawResultIterator rawResultIterator : rawResultIteratorList) {
          rawResultIterator.close();
        }
      }
      for (QueryExecutor queryExecutor : queryExecutorList) {
        queryExecutor.finish();
      }
      logStatistics(queryStartTime);
    } catch (QueryExecutionException e) {
      LOGGER.error("Problem while close. Ignoring the exception", e);
    }
    clearDictionaryFromQueryModel();
  }

  private void logStatistics(long queryStartTime) {
    if (!queryStatisticsRecorders.isEmpty()) {
      QueryStatistic queryStatistic = new QueryStatistic();
      queryStatistic.addFixedTimeStatistic(QueryStatisticsConstants.EXECUTOR_PART,
          System.currentTimeMillis() - queryStartTime);
      for (QueryStatisticsRecorder recorder : queryStatisticsRecorders) {
        recorder.recordStatistics(queryStatistic);
        // print executor query statistics for each task_id
        recorder.logStatistics();
      }
    }
  }

  /**
   * This method will clear the dictionary access count after its usage is complete so
   * that column can be deleted form LRU cache whenever memory reaches threshold
   */
  private void clearDictionaryFromQueryModel() {
    if (null != queryModel) {
      Map<String, Dictionary> columnToDictionaryMapping = queryModel.getColumnToDictionaryMapping();
      if (null != columnToDictionaryMapping) {
        for (Map.Entry<String, Dictionary> entry : columnToDictionaryMapping.entrySet()) {
          CarbonUtil.clearDictionaryCache(entry.getValue());
        }
      }
    }
  }

  /**
   * Whether to enable page level reader for compaction or not.
   */
  private boolean enablePageLevelReaderForCompaction() {
    String enablePageReaderProperty = CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.CARBON_ENABLE_PAGE_LEVEL_READER_IN_COMPACTION,
            CarbonCommonConstants.CARBON_ENABLE_PAGE_LEVEL_READER_IN_COMPACTION_DEFAULT);
    boolean enablePageReader;
    try {
      enablePageReader = Boolean.parseBoolean(enablePageReaderProperty);
    } catch (Exception e) {
      enablePageReader = Boolean.parseBoolean(
          CarbonCommonConstants.CARBON_ENABLE_PAGE_LEVEL_READER_IN_COMPACTION_DEFAULT);
    }
    LOGGER.info("Page level reader is set to: " + enablePageReader);
    return enablePageReader;
  }

}
