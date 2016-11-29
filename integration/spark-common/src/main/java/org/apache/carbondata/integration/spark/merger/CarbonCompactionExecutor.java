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
package org.apache.carbondata.integration.spark.merger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.carbon.datastore.block.SegmentProperties;
import org.apache.carbondata.core.carbon.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.carbon.datastore.block.TaskBlockInfo;
import org.apache.carbondata.core.carbon.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.carbon.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.scan.executor.QueryExecutor;
import org.apache.carbondata.scan.executor.QueryExecutorFactory;
import org.apache.carbondata.scan.executor.exception.QueryExecutionException;
import org.apache.carbondata.scan.model.QueryDimension;
import org.apache.carbondata.scan.model.QueryMeasure;
import org.apache.carbondata.scan.model.QueryModel;
import org.apache.carbondata.scan.result.BatchResult;
import org.apache.carbondata.scan.result.iterator.RawResultIterator;

/**
 * Executor class for executing the query on the selected segments to be merged.
 * This will fire a select * query and get the raw result.
 */
public class CarbonCompactionExecutor {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonCompactionExecutor.class.getName());
  private final Map<String, List<DataFileFooter>> dataFileMetadataSegMapping;
  private final SegmentProperties destinationSegProperties;
  private final String databaseName;
  private final String factTableName;
  private final Map<String, TaskBlockInfo> segmentMapping;
  private final String storePath;
  private QueryExecutor queryExecutor;
  private CarbonTable carbonTable;
  private QueryModel queryModel;

  /**
   * Constructor
   *
   * @param segmentMapping
   * @param segmentProperties
   * @param databaseName
   * @param factTableName
   * @param storePath
   * @param carbonTable
   */
  public CarbonCompactionExecutor(Map<String, TaskBlockInfo> segmentMapping,
      SegmentProperties segmentProperties, String databaseName, String factTableName,
      String storePath, CarbonTable carbonTable,
      Map<String, List<DataFileFooter>> dataFileMetadataSegMapping) {

    this.segmentMapping = segmentMapping;

    this.destinationSegProperties = segmentProperties;

    this.databaseName = databaseName;

    this.factTableName = factTableName;

    this.storePath = storePath;

    this.carbonTable = carbonTable;

    this.dataFileMetadataSegMapping = dataFileMetadataSegMapping;
  }

  /**
   * For processing of the table blocks.
   *
   * @return List of Carbon iterators
   */
  public List<RawResultIterator> processTableBlocks() throws QueryExecutionException {

    List<RawResultIterator> resultList =
        new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    List<TableBlockInfo> list = null;
    queryModel = prepareQueryModel(list);
    // iterate each seg ID
    for (Map.Entry<String, TaskBlockInfo> taskMap : segmentMapping.entrySet()) {
      String segmentId = taskMap.getKey();
      List<DataFileFooter> listMetadata = dataFileMetadataSegMapping.get(segmentId);

      int[] colCardinality = listMetadata.get(0).getSegmentInfo().getColumnCardinality();

      SegmentProperties sourceSegProperties =
          new SegmentProperties(listMetadata.get(0).getColumnInTable(), colCardinality);

      // for each segment get taskblock info
      TaskBlockInfo taskBlockInfo = taskMap.getValue();
      Set<String> taskBlockListMapping = taskBlockInfo.getTaskSet();

      for (String task : taskBlockListMapping) {

        list = taskBlockInfo.getTableBlockInfoList(task);
        Collections.sort(list);
        LOGGER.info("for task -" + task + "-block size is -" + list.size());
        queryModel.setTableBlockInfos(list);
        resultList.add(new RawResultIterator(executeBlockList(list), sourceSegProperties,
            destinationSegProperties));

      }
    }

    return resultList;
  }

  /**
   * get executor and execute the query model.
   *
   * @param blockList
   * @return
   */
  private CarbonIterator<BatchResult> executeBlockList(List<TableBlockInfo> blockList)
      throws QueryExecutionException {

    queryModel.setTableBlockInfos(blockList);
    this.queryExecutor = QueryExecutorFactory.getQueryExecutor();
    CarbonIterator<BatchResult> iter = null;
    try {
      iter = queryExecutor.execute(queryModel);
    } catch (QueryExecutionException e) {
      LOGGER.error(e.getMessage());
      throw e;
    }

    return iter;
  }

  /**
   * Below method will be used
   * for cleanup
   */
  public void finish() {
    try {
      queryExecutor.finish();
    } catch (QueryExecutionException e) {
      LOGGER.error(e, "Problem while finish: ");
    }
    clearDictionaryFromQueryModel();
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
   * Preparing of the query model.
   *
   * @param blockList
   * @return
   */
  public QueryModel prepareQueryModel(List<TableBlockInfo> blockList) {

    QueryModel model = new QueryModel();

    model.setTableBlockInfos(blockList);
    model.setCountStarQuery(false);
    model.setDetailQuery(true);
    model.setForcedDetailRawQuery(true);
    model.setFilterExpressionResolverTree(null);

    List<QueryDimension> dims = new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    for (CarbonDimension dim : destinationSegProperties.getDimensions()) {
      QueryDimension queryDimension = new QueryDimension(dim.getColName());
      dims.add(queryDimension);
    }
    model.setQueryDimension(dims);

    List<QueryMeasure> msrs = new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    for (CarbonMeasure carbonMeasure : destinationSegProperties.getMeasures()) {
      QueryMeasure queryMeasure = new QueryMeasure(carbonMeasure.getColName());
      msrs.add(queryMeasure);
    }
    model.setQueryMeasures(msrs);

    model.setQueryId(System.nanoTime() + "");

    model.setAbsoluteTableIdentifier(carbonTable.getAbsoluteTableIdentifier());

    model.setAggTable(false);
    model.setLimit(-1);

    model.setTable(carbonTable);

    model.setInMemoryRecordSize(CarbonCommonConstants.COMPACTION_INMEMORY_RECORD_SIZE);

    return model;
  }

}
