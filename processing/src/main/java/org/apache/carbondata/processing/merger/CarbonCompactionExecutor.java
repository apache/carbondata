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
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.datastore.block.TaskBlockInfo;
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.scan.executor.QueryExecutor;
import org.apache.carbondata.core.scan.executor.QueryExecutorFactory;
import org.apache.carbondata.core.scan.executor.exception.QueryExecutionException;
import org.apache.carbondata.core.scan.model.QueryDimension;
import org.apache.carbondata.core.scan.model.QueryMeasure;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.result.BatchResult;
import org.apache.carbondata.core.scan.result.iterator.RawResultIterator;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeUtil;

/**
 * Executor class for executing the query on the selected segments to be merged.
 * This will fire a select * query and get the raw result.
 */
public class CarbonCompactionExecutor {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonCompactionExecutor.class.getName());
  private final Map<String, List<DataFileFooter>> dataFileMetadataSegMapping;
  private final SegmentProperties destinationSegProperties;
  private final Map<String, TaskBlockInfo> segmentMapping;
  private List<QueryExecutor> queryExecutorList;
  private CarbonTable carbonTable;
  private QueryModel queryModel;

  /**
   * flag to check whether any restructured block exists in the blocks selected for compaction.
   * Based on this decision will be taken whether complete data has to be sorted again
   */
  private boolean restructuredBlockExists;

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
      boolean restructuredBlockExists) {
    this.segmentMapping = segmentMapping;
    this.destinationSegProperties = segmentProperties;
    this.carbonTable = carbonTable;
    this.dataFileMetadataSegMapping = dataFileMetadataSegMapping;
    this.restructuredBlockExists = restructuredBlockExists;
    queryExecutorList = new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
  }

  /**
   * For processing of the table blocks.
   *
   * @return List of Carbon iterators
   */
  public List<RawResultIterator> processTableBlocks() throws QueryExecutionException, IOException {
    List<RawResultIterator> resultList =
        new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    List<TableBlockInfo> list = null;
    queryModel = prepareQueryModel(list);
    // iterate each seg ID
    for (Map.Entry<String, TaskBlockInfo> taskMap : segmentMapping.entrySet()) {
      String segmentId = taskMap.getKey();
      List<DataFileFooter> listMetadata = dataFileMetadataSegMapping.get(segmentId);
      SegmentProperties sourceSegProperties = getSourceSegmentProperties(listMetadata);
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
  private CarbonIterator<BatchResult> executeBlockList(List<TableBlockInfo> blockList)
      throws QueryExecutionException, IOException {
    queryModel.setTableBlockInfos(blockList);
    QueryExecutor queryExecutor = QueryExecutorFactory.getQueryExecutor(queryModel);
    queryExecutorList.add(queryExecutor);
    return queryExecutor.execute(queryModel);
  }

  /**
   * Below method will be used
   * for cleanup
   */
  public void finish() {
    try {
      for (QueryExecutor queryExecutor : queryExecutorList) {
        queryExecutor.finish();
      }
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
  private QueryModel prepareQueryModel(List<TableBlockInfo> blockList) {
    QueryModel model = new QueryModel();
    model.setTableBlockInfos(blockList);
    model.setForcedDetailRawQuery(true);
    model.setFilterExpressionResolverTree(null);
    model.setConverter(DataTypeUtil.getDataTypeConverter());

    List<QueryDimension> dims = new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    List<CarbonDimension> dimensions =
        carbonTable.getDimensionByTableName(carbonTable.getTableName());
    for (CarbonDimension dim : dimensions) {
      // check if dimension is deleted
      QueryDimension queryDimension = new QueryDimension(dim.getColName());
      queryDimension.setDimension(dim);
      dims.add(queryDimension);
    }
    model.setQueryDimension(dims);

    List<QueryMeasure> msrs = new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    List<CarbonMeasure> measures =
        carbonTable.getMeasureByTableName(carbonTable.getTableName());
    for (CarbonMeasure carbonMeasure : measures) {
      // check if measure is deleted
      QueryMeasure queryMeasure = new QueryMeasure(carbonMeasure.getColName());
      queryMeasure.setMeasure(carbonMeasure);
      msrs.add(queryMeasure);
    }
    model.setQueryMeasures(msrs);
    model.setQueryId(System.nanoTime() + "");
    model.setAbsoluteTableIdentifier(carbonTable.getAbsoluteTableIdentifier());
    model.setTable(carbonTable);
    return model;
  }

}
