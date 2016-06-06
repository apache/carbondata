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
package org.carbondata.integration.spark.merger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.carbondata.core.carbon.CarbonTableIdentifier;
import org.carbondata.core.carbon.datastore.block.SegmentProperties;
import org.carbondata.core.carbon.datastore.block.TableBlockInfo;
import org.carbondata.core.carbon.datastore.block.TaskBlockInfo;
import org.carbondata.core.carbon.metadata.schema.table.CarbonTable;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonMeasure;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.iterator.CarbonIterator;
import org.carbondata.query.carbon.executor.QueryExecutor;
import org.carbondata.query.carbon.executor.QueryExecutorFactory;
import org.carbondata.query.carbon.executor.exception.QueryExecutionException;
import org.carbondata.query.carbon.model.QueryDimension;
import org.carbondata.query.carbon.model.QueryMeasure;
import org.carbondata.query.carbon.model.QueryModel;
import org.carbondata.query.carbon.result.BatchRawResult;

/**
 * Executor class for executing the query on the selected segments to be merged.
 * This will fire a select * query and get the raw result.
 */
public class CarbonCompactionExecutor {

  private QueryExecutor queryExecutor;
  private final SegmentProperties segmentProperties;
  private final String schemaName;
  private final String factTableName;
  private final Map<String, TaskBlockInfo> segmentMapping;
  private final String storePath;
  private CarbonTable carbonTable;

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonCompactionExecutor.class.getName());

  /**
   * Constructor
   * @param segmentMapping
   * @param segmentProperties
   * @param schemaName
   * @param factTableName
   * @param storePath
   * @param carbonTable
   */
  public CarbonCompactionExecutor(Map<String, TaskBlockInfo> segmentMapping,
      SegmentProperties segmentProperties, String schemaName, String factTableName,
      String storePath, CarbonTable carbonTable) {

    this.segmentMapping = segmentMapping;

    this.segmentProperties = segmentProperties;

    this.schemaName = schemaName;

    this.factTableName = factTableName;

    this.storePath = storePath;

    this.carbonTable = carbonTable;
  }

  /**
   * For processing of the table blocks.
   * @return List of Carbon iterators
   */
  public List<CarbonIterator<BatchRawResult>> processTableBlocks() throws QueryExecutionException {

    List<CarbonIterator<BatchRawResult>> resultList =
        new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    List<TableBlockInfo> list = null;
    QueryModel model = prepareQueryModel(list);
    // iterate each seg ID
    for (Map.Entry<String, TaskBlockInfo> taskMap : segmentMapping.entrySet()) {
      // for each segment get taskblock info
      TaskBlockInfo taskBlockInfo = taskMap.getValue();
      Set<String> taskBlockListMapping = taskBlockInfo.getTaskSet();

      for (String task : taskBlockListMapping) {

        list = taskBlockInfo.getTableBlockInfoList(task);
        Collections.sort(list);
        model.setTableBlockInfos(list);
        resultList.add(executeBlockList(list, model));

      }
    }

    return resultList;
  }

  /**
   * get executor and execute the query model.
   * @param blockList
   * @return
   */
  public CarbonIterator<BatchRawResult> executeBlockList(List<TableBlockInfo> blockList,
      QueryModel model) throws QueryExecutionException {

    model.setTableBlockInfos(blockList);
    this.queryExecutor = QueryExecutorFactory.getQueryExecutor(model);
    CarbonIterator<BatchRawResult> iter = null;
    try {
      iter = queryExecutor.execute(model);
    } catch (QueryExecutionException e) {
      LOGGER.error(e.getMessage());
      throw e;
    }

    return iter;
  }

  /**
   * Preparing of the query model.
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

    for (CarbonDimension dim : segmentProperties.getDimensions()) {
      QueryDimension queryDimension = new QueryDimension(dim.getColName());
      dims.add(queryDimension);
    }
    model.setQueryDimension(dims);

    List<QueryMeasure> msrs = new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    for (CarbonMeasure carbonMeasure : segmentProperties.getMeasures()) {
      QueryMeasure queryMeasure = new QueryMeasure(carbonMeasure.getColName());
      msrs.add(queryMeasure);
    }
    model.setQueryMeasures(msrs);

    model.setQueryId(System.nanoTime() + "");

    model.setAbsoluteTableIdentifier(new AbsoluteTableIdentifier(storePath,
        new CarbonTableIdentifier(schemaName, factTableName)));

    model.setAggTable(false);
    model.setLimit(-1);

    model.setTable(carbonTable);

    return model;
  }

}
