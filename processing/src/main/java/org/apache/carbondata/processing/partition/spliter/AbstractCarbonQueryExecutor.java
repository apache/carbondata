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

package org.apache.carbondata.processing.partition.spliter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.datastore.block.TaskBlockInfo;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.scan.executor.QueryExecutor;
import org.apache.carbondata.core.scan.executor.QueryExecutorFactory;
import org.apache.carbondata.core.scan.executor.exception.QueryExecutionException;
import org.apache.carbondata.core.scan.model.QueryDimension;
import org.apache.carbondata.core.scan.model.QueryMeasure;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.result.BatchResult;
import org.apache.carbondata.core.util.CarbonUtil;

public abstract class AbstractCarbonQueryExecutor {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(AbstractCarbonQueryExecutor.class.getName());
  protected CarbonTable carbonTable;
  protected QueryModel queryModel;
  protected QueryExecutor queryExecutor;
  protected Map<String, TaskBlockInfo> segmentMapping;

  /**
   * get executor and execute the query model.
   *
   * @param blockList
   * @return
   */
  protected CarbonIterator<BatchResult> executeBlockList(List<TableBlockInfo> blockList)
      throws QueryExecutionException, IOException {
    queryModel.setTableBlockInfos(blockList);
    this.queryExecutor = QueryExecutorFactory.getQueryExecutor(queryModel);
    return queryExecutor.execute(queryModel);
  }

  /**
   * Preparing of the query model.
   *
   * @param blockList
   * @return
   */
  protected QueryModel prepareQueryModel(List<TableBlockInfo> blockList) {
    QueryModel model = new QueryModel();
    model.setTableBlockInfos(blockList);
    model.setForcedDetailRawQuery(true);
    model.setFilterExpressionResolverTree(null);

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
}
