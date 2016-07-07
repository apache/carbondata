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

package org.carbondata.query.carbon.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.carbondata.core.cache.dictionary.Dictionary;
import org.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.carbondata.core.carbon.datastore.block.TableBlockInfo;
import org.carbondata.core.carbon.metadata.schema.table.CarbonTable;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonColumn;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonMeasure;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.query.expression.ColumnExpression;
import org.carbondata.query.expression.Expression;
import org.carbondata.query.expression.UnknownExpression;
import org.carbondata.query.expression.conditional.ConditionalExpression;
import org.carbondata.query.filter.resolver.FilterResolverIntf;

/**
 * Query model which will have all the detail
 * about the query, This will be sent from driver to executor '
 * This will be refereed to executing the query.
 */
public class QueryModel implements Serializable {

  /**
   * serialization version
   */
  private static final long serialVersionUID = -4674677234007089052L;

  /**
   * list of dimension selected for in query
   */
  private List<QueryDimension> queryDimension;

  /**
   * list of dimension in which sorting is applied
   */
  private List<QueryDimension> sortDimension;

  /**
   * list of measure selected in query
   */
  private List<QueryMeasure> queryMeasures;

  /**
   * query id
   */
  private String queryId;

  /**
   * to check if it a aggregate table
   */
  private boolean isAggTable;

  /**
   * filter tree
   */
  private FilterResolverIntf filterExpressionResolverTree;

  /**
   * in case of lime query we need to know how many
   * records will passed from executor
   */
  private int limit;

  /**
   * for applying aggregation on dimension
   */
  private List<DimensionAggregatorInfo> dimAggregationInfo;

  /**
   * custom aggregate expression
   */
  private List<CustomAggregateExpression> expressions;

  /**
   * to check if it is a count star query , so processing will be different
   */
  private boolean isCountStarQuery;

  /**
   * to check whether aggregation is required during query execution
   */
  private boolean detailQuery;

  /**
   * table block information in which query will be executed
   */
  private List<TableBlockInfo> tableBlockInfos;

  /**
   * sort in which dimension will be get sorted
   */
  private byte[] sortOrder;

  /**
   * absolute table identifier
   */
  private AbsoluteTableIdentifier absoluteTableIdentifier;
  /**
   * in case of detail query with sort we are spilling to disk
   * to this location will be used to write the temp file in this location
   */
  private String queryTempLocation;

  /**
   * To handle most of the computation in query engines like spark and hive, carbon should give
   * raw detailed records to it.
   */
  private boolean forcedDetailRawQuery;

  /**
   * paritition column list
   */
  private List<String> paritionColumns;

  /**
   * this will hold the information about the dictionary dimension
   * which to
   */
  public transient Map<String, Dictionary> columnToDictionaryMapping;

  /**
   * Number of records to keep in memory.
   */
  public int inMemoryRecordSize;

  /**
   * table on which query will be executed
   * TODO need to remove this ad pass only the path
   * and carbon metadata will load the table from metadata file
   */
  private CarbonTable table;

  public QueryModel() {
    tableBlockInfos = new ArrayList<TableBlockInfo>();
    dimAggregationInfo =
        new ArrayList<DimensionAggregatorInfo>();
    expressions =
        new ArrayList<CustomAggregateExpression>();
    queryDimension = new ArrayList<QueryDimension>();
    queryMeasures = new ArrayList<QueryMeasure>();
    sortDimension = new ArrayList<QueryDimension>();
    sortOrder = new byte[0];
    paritionColumns = new ArrayList<String>();

  }

  public static QueryModel createModel(AbsoluteTableIdentifier absoluteTableIdentifier,
      CarbonQueryPlan queryPlan, CarbonTable carbonTable) {
    QueryModel queryModel = new QueryModel();
    String factTableName = carbonTable.getFactTableName();
    queryModel.setAbsoluteTableIdentifier(absoluteTableIdentifier);

    fillQueryModel(queryPlan, carbonTable, queryModel, factTableName);

    fillDimensionAggregator(queryPlan, queryModel);
    queryModel.setLimit(queryPlan.getLimit());
    queryModel.setDetailQuery(queryPlan.isDetailQuery());
    queryModel.setForcedDetailRawQuery(queryPlan.isRawDetailQuery());
    queryModel.setQueryId(queryPlan.getQueryId());
    queryModel.setQueryTempLocation(queryPlan.getOutLocationPath());
    return queryModel;
  }

  private static void fillQueryModel(CarbonQueryPlan queryPlan, CarbonTable carbonTable,
      QueryModel queryModel, String factTableName) {
    queryModel.setAbsoluteTableIdentifier(carbonTable.getAbsoluteTableIdentifier());
    queryModel.setQueryDimension(queryPlan.getDimensions());
    fillSortInfoInModel(queryModel, queryPlan.getSortedDimemsions());
    queryModel.setQueryMeasures(
        queryPlan.getMeasures());
    if (null != queryPlan.getFilterExpression()) {
      processFilterExpression(queryPlan.getFilterExpression(),
          carbonTable.getDimensionByTableName(factTableName),
          carbonTable.getMeasureByTableName(factTableName));
    }
    queryModel.setCountStarQuery(queryPlan.isCountStarQuery());
    //TODO need to remove this code, and executor will load the table
    // from file metadata
    queryModel.setTable(carbonTable);
  }

  private static void fillSortInfoInModel(QueryModel executorModel,
      List<QueryDimension> sortedDims) {
    if (null != sortedDims) {
      byte[] sortOrderByteArray = new byte[sortedDims.size()];
      int i = 0;
      for (QueryColumn mdim : sortedDims) {
        sortOrderByteArray[i++] = (byte) mdim.getSortOrder().ordinal();
      }
      executorModel.setSortOrder(sortOrderByteArray);
      executorModel.setSortDimension(sortedDims);
    } else {
      executorModel.setSortOrder(new byte[0]);
      executorModel.setSortDimension(new ArrayList<QueryDimension>(0));
    }

  }

  private static void fillDimensionAggregator(CarbonQueryPlan logicalPlan,
      QueryModel executorModel) {
    Map<String, DimensionAggregatorInfo> dimAggregatorInfos = logicalPlan.getDimAggregatorInfos();
    List<DimensionAggregatorInfo> dimensionAggregatorInfos =
        new ArrayList<DimensionAggregatorInfo>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    for (Map.Entry<String, DimensionAggregatorInfo> entry : dimAggregatorInfos.entrySet()) {
      dimensionAggregatorInfos.add(entry.getValue());
    }
    executorModel.setDimAggregationInfo(dimensionAggregatorInfos);
  }

  public static void processFilterExpression(
      Expression filterExpression, List<CarbonDimension> dimensions, List<CarbonMeasure> measures) {
    if (null != filterExpression) {
      if (null != filterExpression.getChildren() && filterExpression.getChildren().size() == 0) {
        if (filterExpression instanceof ConditionalExpression) {
          List<ColumnExpression> listOfCol =
              ((ConditionalExpression) filterExpression).getColumnList();
          for (ColumnExpression expression : listOfCol) {
            setDimAndMsrColumnNode(dimensions, measures, (ColumnExpression) expression);
          }

        }
      }
      for (Expression expression : filterExpression.getChildren()) {

        if (expression instanceof ColumnExpression) {
          setDimAndMsrColumnNode(dimensions, measures, (ColumnExpression) expression);
        } else if (expression instanceof UnknownExpression) {
          UnknownExpression exp = ((UnknownExpression) expression);
          List<ColumnExpression> listOfColExpression = exp.getAllColumnList();
          for (ColumnExpression col : listOfColExpression) {
            setDimAndMsrColumnNode(dimensions, measures, col);
          }
        } else {
          processFilterExpression(expression, dimensions, measures);
        }
      }
    }

  }

  private static CarbonMeasure getCarbonMetadataMeasure(String name, List<CarbonMeasure> measures) {
    for (CarbonMeasure measure : measures) {
      if (measure.getColName().equalsIgnoreCase(name)) {
        return measure;
      }
    }
    return null;
  }

  private static void setDimAndMsrColumnNode(List<CarbonDimension> dimensions,
      List<CarbonMeasure> measures, ColumnExpression col) {
    CarbonDimension dim;
    CarbonMeasure msr;
    String columnName;
    columnName = col.getColumnName();
    dim = CarbonUtil.findDimension(dimensions, columnName);
    col.setCarbonColumn(dim);
    col.setDimension(dim);
    col.setDimension(true);
    if (null == dim) {
      msr = getCarbonMetadataMeasure(columnName, measures);
      col.setCarbonColumn(msr);
      col.setDimension(false);
    }
  }

  /**
   * It gets the projection columns
   */
  public CarbonColumn[] getProjectionColumns() {
    CarbonColumn[] carbonColumns =
        new CarbonColumn[getQueryDimension().size() + getQueryMeasures()
            .size()];
    for (QueryDimension dimension : getQueryDimension()) {
      carbonColumns[dimension.getQueryOrder()] = dimension.getDimension();
    }
    for (QueryMeasure msr : getQueryMeasures()) {
      carbonColumns[msr.getQueryOrder()] = msr.getMeasure();
    }
    return carbonColumns;
  }

  /**
   * @return the queryDimension
   */
  public List<QueryDimension> getQueryDimension() {
    return queryDimension;
  }

  /**
   * @param queryDimension the queryDimension to set
   */
  public void setQueryDimension(List<QueryDimension> queryDimension) {
    this.queryDimension = queryDimension;
  }

  /**
   * @return the queryMeasures
   */
  public List<QueryMeasure> getQueryMeasures() {
    return queryMeasures;
  }

  /**
   * @param queryMeasures the queryMeasures to set
   */
  public void setQueryMeasures(List<QueryMeasure> queryMeasures) {
    this.queryMeasures = queryMeasures;
  }

  /**
   * @return the queryId
   */
  public String getQueryId() {
    return queryId;
  }

  /**
   * @param queryId the queryId to set
   */
  public void setQueryId(String queryId) {
    this.queryId = queryId;
  }

  /**
   * @return the isAggTable
   */
  public boolean isAggTable() {
    return isAggTable;
  }

  /**
   * @param isAggTable the isAggTable to set
   */
  public void setAggTable(boolean isAggTable) {
    this.isAggTable = isAggTable;
  }

  /**
   * @return the limit
   */
  public int getLimit() {
    return limit;
  }

  /**
   * @param limit the limit to set
   */
  public void setLimit(int limit) {
    this.limit = limit;
  }

  /**
   * @return the isCountStarQuery
   */
  public boolean isCountStarQuery() {
    return isCountStarQuery;
  }

  /**
   * @param isCountStarQuery the isCountStarQuery to set
   */
  public void setCountStarQuery(boolean isCountStarQuery) {
    this.isCountStarQuery = isCountStarQuery;
  }

  /**
   * @return the isdetailQuery
   */
  public boolean isDetailQuery() {
    return detailQuery;
  }

  public void setDetailQuery(boolean detailQuery) {
    this.detailQuery = detailQuery;
  }

  /**
   * @return the dimAggregationInfo
   */
  public List<DimensionAggregatorInfo> getDimAggregationInfo() {
    return dimAggregationInfo;
  }

  /**
   * @param dimAggregationInfo the dimAggregationInfo to set
   */
  public void setDimAggregationInfo(List<DimensionAggregatorInfo> dimAggregationInfo) {
    this.dimAggregationInfo = dimAggregationInfo;
  }

  /**
   * @return the tableBlockInfos
   */
  public List<TableBlockInfo> getTableBlockInfos() {
    return tableBlockInfos;
  }

  /**
   * @param tableBlockInfos the tableBlockInfos to set
   */
  public void setTableBlockInfos(List<TableBlockInfo> tableBlockInfos) {
    this.tableBlockInfos = tableBlockInfos;
  }

  /**
   * @return the expressions
   */
  public List<CustomAggregateExpression> getExpressions() {
    return expressions;
  }

  /**
   * @param expressions the expressions to set
   */
  public void setExpressions(List<CustomAggregateExpression> expressions) {
    this.expressions = expressions;
  }

  /**
   * @return the queryTempLocation
   */
  public String getQueryTempLocation() {
    return queryTempLocation;
  }

  /**
   * @param queryTempLocation the queryTempLocation to set
   */
  public void setQueryTempLocation(String queryTempLocation) {
    this.queryTempLocation = queryTempLocation;
  }

  /**
   * @return the sortOrder
   */
  public byte[] getSortOrder() {
    return sortOrder;
  }

  /**
   * @param sortOrder the sortOrder to set
   */
  public void setSortOrder(byte[] sortOrder) {
    this.sortOrder = sortOrder;
  }

  /**
   * @return the sortDimension
   */
  public List<QueryDimension> getSortDimension() {
    return sortDimension;
  }

  /**
   * @param sortDimension the sortDimension to set
   */
  public void setSortDimension(List<QueryDimension> sortDimension) {
    this.sortDimension = sortDimension;
  }

  /**
   * @return the filterEvaluatorTree
   */
  public FilterResolverIntf getFilterExpressionResolverTree() {
    return filterExpressionResolverTree;
  }

  public void setFilterExpressionResolverTree(FilterResolverIntf filterExpressionResolverTree) {
    this.filterExpressionResolverTree = filterExpressionResolverTree;
  }

  /**
   * @return the absoluteTableIdentifier
   */
  public AbsoluteTableIdentifier getAbsoluteTableIdentifier() {
    return absoluteTableIdentifier;
  }

  /**
   * @param absoluteTableIdentifier the absoluteTableIdentifier to set
   */
  public void setAbsoluteTableIdentifier(AbsoluteTableIdentifier absoluteTableIdentifier) {
    this.absoluteTableIdentifier = absoluteTableIdentifier;
  }

  /**
   * @return the paritionColumns
   */
  public List<String> getParitionColumns() {
    return paritionColumns;
  }

  /**
   * @param paritionColumns the paritionColumns to set
   */
  public void setParitionColumns(List<String> paritionColumns) {
    this.paritionColumns = paritionColumns;
  }

  /**
   * @return the table
   */
  public CarbonTable getTable() {
    return table;
  }

  /**
   * @param table the table to set
   */
  public void setTable(CarbonTable table) {
    this.table = table;
  }

  public boolean isForcedDetailRawQuery() {
    return forcedDetailRawQuery;
  }

  public void setForcedDetailRawQuery(boolean forcedDetailRawQuery) {
    this.forcedDetailRawQuery = forcedDetailRawQuery;
  }

  /**
   * @return
   */
  public Map<String, Dictionary> getColumnToDictionaryMapping() {
    return columnToDictionaryMapping;
  }

  /**
   * @param columnToDictionaryMapping
   */
  public void setColumnToDictionaryMapping(Map<String, Dictionary> columnToDictionaryMapping) {
    this.columnToDictionaryMapping = columnToDictionaryMapping;
  }

  public int getInMemoryRecordSize() {
    return inMemoryRecordSize;
  }

  public void setInMemoryRecordSize(int inMemoryRecordSize) {
    this.inMemoryRecordSize = inMemoryRecordSize;
  }
}
