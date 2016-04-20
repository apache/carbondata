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

import org.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.carbondata.core.carbon.datastore.block.TableBlockInfo;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonMeasure;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.query.evaluators.FilterEvaluator;
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
  private List<CarbonDimension> queryDimension;

  /**
   * list of dimension in which sorting is applied
   */
  private List<CarbonDimension> sortDimension;

  /**
   * list of measure selected in query
   */
  private List<CarbonMeasure> queryMeasures;

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
   * paritition column list
   */
  private List<String> paritionColumns;

  public QueryModel() {
    tableBlockInfos = new ArrayList<TableBlockInfo>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    dimAggregationInfo =
        new ArrayList<DimensionAggregatorInfo>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    expressions =
        new ArrayList<CustomAggregateExpression>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

  }

  /**
   * @return the queryDimension
   */
  public List<CarbonDimension> getQueryDimension() {
    return queryDimension;
  }

  /**
   * @param queryDimension the queryDimension to set
   */
  public void setQueryDimension(List<CarbonDimension> queryDimension) {
    this.queryDimension = queryDimension;
  }

  /**
   * @return the queryMeasures
   */
  public List<CarbonMeasure> getQueryMeasures() {
    return queryMeasures;
  }

  /**
   * @param queryMeasures the queryMeasures to set
   */
  public void setQueryMeasures(List<CarbonMeasure> queryMeasures) {
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

  /**
   * @param isdetailQuery the isdetailQuery to set
   */
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
  public List<CarbonDimension> getSortDimension() {
    return sortDimension;
  }

  /**
   * @param sortDimension the sortDimension to set
   */
  public void setSortDimension(List<CarbonDimension> sortDimension) {
    this.sortDimension = sortDimension;
  }

  /**
   * @return the filterEvaluatorTree
   */
  public FilterResolverIntf getFilterExpressionResolverTree() {
    return filterExpressionResolverTree;
  }

  /**
   * @param filterEvaluatorTree the filterEvaluatorTree to set
   */
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

}
