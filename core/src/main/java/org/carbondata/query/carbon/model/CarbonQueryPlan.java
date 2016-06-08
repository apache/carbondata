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

/**
 *
 */
package org.carbondata.query.carbon.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.query.expression.Expression;

/**
 * This class contains all the logical information about the query like dimensions,measures,
 * sort order, topN etc..
 */
public class CarbonQueryPlan implements Serializable {
  /**
   *
   */
  private static final long serialVersionUID = -9036044826928017164L;

  /**
   * Schema name , if user asks select * from datasight.employee.
   * then datasight is the schame name.
   * Remains null if the user does not select schema name.
   */
  private String schemaName;

  /**
   * Cube name .
   * if user asks select * from datasight.employee. then employee is the cube name.
   * It is mandatory.
   */
  private String cubeName;

  /**
   * List of dimensions.
   * Ex : select employee_name,department_name,sum(salary) from employee, then employee_name
   * and department_name are dimensions
   * If there is no dimensions asked in query then it would be remained as empty.
   */
  private List<QueryDimension> dimensions =
      new ArrayList<QueryDimension>(CarbonCommonConstants.CONSTANT_SIZE_TEN);

  /**
   * List of measures.
   * Ex : select employee_name,department_name,sum(salary) from employee, then sum(salary)
   * would be measure.
   * If there is no dimensions asked in query then it would be remained as empty.
   */
  private List<QueryMeasure> measures =
      new ArrayList<QueryMeasure>(CarbonCommonConstants.CONSTANT_SIZE_TEN);

  /**
   * Limit
   */
  private int limit = -1;

  /**
   * If it is detail query, no need to aggregate in backend
   */
  private boolean detailQuery;

  /**
   * expression
   */
  private Expression expression;

  /**
   * queryId
   */
  private String queryId;

  /**
   * outLocationPath
   */
  private String outLocationPath;

  /**
   * dimAggregatorInfoList
   */
  private Map<String, DimensionAggregatorInfo> dimAggregatorInfos =
      new LinkedHashMap<String, DimensionAggregatorInfo>();

  /**
   * isCountStarQuery
   */
  private boolean isCountStartQuery;

  private List<QueryDimension> sortedDimensions;

  /**
   * If it is raw detail query, no need to aggregate in backend. And it reurns with dictionary data
   * with out decoding.
   */
  private boolean rawDetailQuery;

  /**
   * Constructor created with cube name.
   *
   * @param cubeName
   */
  public CarbonQueryPlan(String cubeName) {
    this.cubeName = cubeName;
  }

  /**
   * Constructor created with schema name and cube name.
   *
   * @param schemaName
   * @param cubeName
   */
  public CarbonQueryPlan(String schemaName, String cubeName) {
    this.cubeName = cubeName;
    this.schemaName = schemaName;
  }

  /**
   * @return the dimensions
   */
  public List<QueryDimension> getDimensions() {
    return dimensions;
  }

  public void addDimension(QueryDimension dimension) {
    this.dimensions.add(dimension);
  }

  /**
   * @return the measures
   */
  public List<QueryMeasure> getMeasures() {
    return measures;
  }

  public void addMeasure(QueryMeasure measure) {
    this.measures.add(measure);
  }

  public Expression getFilterExpression() {
    return expression;
  }

  public void setFilterExpression(Expression expression) {
    this.expression = expression;
  }

  /**
   * @return the schemaName
   */
  public String getSchemaName() {
    return schemaName;
  }

  /**
   * @return the cubeName
   */
  public String getCubeName() {
    return cubeName;
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
   * @return the detailQuery
   */
  public boolean isDetailQuery() {
    return detailQuery;
  }

  /**
   * @param detailQuery the detailQuery to set
   */
  public void setDetailQuery(boolean detailQuery) {
    this.detailQuery = detailQuery;
  }

  public String getQueryId() {
    return queryId;
  }

  public void setQueryId(String queryId) {
    this.queryId = queryId;
  }

  public String getOutLocationPath() {
    return outLocationPath;
  }

  public void setOutLocationPath(String outLocationPath) {
    this.outLocationPath = outLocationPath;
  }

  public void addAggDimAggInfo(String columnName, String aggType, int queryOrder) {
    DimensionAggregatorInfo dimensionAggregatorInfo = dimAggregatorInfos.get(columnName);
    if (null == dimensionAggregatorInfo) {
      dimensionAggregatorInfo = new DimensionAggregatorInfo();
      dimensionAggregatorInfo.setColumnName(columnName);
      List<Integer> queryOrderList= new ArrayList<Integer>();
      queryOrderList.add(queryOrder);
      List<String> aggTypeList= new ArrayList<String>();
      aggTypeList.add(aggType);
      dimensionAggregatorInfo.setOrderList(queryOrderList);
      dimensionAggregatorInfo.setAggList(aggTypeList);
      dimAggregatorInfos.put(columnName, dimensionAggregatorInfo);
    } else {
      dimensionAggregatorInfo.getOrderList().add(queryOrder);
      dimensionAggregatorInfo.getAggList().add(aggType);
    }
  }

  public boolean isCountStarQuery() {
    return isCountStartQuery;
  }

  public void setCountStartQuery(boolean isCountStartQuery) {
    this.isCountStartQuery = isCountStartQuery;
  }

  public Map<String, DimensionAggregatorInfo> getDimAggregatorInfos() {
    return dimAggregatorInfos;
  }

  public List<QueryDimension> getSortedDimemsions() {
    return sortedDimensions;
  }

  public void setSortedDimemsions(List<QueryDimension> dims) {
    this.sortedDimensions = dims;
  }

  public boolean isRawDetailQuery() {
    return rawDetailQuery;
  }

  public void setRawDetailQuery(boolean rawDetailQuery) {
    this.rawDetailQuery = rawDetailQuery;
  }
}
