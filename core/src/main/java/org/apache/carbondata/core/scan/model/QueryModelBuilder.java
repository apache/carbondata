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

package org.apache.carbondata.core.scan.model;

import java.util.List;

import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;

public class QueryModelBuilder {

  private CarbonTable carbonTable;

  public QueryModelBuilder(CarbonTable carbonTable) {
    this.carbonTable = carbonTable;
  }

  public QueryModel build(String[] projectionColumnNames, Expression filterExpression) {
    QueryModel queryModel = QueryModel.newInstance(carbonTable);
    QueryProjection projection = carbonTable.createProjection(projectionColumnNames);
    queryModel.setProjection(projection);
    boolean[] isFilterDimensions = new boolean[carbonTable.getDimensionOrdinalMax()];
    boolean[] isFilterMeasures = new boolean[carbonTable.getAllMeasures().size()];
    carbonTable.processFilterExpression(filterExpression, isFilterDimensions, isFilterMeasures);
    queryModel.setIsFilterDimensions(isFilterDimensions);
    queryModel.setIsFilterMeasures(isFilterMeasures);
    FilterResolverIntf filterIntf = carbonTable.resolveFilter(filterExpression, null);
    queryModel.setFilterExpressionResolverTree(filterIntf);
    return queryModel;
  }

  public QueryModel build(Expression filterExpression) {
    QueryProjection projection = new QueryProjection();

    List<CarbonDimension> dimensions = carbonTable.getDimensions();
    for (int i = 0; i < dimensions.size(); i++) {
      projection.addDimension(dimensions.get(i), i);
    }
    List<CarbonMeasure> measures = carbonTable.getMeasures();
    for (int i = 0; i < measures.size(); i++) {
      projection.addMeasure(measures.get(i), i);
    }

    QueryModel queryModel = QueryModel.newInstance(carbonTable);
    queryModel.setProjection(projection);
    boolean[] isFilterDimensions = new boolean[carbonTable.getDimensionOrdinalMax()];
    boolean[] isFilterMeasures = new boolean[carbonTable.getAllMeasures().size()];
    carbonTable.processFilterExpression(filterExpression, isFilterDimensions, isFilterMeasures);
    queryModel.setIsFilterDimensions(isFilterDimensions);
    queryModel.setIsFilterMeasures(isFilterMeasures);
    FilterResolverIntf filterIntf = carbonTable.resolveFilter(filterExpression, null);
    queryModel.setFilterExpressionResolverTree(filterIntf);
    return queryModel;
  }
}
