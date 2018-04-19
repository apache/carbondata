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
import java.util.Objects;

import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.filter.SingleTableProvider;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.util.DataTypeConverter;

public class QueryModelBuilder {

  private CarbonTable table;
  private QueryProjection projection;
  private Expression filterExpression;
  private DataTypeConverter dataTypeConverter;
  private boolean forcedDetailRawQuery;
  private boolean readPageByPage;

  public QueryModelBuilder(CarbonTable table) {
    this.table = table;
  }

  public QueryModelBuilder projectColumns(String[] projectionColumns) {
    Objects.requireNonNull(projectionColumns);
    String factTableName = table.getTableName();
    QueryProjection projection = new QueryProjection();

    int i = 0;
    for (String projectionColumnName : projectionColumns) {
      CarbonDimension dimension = table.getDimensionByName(factTableName, projectionColumnName);
      if (dimension != null) {
        projection.addDimension(dimension, i);
        i++;
      } else {
        CarbonMeasure measure = table.getMeasureByName(factTableName, projectionColumnName);
        if (measure == null) {
          throw new RuntimeException(projectionColumnName +
              " column not found in the table " + factTableName);
        }
        projection.addMeasure(measure, i);
        i++;
      }
    }

    this.projection = projection;
    return this;
  }

  public QueryModelBuilder projectAllColumns() {
    QueryProjection projection = new QueryProjection();
    List<CarbonDimension> dimensions = table.getDimensions();
    for (int i = 0; i < dimensions.size(); i++) {
      projection.addDimension(dimensions.get(i), i);
    }
    List<CarbonMeasure> measures = table.getMeasures();
    for (int i = 0; i < measures.size(); i++) {
      projection.addMeasure(measures.get(i), i);
    }
    this.projection = projection;
    return this;
  }

  public QueryModelBuilder filterExpression(Expression filterExpression) {
    this.filterExpression = filterExpression;
    return this;
  }

  public QueryModelBuilder dataConverter(DataTypeConverter dataTypeConverter) {
    this.dataTypeConverter = dataTypeConverter;
    return this;
  }

  public QueryModelBuilder enableForcedDetailRawQuery() {
    this.forcedDetailRawQuery = true;
    return this;
  }

  public QueryModelBuilder enableReadPageByPage() {
    this.readPageByPage = true;
    return this;
  }

  public QueryModel build() {
    QueryModel queryModel = QueryModel.newInstance(table);
    queryModel.setConverter(dataTypeConverter);
    queryModel.setForcedDetailRawQuery(forcedDetailRawQuery);
    queryModel.setReadPageByPage(readPageByPage);
    queryModel.setProjection(projection);

    // set the filter to the query model in order to filter blocklet before scan
    boolean[] isFilterDimensions = new boolean[table.getDimensionOrdinalMax()];
    boolean[] isFilterMeasures = new boolean[table.getAllMeasures().size()];
    table.processFilterExpression(filterExpression, isFilterDimensions, isFilterMeasures);
    queryModel.setIsFilterDimensions(isFilterDimensions);
    queryModel.setIsFilterMeasures(isFilterMeasures);
    FilterResolverIntf filterIntf =
        table.resolveFilter(filterExpression, new SingleTableProvider(table));
    queryModel.setFilterExpressionResolverTree(filterIntf);
    return queryModel;
  }
}
