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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.util.DataTypeConverter;

import org.apache.log4j.Logger;

public class QueryModelBuilder {

  private CarbonTable table;
  private QueryProjection projection;
  private Expression filterExpression;
  private DataTypeConverter dataTypeConverter;
  private boolean forcedDetailRawQuery;
  private boolean readPageByPage;
  private boolean convertToRangeFilter = true;
  /**
   * log information
   */
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(QueryModelBuilder.class.getName());

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
        CarbonDimension complexParentDimension = dimension.getComplexParentDimension();
        if (null != complexParentDimension && dimension.hasEncoding(Encoding.DICTIONARY)) {
          if (!isAlreadyExists(complexParentDimension, projection.getDimensions())) {
            projection.addDimension(complexParentDimension, i);
            i++;
          }
        } else {
          projection.addDimension(dimension, i);
          i++;
        }
      } else {
        CarbonMeasure measure = table.getMeasureByName(factTableName, projectionColumnName);
        if (measure == null) {
          throw new RuntimeException(
              projectionColumnName + " column not found in the table " + factTableName);
        }
        projection.addMeasure(measure, i);
        i++;
      }
    }
    projection = optimizeProjectionForComplexColumns(projection, projectionColumns, factTableName);
    List<String> projectionDimensionAndMeasures = new ArrayList<>();
    this.projection = projection;
    for (ProjectionDimension projectionDimension : projection.getDimensions()) {
      projectionDimensionAndMeasures.add(projectionDimension.getColumnName());
    }
    for (ProjectionMeasure projectionMeasure : projection.getMeasures()) {
      projectionDimensionAndMeasures.add(projectionMeasure.getColumnName());
    }
    LOGGER.info("Projection Columns: " + projectionDimensionAndMeasures);
    return this;
  }

  /**
   * For complex dimensions, check if the dimension already exists in the projection list or not
   *
   * @param dimension
   * @param projectionDimensions
   * @return
   */
  private boolean isAlreadyExists(CarbonDimension dimension,
      List<ProjectionDimension> projectionDimensions) {
    boolean exists = false;
    for (ProjectionDimension projectionDimension : projectionDimensions) {
      if (dimension.getColName().equals(projectionDimension.getColumnName())) {
        exists = true;
        break;
      }
    }
    return exists;
  }

  private QueryProjection optimizeProjectionForComplexColumns(QueryProjection projection,
      String[] projectionColumns, String factTableName) {
    // Get the List of Complex Column Projection.
    // The optimization techniques which can be applied are
    // A. Merging in Driver Side
    // B. Merging in the result Collector side.
    // Merging is driver side cases are
    // Driver merging will eliminate one of the CarbonDimension.
    // Executor merging will merge the column output in Result Collector.
    // In this routine we are going to do driver merging and leave executor merging.
    Map<Integer, List<Integer>> complexColumnMap = new HashMap<>();
    List<ProjectionDimension> carbonDimensions = projection.getDimensions();
    // Traverse and find out if the top most parent of projection column is already there
    List<CarbonDimension> projectionDimenesionToBeMerged = new ArrayList<>();
    for (ProjectionDimension projectionDimension : carbonDimensions) {
      CarbonDimension complexParentDimension =
          projectionDimension.getDimension().getComplexParentDimension();
      if (null != complexParentDimension && isAlreadyExists(complexParentDimension,
          carbonDimensions)) {
        projectionDimenesionToBeMerged.add(projectionDimension.getDimension());
      }
    }

    if (projectionDimenesionToBeMerged.size() != 0) {
      projection =
          removeMergedDimensions(projectionDimenesionToBeMerged, projectionColumns, factTableName);
      carbonDimensions = projection.getDimensions();
    }

    for (ProjectionDimension cols : carbonDimensions) {
      // get all the Projections with Parent Ordinal Set.
      if (null != cols.getDimension().getComplexParentDimension()) {
        if (complexColumnMap.get(cols.getDimension().getComplexParentDimension().getOrdinal())
            != null) {
          List<Integer> childColumns =
              complexColumnMap.get(cols.getDimension().getComplexParentDimension().getOrdinal());
          childColumns.add(cols.getDimension().getOrdinal());
          complexColumnMap
              .put(cols.getDimension().getComplexParentDimension().getOrdinal(), childColumns);
        } else {
          List<Integer> childColumns = new ArrayList<>();
          childColumns.add(cols.getDimension().getOrdinal());
          complexColumnMap
              .put(cols.getDimension().getComplexParentDimension().getOrdinal(), childColumns);
        }
      }
    }

    // Traverse the Map to Find any columns are parent.
    for (Map.Entry<Integer, List<Integer>> entry : complexColumnMap.entrySet()) {
      List<Integer> childOrdinals = entry.getValue();
      if (childOrdinals.size() > 1) {
        // In case of more that one child, have to check if the child columns are in the same path
        // and have a common parent.
        Collections.sort(childOrdinals);
        List<CarbonDimension> mergedDimensions = mergeChildColumns(childOrdinals);
        if (mergedDimensions.size() > 0) {
          projection = removeMergedDimensions(mergedDimensions, projectionColumns, factTableName);
        }
      }
    }
    return projection;
  }

  /**
   * Remove the dimensions from the projection list which are merged
   *
   * @param mergedDimensions
   * @param projectionColumns
   * @param factTableName
   * @return
   */
  private QueryProjection removeMergedDimensions(List<CarbonDimension> mergedDimensions,
      String[] projectionColumns, String factTableName) {
    QueryProjection queryProjection = new QueryProjection();
    int i = 0;
    for (String projectionColumnName : projectionColumns) {
      CarbonDimension dimension = table.getDimensionByName(factTableName, projectionColumnName);
      if (dimension != null) {
        if (!mergedDimensions.contains(dimension)) {
          if (!isAlreadyExists(dimension, queryProjection.getDimensions())) {
            queryProjection.addDimension(dimension, i);
            i++;
          }
        }
      } else {
        CarbonMeasure measure = table.getMeasureByName(factTableName, projectionColumnName);
        if (measure == null) {
          throw new RuntimeException(
              projectionColumnName + " column not found in the table " + factTableName);
        }
        queryProjection.addMeasure(measure, i);
        i++;
      }
    }
    return queryProjection;
  }

  private List<CarbonDimension> mergeChildColumns(List<Integer> childOrdinals) {
    // Check If children if they are in the path of not.
    List<CarbonDimension> mergedChild = new ArrayList<>();
    List<CarbonDimension> dimList = table.getDimensions();
    for (int i = 0; i < childOrdinals.size(); i++) {
      for (int j = i; j < childOrdinals.size(); j++) {
        CarbonDimension parentDimension = getDimensionBasedOnOrdinal(dimList, childOrdinals.get(i));
        CarbonDimension childDimension = getDimensionBasedOnOrdinal(dimList, childOrdinals.get(j));
        if (!mergedChild.contains(childOrdinals.get(j)) && checkChildsInSamePath(parentDimension,
            childDimension)) {
          mergedChild.add(childDimension);
        }
      }
    }
    return mergedChild;
  }

  private boolean checkChildsInSamePath(CarbonDimension parentDimension,
      CarbonDimension childDimension) {
    if (parentDimension.getColName().equals(childDimension.getColName())) {
      return false;
    } else if (checkForChildColumns(parentDimension, childDimension)) {
      return true;
    } else {
      return false;
    }
  }

  private boolean checkForChildColumns(CarbonDimension parentDimension,
      CarbonDimension childDimension) {
    boolean output = false;
    if (parentDimension.getOrdinal() == childDimension.getOrdinal()) {
      output = true;
    } else if (parentDimension.getNumberOfChild() > 0) {
      for (int i = 0; i < parentDimension.getNumberOfChild() && !output; i++) {
        output =
            checkForChildColumns(parentDimension.getListOfChildDimensions().get(i), childDimension);
      }
    } else {
      output = false;
    }
    return output;
  }

  private CarbonDimension getDimensionBasedOnOrdinal(List<CarbonDimension> dimList,
      Integer ordinal) {
    for (CarbonDimension dims : dimList) {
      if (dims.getOrdinal() == ordinal) {
        return dims;
      } else if (dims.getNumberOfChild() > 0) {
        CarbonDimension dimensionBasedOnOrdinal =
            getDimensionBasedOnOrdinal(dims.getListOfChildDimensions(), ordinal);
        if (null != dimensionBasedOnOrdinal) {
          return dimensionBasedOnOrdinal;
        }
      }
    }
    return null;
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

  public QueryModelBuilder convertToRangeFilter(boolean convertToRangeFilter) {
    this.convertToRangeFilter = convertToRangeFilter;
    return this;
  }

  public boolean isConvertToRangeFilter() {
    return this.convertToRangeFilter;
  }

  public void enableReadPageByPage() {
    this.readPageByPage = true;
  }

  public QueryModel build() {
    QueryModel queryModel = QueryModel.newInstance(table);
    queryModel.setConverter(dataTypeConverter);
    queryModel.setForcedDetailRawQuery(forcedDetailRawQuery);
    queryModel.setReadPageByPage(readPageByPage);
    queryModel.setProjection(projection);

    if (table.isTransactionalTable() && !table.hasColumnDrift()) {
      // set the filter to the query model in order to filter blocklet before scan
      boolean[] isFilterDimensions = new boolean[table.getDimensionOrdinalMax()];
      boolean[] isFilterMeasures = new boolean[table.getAllMeasures().size()];
      // In case of Dictionary Include Range Column we donot optimize the range expression
      if (isConvertToRangeFilter()) {
        table.processFilterExpression(filterExpression, isFilterDimensions, isFilterMeasures);
      } else {
        table.processFilterExpressionWithoutRange(filterExpression, isFilterDimensions,
            isFilterMeasures);
      }
      queryModel.setIsFilterDimensions(isFilterDimensions);
      queryModel.setIsFilterMeasures(isFilterMeasures);
      FilterResolverIntf filterIntf =
          CarbonTable.resolveFilter(filterExpression, table.getAbsoluteTableIdentifier());
      queryModel.setFilterExpressionResolverTree(filterIntf);
    } else {
      queryModel.setFilterExpression(filterExpression);
    }
    return queryModel;
  }
}
