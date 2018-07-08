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

package org.apache.carbondata.core.scan.filter.resolver.resolverinfo;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.filter.ColumnFilterInfo;
import org.apache.carbondata.core.scan.filter.resolver.metadata.FilterResolverMetadata;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.visitor.ResolvedFilterInfoVisitorIntf;

public class DimColumnResolvedFilterInfo extends ColumnResolvedFilterInfo implements Serializable {
  /**
   *
   */
  private static final long serialVersionUID = 3428115141211084114L;

  /**
   * rowIndex
   */
  private int rowIndex = -1;

  private boolean isDimensionExistsInCurrentSilce = true;

  private CarbonDimension dimension;

  /**
   * reolved filter object of a particlar filter Expression.
   */
  private ColumnFilterInfo resolvedFilterValueObj;

  private Map<CarbonDimension, List<ColumnFilterInfo>> dimensionResolvedFilter;

  public DimColumnResolvedFilterInfo() {
    dimensionResolvedFilter = new HashMap<CarbonDimension, List<ColumnFilterInfo>>(20);
  }

  public void addDimensionResolvedFilterInstance(CarbonDimension dimension,
      ColumnFilterInfo filterResolvedObj) {
    List<ColumnFilterInfo> currentVals = dimensionResolvedFilter.get(dimension);
    if (null == currentVals) {
      currentVals = new ArrayList<ColumnFilterInfo>(20);
      currentVals.add(filterResolvedObj);
      dimensionResolvedFilter.put(dimension, currentVals);
    } else {
      currentVals.add(filterResolvedObj);
    }
  }

  public Map<CarbonDimension, List<ColumnFilterInfo>> getDimensionResolvedFilterInstance() {
    return dimensionResolvedFilter;
  }

  public CarbonDimension getDimension() {
    return dimension;
  }

  public void setDimension(CarbonDimension dimension) {
    this.dimension = dimension;
  }

  public int getColumnIndex() {
    return columnIndex;
  }

  public void setColumnIndex(int columnIndex) {
    this.columnIndex = columnIndex;
  }

  public ColumnFilterInfo getFilterValues() {
    return resolvedFilterValueObj;
  }

  public void setFilterValues(final ColumnFilterInfo resolvedFilterValueObj) {
    this.resolvedFilterValueObj = resolvedFilterValueObj;
  }

  public int getRowIndex() {
    return rowIndex;
  }

  public void setRowIndex(int rowIndex) {
    this.rowIndex = rowIndex;
  }

  public boolean isDimensionExistsInCurrentSilce() {
    return isDimensionExistsInCurrentSilce;
  }

  public void setDimensionExistsInCurrentSilce(boolean isDimensionExistsInCurrentSilce) {
    this.isDimensionExistsInCurrentSilce = isDimensionExistsInCurrentSilce;
  }

  public void populateFilterInfoBasedOnColumnType(ResolvedFilterInfoVisitorIntf visitor,
      FilterResolverMetadata metadata) throws FilterUnsupportedException, IOException {
    if (null != visitor) {
      visitor.populateFilterResolvedInfo(this, metadata);
      this.addDimensionResolvedFilterInstance(metadata.getColumnExpression().getDimension(),
          this.getFilterValues());
      this.setDimension(metadata.getColumnExpression().getDimension());
      this.setColumnIndex(metadata.getColumnExpression().getDimension().getOrdinal());
    }

  }

  /**
   * This method will clone the current object
   *
   * @return
   */
  public DimColumnResolvedFilterInfo getCopyObject() {
    DimColumnResolvedFilterInfo dimColumnResolvedFilterInfo = new DimColumnResolvedFilterInfo();
    dimColumnResolvedFilterInfo.resolvedFilterValueObj = this.resolvedFilterValueObj;
    dimColumnResolvedFilterInfo.rowIndex = this.rowIndex;
    dimColumnResolvedFilterInfo.dimensionResolvedFilter = this.dimensionResolvedFilter;
    dimColumnResolvedFilterInfo.isDimensionExistsInCurrentSilce = isDimensionExistsInCurrentSilce;
    dimColumnResolvedFilterInfo.columnIndexInMinMaxByteArray = columnIndexInMinMaxByteArray;
    return dimColumnResolvedFilterInfo;
  }

  @Override public CarbonMeasure getMeasure() {
    throw new UnsupportedOperationException("Operation not supported");
  }
}
