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

import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.filter.ColumnFilterInfo;
import org.apache.carbondata.core.scan.filter.resolver.metadata.FilterResolverMetadata;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.visitor.ResolvedFilterInfoVisitorIntf;

public class MeasureColumnResolvedFilterInfo extends ColumnResolvedFilterInfo
    implements Serializable {
  /**
   *
   */
  private static final long serialVersionUID = 4222568289115151561L;

  private int columnIndex = -1;

  private int rowIndex = -1;

  private boolean isMeasureExistsInCurrentSilce = true;

  private CarbonColumn carbonColumn;

  private CarbonMeasure carbonMeasure;

  /**
   * reolved filter object of a particlar filter Expression.
   */
  private ColumnFilterInfo resolvedFilterValueObj;

  private Map<CarbonMeasure, List<ColumnFilterInfo>> measureResolvedFilter;

  private org.apache.carbondata.core.metadata.datatype.DataType type;

  public int getColumnIndex() {
    return columnIndex;
  }

  public MeasureColumnResolvedFilterInfo() {
    measureResolvedFilter = new HashMap<CarbonMeasure, List<ColumnFilterInfo>>(20);
  }

  public void addMeasureResolvedFilterInstance(CarbonMeasure measures,
      ColumnFilterInfo filterResolvedObj) {
    List<ColumnFilterInfo> currentVals = measureResolvedFilter.get(measures);
    if (null == currentVals) {
      currentVals = new ArrayList<ColumnFilterInfo>(20);
      currentVals.add(filterResolvedObj);
      measureResolvedFilter.put(measures, currentVals);
    } else {
      currentVals.add(filterResolvedObj);
    }
  }

  public ColumnFilterInfo getFilterValues() {
    return resolvedFilterValueObj;
  }

  public void setFilterValues(final ColumnFilterInfo resolvedFilterValueObj) {
    this.resolvedFilterValueObj = resolvedFilterValueObj;
  }

  public void setColumnIndex(int columnIndex) {
    this.columnIndex = columnIndex;
  }

  public int getRowIndex() {
    return rowIndex;
  }

  public void setRowIndex(int rowIndex) {
    this.rowIndex = rowIndex;
  }

  public org.apache.carbondata.core.metadata.datatype.DataType getType() {
    return type;
  }

  public void setType(org.apache.carbondata.core.metadata.datatype.DataType dataType) {
    this.type = dataType;
  }

  public CarbonColumn getCarbonColumn() {
    return carbonColumn;
  }

  public void setCarbonColumn(CarbonColumn carbonColumn) {
    this.carbonColumn = carbonColumn;
  }

  public CarbonMeasure getMeasure() {
    return carbonMeasure;
  }

  public void setMeasureExistsInCurrentSilce(boolean measureExistsInCurrentSilce) {
    isMeasureExistsInCurrentSilce = measureExistsInCurrentSilce;
  }

  public void setMeasure(CarbonMeasure carbonMeasure) {
    this.carbonMeasure = carbonMeasure;
  }

  public void populateFilterInfoBasedOnColumnType(ResolvedFilterInfoVisitorIntf visitor,
      FilterResolverMetadata metadata) throws FilterUnsupportedException, IOException {
    if (null != visitor) {
      visitor.populateFilterResolvedInfo(this, metadata);
      this.addMeasureResolvedFilterInstance(metadata.getColumnExpression().getMeasure(),
          this.getFilterValues());
      this.setMeasure(metadata.getColumnExpression().getMeasure());
      this.setColumnIndex(metadata.getColumnExpression().getMeasure().getOrdinal());
    }
  }

  /**
   * This method will clone the current object
   *
   * @return
   */
  public MeasureColumnResolvedFilterInfo getCopyObject() {
    MeasureColumnResolvedFilterInfo msrColumnResolvedFilterInfo =
        new MeasureColumnResolvedFilterInfo();
    msrColumnResolvedFilterInfo.resolvedFilterValueObj = this.resolvedFilterValueObj;
    msrColumnResolvedFilterInfo.rowIndex = this.rowIndex;
    msrColumnResolvedFilterInfo.measureResolvedFilter = this.measureResolvedFilter;
    msrColumnResolvedFilterInfo.setMeasureExistsInCurrentSilce(this.isMeasureExistsInCurrentSilce);
    return msrColumnResolvedFilterInfo;
  }


}
