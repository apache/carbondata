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

package org.apache.carbondata.core.scan.expression;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.scan.filter.intf.ExpressionType;
import org.apache.carbondata.core.scan.filter.intf.RowIntf;

public class ColumnExpression extends LeafExpression {

  private static final long serialVersionUID = 1L;

  private String columnName;

  private boolean isDimension;

  private boolean isMeasure;

  private int colIndex = -1;

  private DataType dataType;

  private CarbonDimension dimension;

  private CarbonMeasure measure;

  private CarbonColumn carbonColumn;

  public ColumnExpression(String columnName, DataType dataType) {
    this.columnName = columnName;
    this.dataType = dataType;

  }

  public CarbonDimension getDimension() {
    return dimension;
  }

  public void setDimension(CarbonDimension dimension) {
    this.dimension = dimension;
  }

  public CarbonMeasure getMeasure() {
    return measure;
  }

  public void setMeasure(CarbonMeasure measure) {
    this.measure = measure;
  }

  public String getColumnName() {
    return columnName;
  }

  public boolean isDimension() {
    return isDimension;
  }

  public void setDimension(boolean isDimension) {
    this.isDimension = isDimension;
  }

  public boolean isMeasure() {
    return isMeasure;
  }

  public void setMeasure(boolean isMeasure) {
    this.isMeasure = isMeasure;
  }

  public int getColIndex() {
    return colIndex;
  }

  public void setColIndex(int colIndex) {
    this.colIndex = colIndex;
  }

  public DataType getDataType() {
    return dataType;
  }

  @Override public ExpressionResult evaluate(RowIntf value) {
    return new ExpressionResult(dataType, (null == value ? null : value.getVal(colIndex)));
  }

  @Override public ExpressionType getFilterExpressionType() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override public String getString() {
    // TODO Auto-generated method stub
    return "ColumnExpression(" + columnName + ')';
  }

  public CarbonColumn getCarbonColumn() {
    return carbonColumn;
  }

  public void setCarbonColumn(CarbonColumn carbonColumn) {
    this.carbonColumn = carbonColumn;
  }

  @Override public void findAndSetChild(Expression oldExpr, Expression newExpr) {
  }


}
