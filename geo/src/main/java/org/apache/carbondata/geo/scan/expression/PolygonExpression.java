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

package org.apache.carbondata.geo.scan.expression;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.ExpressionResult;
import org.apache.carbondata.core.scan.expression.LiteralExpression;
import org.apache.carbondata.core.scan.expression.conditional.InExpression;
import org.apache.carbondata.core.scan.expression.conditional.ListExpression;
import org.apache.carbondata.core.scan.filter.intf.ExpressionType;
import org.apache.carbondata.core.scan.filter.intf.RowIntf;
import org.apache.carbondata.core.util.CustomIndex;

/**
 * InPolygon expression processor. It inputs the InPolygon string to the Geo implementation's
 * query method, gets the list of ranges of IDs to filter as an output. And then, build
 * InExpression with list of all the IDs present in those list of ranges.
 */
@InterfaceAudience.Internal
public class PolygonExpression extends Expression {
  private String polygon;
  private String columnName;
  private CustomIndex<List<Long[]>> handler;
  private List<Expression> children = new ArrayList<Expression>();

  public PolygonExpression(String polygon, String columnName, CustomIndex handler) {
    this.polygon = polygon;
    this.handler = handler;
    this.columnName = columnName;
  }

  private void buildExpression(List<Long[]> ranges) {
    // Build InExpression with list of all the values present in the ranges
    List<Expression> inList = new ArrayList<Expression>();
    for (Long[] range : ranges) {
      if (range.length != 2) {
        throw new RuntimeException("Handler query must return list of ranges with each range "
            + "containing minimum and maximum values");
      }
      for (long i = range[0]; i <= range[1]; i++) {
        inList.add(new LiteralExpression(i, DataTypes.LONG));
      }
    }
    children.add(new InExpression(new ColumnExpression(columnName, DataTypes.LONG),
        new ListExpression(inList)));
  }

  /**
   * This method builds InExpression with list of all the values present in the list of ranges of
   * IDs.
   */
  private void processExpression() {
    List<Long[]> ranges;
    try {
      ranges = handler.query(polygon);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    buildExpression(ranges);
  }

  @Override
  public ExpressionResult evaluate(RowIntf value) {
    throw new UnsupportedOperationException("Operation not supported for Polygon expression");
  }

  @Override
  public ExpressionType getFilterExpressionType() {
    return ExpressionType.POLYGON;
  }

  @Override
  public List<Expression> getChildren() {
    if (children.isEmpty()) {
      processExpression();
    }
    return children;
  }

  @Override
  public void findAndSetChild(Expression oldExpr, Expression newExpr) {
  }

  @Override
  public String getString() {
    return polygon;
  }

  @Override
  public String getStatement() {
    return "IN_POLYGON('" + polygon + "')";
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    out.writeObject(polygon);
    out.writeObject(columnName);
    out.writeObject(handler);
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    polygon = (String) in.readObject();
    columnName = (String) in.readObject();
    handler = (CustomIndex<List<Long[]>>) in.readObject();
    children = new ArrayList<Expression>();
  }
}
