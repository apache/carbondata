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
import java.util.Arrays;
import java.util.List;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.ExpressionResult;
import org.apache.carbondata.core.scan.expression.UnknownExpression;
import org.apache.carbondata.core.scan.expression.conditional.ConditionalExpression;
import org.apache.carbondata.core.scan.filter.executer.FilterExecutor;
import org.apache.carbondata.core.scan.filter.intf.ExpressionType;
import org.apache.carbondata.core.scan.filter.intf.RowIntf;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.scan.filter.resolver.RowLevelFilterResolverImpl;
import org.apache.carbondata.core.util.CustomIndex;
import org.apache.carbondata.geo.scan.filter.executor.PolygonFilterExecutorImpl;

/**
 * InPolygon expression processor. It inputs the InPolygon string to the Geo implementation's
 * query method, gets the list of ranges of IDs to filter as an output. And then, build
 * InExpression with list of all the IDs present in those list of ranges.
 */
@InterfaceAudience.Internal
public class PolygonExpression extends UnknownExpression implements ConditionalExpression {
  private String polygon;
  private CustomIndex<List<Long[]>> instance;
  private List<Long[]> ranges = new ArrayList<Long[]>();
  private ColumnExpression column;
  private static final ExpressionResult trueExpRes =
      new ExpressionResult(DataTypes.BOOLEAN, true);
  private static final ExpressionResult falseExpRes =
      new ExpressionResult(DataTypes.BOOLEAN, false);


  public PolygonExpression(String polygon, String columnName, CustomIndex indexInstance) {
    this.polygon = polygon;
    this.instance = indexInstance;
    this.column = new ColumnExpression(columnName, DataTypes.LONG);
  }

  private void validate(List<Long[]> ranges) {
    // Validate the ranges
    for (Long[] range : ranges) {
      if (range.length != 2) {
        throw new RuntimeException("Query processor must return list of ranges with each range "
            + "containing minimum and maximum values");
      }
    }
  }

  /**
   * This method calls the query processor and gets the list of ranges of IDs.
   */
  private void processExpression() {
    try {
      ranges = instance.query(polygon);
      validate(ranges);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public List<Long[]> getRanges() {
    return ranges;
  }

  private boolean rangeBinarySearch(List<Long[]> ranges, long searchForNumber) {
    Long[] range;
    int low = 0, mid, high = ranges.size() - 1;
    while (low <= high) {
      mid = low + ((high - low) / 2);
      range = ranges.get(mid);
      if (searchForNumber >= range[0]) {
        if (searchForNumber <= range[1]) {
          // Return true if the number is between min and max values of the range
          return true;
        } else {
          // Number is bigger than this range's min and max. Search on the right side of the range
          low = mid + 1;
        }
      } else {
        // Number is smaller than this range's min and max. Search on the left side of the range
        high = mid - 1;
      }
    }
    return false;
  }

  @Override
  public ExpressionResult evaluate(RowIntf value) {
    if (rangeBinarySearch(ranges, (Long) value.getVal(0))) {
      return trueExpRes;
    }
    return falseExpRes;
  }

  @Override
  public ExpressionType getFilterExpressionType() {
    return ExpressionType.UNKNOWN;
  }

  @Override
  public List<Expression> getChildren() {
    if (ranges.isEmpty()) {
      processExpression();
    }
    return super.getChildren();
  }

  @Override
  public void findAndSetChild(Expression oldExpr, Expression newExpr) {
  }

  @Override
  public String getString() {
    return getStatement();
  }

  @Override
  public String getStatement() {
    return "IN_POLYGON('" + polygon + "')";
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    out.writeObject(polygon);
    out.writeObject(instance);
    out.writeObject(column);
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    polygon = (String) in.readObject();
    instance = (CustomIndex<List<Long[]>>) in.readObject();
    column = (ColumnExpression) in.readObject();
    ranges = new ArrayList<Long[]>();
  }

  @Override
  public List<ColumnExpression> getAllColumnList() {
    return new ArrayList<ColumnExpression>(Arrays.asList(column));
  }

  @Override
  public List<ColumnExpression> getColumnList() {
    return getAllColumnList();
  }

  @Override
  public boolean isSingleColumn() {
    return true;
  }

  @Override
  public List<ExpressionResult> getLiterals() {
    return null;
  }

  @Override
  public FilterExecutor getFilterExecutor(FilterResolverIntf resolver,
      SegmentProperties segmentProperties) {
    assert (resolver instanceof RowLevelFilterResolverImpl);
    RowLevelFilterResolverImpl rowLevelResolver = (RowLevelFilterResolverImpl) resolver;
    return new PolygonFilterExecutorImpl(rowLevelResolver.getDimColEvaluatorInfoList(),
        rowLevelResolver.getMsrColEvalutorInfoList(), rowLevelResolver.getFilterExpresion(),
        rowLevelResolver.getTableIdentifier(), segmentProperties, null);
  }
}
