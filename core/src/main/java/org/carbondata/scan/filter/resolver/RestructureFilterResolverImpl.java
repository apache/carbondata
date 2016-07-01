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

package org.carbondata.scan.filter.resolver;

import java.util.List;
import java.util.SortedMap;

import org.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.carbondata.core.carbon.datastore.block.SegmentProperties;
import org.carbondata.scan.expression.ColumnExpression;
import org.carbondata.scan.expression.DataType;
import org.carbondata.scan.expression.Expression;
import org.carbondata.scan.expression.conditional.BinaryConditionalExpression;
import org.carbondata.scan.expression.conditional.ConditionalExpression;
import org.carbondata.scan.expression.exception.FilterUnsupportedException;
import org.carbondata.scan.filter.FilterUtil;
import org.carbondata.scan.filter.intf.FilterExecuterType;
import org.carbondata.scan.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;

public class RestructureFilterResolverImpl implements FilterResolverIntf {
  /**
   *
   */
  private static final long serialVersionUID = -5399656036192814524L;

  protected DimColumnResolvedFilterInfo dimColumnResolvedFilterInfo;

  private Expression exp;

  private String defaultValue;

  private int surrogate;

  private boolean isExpressionResolve;

  private boolean isIncludeFilter;

  public RestructureFilterResolverImpl(Expression exp, String defaultValue, int surrogate,
      boolean isExpressionResolve, boolean isIncludeFilter) {
    dimColumnResolvedFilterInfo = new DimColumnResolvedFilterInfo();
    this.exp = exp;
    this.defaultValue = defaultValue;
    this.surrogate = surrogate;
    this.isExpressionResolve = isExpressionResolve;
    this.isIncludeFilter = isIncludeFilter;
  }

  /**
   * Method will resolve the filters and it will replace the newly added dimension with default
   * value
   *
   * @param absoluteTableIdentifier
   * @throws FilterUnsupportedException
   */
  @Override public void resolve(AbsoluteTableIdentifier absoluteTableIdentifier)
      throws FilterUnsupportedException {

    DimColumnResolvedFilterInfo dimColumnResolvedFilterInfo = new DimColumnResolvedFilterInfo();
    if (!this.isExpressionResolve && exp instanceof BinaryConditionalExpression) {
      BinaryConditionalExpression binaryConditionalExpression = (BinaryConditionalExpression) exp;
      Expression left = binaryConditionalExpression.getLeft();
      Expression right = binaryConditionalExpression.getRight();
      if (left instanceof ColumnExpression) {
        ColumnExpression columnExpression = (ColumnExpression) left;
        if (columnExpression.getDataType().equals(DataType.TimestampType)) {
          isExpressionResolve = true;
        } else {
          // If imei=imei comes in filter condition then we need to
          // skip processing of right expression.
          // This flow has reached here assuming that this is a single
          // column expression.
          // we need to check if the other expression contains column
          // expression or not in depth.
          if (FilterUtil.checkIfExpressionContainsColumn(right)) {
            isExpressionResolve = true;
          } else {
            dimColumnResolvedFilterInfo
                .setColumnIndex(columnExpression.getCarbonColumn().getOrdinal());
            dimColumnResolvedFilterInfo.setFilterValues(
                FilterUtil.getFilterListForRS(right, columnExpression, defaultValue, surrogate));
          }
        }
      } else if (right instanceof ColumnExpression) {
        ColumnExpression columnExpression = (ColumnExpression) right;
        if (columnExpression.getDataType().equals(DataType.TimestampType)) {
          isExpressionResolve = true;
        } else {

          // If imei=imei comes in filter condition then we need to
          // skip processing of right expression.
          // This flow has reached here assuming that this is a single
          // column expression.
          // we need to check if the other expression contains column
          // expression or not in depth.
          if (checkIfExpressionContainsColumn(left)) {
            isExpressionResolve = true;
          } else {
            dimColumnResolvedFilterInfo
                .setColumnIndex(columnExpression.getCarbonColumn().getOrdinal());
            dimColumnResolvedFilterInfo.setFilterValues(
                FilterUtil.getFilterListForRS(left, columnExpression, defaultValue, surrogate));
          }
        }
      }
    }
    if (this.isExpressionResolve && exp instanceof ConditionalExpression) {
      ConditionalExpression conditionalExpression = (ConditionalExpression) exp;
      List<ColumnExpression> columnList = conditionalExpression.getColumnList();
      dimColumnResolvedFilterInfo.setColumnIndex(columnList.get(0).getDimension().getOrdinal());
      dimColumnResolvedFilterInfo.setFilterValues(FilterUtil
          .getFilterListForAllMembersRS(exp, columnList.get(0), defaultValue, surrogate,
              isIncludeFilter));
    }

  }

  /**
   * This method will check if a given expression contains a column expression recursively.
   *
   * @return boolean
   */
  private boolean checkIfExpressionContainsColumn(Expression expression) {
    if (expression instanceof ColumnExpression) {
      return true;
    }
    for (Expression child : expression.getChildren()) {
      if (checkIfExpressionContainsColumn(child)) {
        return true;
      }
    }

    return false;
  }

  @Override public FilterResolverIntf getLeft() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override public FilterResolverIntf getRight() {
    // TODO Auto-generated method stub
    return null;
  }

  /**
   * Method will return the DimColumnResolvedFilterInfo instance which consists
   * the mapping of the respective dimension and its surrogates involved in
   * filter expression.
   *
   * @return DimColumnResolvedFilterInfo
   */
  public DimColumnResolvedFilterInfo getDimColResolvedFilterInfo() {
    return dimColumnResolvedFilterInfo;
  }

  /**
   * For restructure resolver no implementation is required for getting
   * the start key since it already has default values
   */
  @Override public void getStartKey(SegmentProperties segmentProperties, long[] startKeys,
      SortedMap<Integer, byte[]> noDicStartKeys) {

  }

  /**
   * For restructure resolver no implementation is required for getting
   * the end  key since it already has default values
   *
   * @return IndexKey.
   */
  @Override public void getEndKey(SegmentProperties segmentProperties,
      AbsoluteTableIdentifier tableIdentifier, long[] endKeys,
      SortedMap<Integer, byte[]> noDicEndKeys) {
  }

  /**
   * Method will get the executer type inorder to create filter executer tree
   *
   * @return FilterExecuterType
   */
  @Override public FilterExecuterType getFilterExecuterType() {
    return FilterExecuterType.RESTRUCTURE;
  }

  @Override public Expression getFilterExpression() {
    // TODO Auto-generated method stub
    return exp;
  }
}
