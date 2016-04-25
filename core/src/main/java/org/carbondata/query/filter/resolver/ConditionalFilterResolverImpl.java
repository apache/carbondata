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
package org.carbondata.query.filter.resolver;

import java.util.List;

import org.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.carbondata.core.carbon.datastore.IndexKey;
import org.carbondata.core.carbon.datastore.block.AbstractIndex;
import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.query.carbonfilterinterface.FilterExecuterType;
import org.carbondata.query.evaluators.DimColumnResolvedFilterInfo;
import org.carbondata.query.expression.ColumnExpression;
import org.carbondata.query.expression.DataType;
import org.carbondata.query.expression.Expression;
import org.carbondata.query.expression.conditional.BinaryConditionalExpression;
import org.carbondata.query.expression.conditional.ConditionalExpression;
import org.carbondata.query.filters.measurefilter.util.FilterUtil;

public class ConditionalFilterResolverImpl implements FilterResolverIntf {

  protected Expression exp;
  protected boolean isExpressionResolve;
  protected boolean isIncludeFilter;
  private DimColumnResolvedFilterInfo dimColResolvedFilterInfo;

  public ConditionalFilterResolverImpl(Expression exp, boolean isExpressionResolve,
      boolean isIncludeFilter) {
    this.exp = exp;
    this.isExpressionResolve = isExpressionResolve;
    this.isIncludeFilter = isIncludeFilter;
    this.dimColResolvedFilterInfo = new DimColumnResolvedFilterInfo();
  }

  @Override public void resolve(AbsoluteTableIdentifier absoluteTableIdentifier) {

    if ((!isExpressionResolve) && exp instanceof BinaryConditionalExpression) {
      BinaryConditionalExpression binaryConditionalExpression = (BinaryConditionalExpression) exp;
      Expression leftExp = binaryConditionalExpression.getLeft();
      Expression rightExp = binaryConditionalExpression.getRight();

      if (leftExp instanceof ColumnExpression) {
        ColumnExpression columnExpression = (ColumnExpression) leftExp;
        if (columnExpression.getDataType().equals(DataType.TimestampType)) {
          isExpressionResolve = true;
        } else {
          // If imei=imei comes in filter condition then we need to
          // skip processing of right expression.
          // This flow has reached here assuming that this is a single
          // column expression.
          // we need to check if the other expression contains column
          // expression or not in depth.
          if (FilterUtil.checkIfExpressionContainsColumn(rightExp)) {
            isExpressionResolve = true;
          } else {

            dimColResolvedFilterInfo.setFilterValues(FilterUtil
                .getFilterList(absoluteTableIdentifier, rightExp, columnExpression,
                    this.isIncludeFilter));
            dimColResolvedFilterInfo.setDimension(columnExpression.getDimension());
          }
        }
      } else if (rightExp instanceof ColumnExpression) {
        ColumnExpression columnExpression = (ColumnExpression) rightExp;
        if (columnExpression.getDataType().equals(DataType.TimestampType)) {
          isExpressionResolve = true;
        } else {
          // if imei=imei comes in filter condition then we need to
          // skip processing of right expression.
          // This flow has reached here assuming that this is a single
          // column expression.
          // we need to check if the other expression contains column
          // expression or not in depth.
          if (FilterUtil.checkIfExpressionContainsColumn(leftExp)) {
            isExpressionResolve = true;
          } else {
            dimColResolvedFilterInfo.setColumnIndex(columnExpression.getDimension().getOrdinal());
            dimColResolvedFilterInfo
                .addDimensionResolvedFilterInstance(columnExpression.getDimension(), FilterUtil
                    .getFilterList(absoluteTableIdentifier, leftExp, columnExpression,
                        isIncludeFilter));
          }
        }
      } else {
        isExpressionResolve = true;
      }
    }
    if (isExpressionResolve && exp instanceof ConditionalExpression) {
      ConditionalExpression conditionalExpression = (ConditionalExpression) exp;
      List<ColumnExpression> columnList = conditionalExpression.getColumnList();
      dimColResolvedFilterInfo.setColumnIndex(columnList.get(0).getDimension().getOrdinal());
      if (columnList.get(0).getDimension().getEncoder().isEmpty()) {
        dimColResolvedFilterInfo.setFilterValues(FilterUtil
            .getFilterList(absoluteTableIdentifier, exp, columnList.get(0), isIncludeFilter));
      } else if (!(columnList.get(0).getDimension().getDataType()
          == org.carbondata.core.carbon.metadata.datatype.DataType.STRUCT
          || columnList.get(0).getDimension().getDataType()
          == org.carbondata.core.carbon.metadata.datatype.DataType.STRUCT)) {
        dimColResolvedFilterInfo.setFilterValues(FilterUtil
            .getFilterListForAllValues(absoluteTableIdentifier, exp, columnList.get(0),
                isIncludeFilter));
      }
    }

  }

  public FilterResolverIntf getLeft() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override public FilterResolverIntf getRight() {
    // TODO Auto-generated method stub
    return null;
  }

  public DimColumnResolvedFilterInfo getDimColResolvedFilterInfo() {
    return dimColResolvedFilterInfo;
  }

  @Override public IndexKey getstartKey(KeyGenerator keyGen) {
    IndexKey startIndexKey = null;
    if (null == dimColResolvedFilterInfo.getStarIndexKey()) {
      long[] startKey = FilterUtil.getStartKey(dimColResolvedFilterInfo, keyGen);
      startIndexKey = FilterUtil.createIndexKeyFromResolvedFilterVal(startKey, keyGen);
      dimColResolvedFilterInfo.setStarIndexKey(startIndexKey);
    } else {
      return dimColResolvedFilterInfo.getStarIndexKey();
    }
    return startIndexKey;
  }

  @Override public IndexKey getEndKey(AbstractIndex tableSegment,
      AbsoluteTableIdentifier absoluteTableIdentifier) {
    long[] endKey =
        new long[tableSegment.getSegmentProperties().getDimensionKeyGenerator().getDimCount()];
    IndexKey endIndexKey = null;
    if (null == dimColResolvedFilterInfo.getEndIndexKey())

    {
      endKey = FilterUtil.getEndKey(dimColResolvedFilterInfo.getDimensionResolvedFilterInstance(),
          absoluteTableIdentifier, endKey, tableSegment.getSegmentProperties().getDimensions());
      endIndexKey = FilterUtil.createIndexKeyFromResolvedFilterVal(endKey,
          tableSegment.getSegmentProperties().getDimensionKeyGenerator());
    } else {
      return dimColResolvedFilterInfo.getEndIndexKey();
    }
    return endIndexKey;
  }

  @Override public FilterExecuterType getFilterExecuterType() {
    switch (exp.getFilterExpressionType()) {
      case NOT_EQUALS:
      case NOT_IN:
        return FilterExecuterType.EXCLUDE;

      default:
        return FilterExecuterType.INCLUDE;
    }

  }


}
