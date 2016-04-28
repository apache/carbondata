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
import org.carbondata.query.carbon.executor.exception.QueryExecutionException;
import org.carbondata.query.carbonfilterinterface.FilterExecuterType;
import org.carbondata.query.evaluators.DimColumnResolvedFilterInfo;
import org.carbondata.query.expression.ColumnExpression;
import org.carbondata.query.expression.DataType;
import org.carbondata.query.expression.Expression;
import org.carbondata.query.expression.conditional.BinaryConditionalExpression;
import org.carbondata.query.expression.conditional.ConditionalExpression;
import org.carbondata.query.filters.measurefilter.util.FilterUtil;
import org.carbondata.query.schema.metadata.DimColumnFilterInfo;

public class ConditionalFilterResolverImpl implements FilterResolverIntf {

  private static final long serialVersionUID = 1838955268462201691L;
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

  /**
   * This API will resolve the filter expression and generates the
   * dictionaries for executing/evaluating the filter expressions in the
   * executer layer.
   *
   * @throws QueryExecutionException
   */
  @Override public void resolve(AbsoluteTableIdentifier absoluteTableIdentifier)
      throws QueryExecutionException {

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

            DimColumnFilterInfo filterInfo = FilterUtil
                .getFilterList(absoluteTableIdentifier, rightExp, columnExpression,
                    isIncludeFilter);
            dimColResolvedFilterInfo.setFilterValues(filterInfo);
            dimColResolvedFilterInfo
                .addDimensionResolvedFilterInstance(columnExpression.getDimension(), filterInfo);
            dimColResolvedFilterInfo.setDimension(columnExpression.getDimension());
            dimColResolvedFilterInfo.setColumnIndex(columnExpression.getDimension().getOrdinal());
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
            DimColumnFilterInfo filterInfo = FilterUtil
                .getFilterList(absoluteTableIdentifier, leftExp, columnExpression, isIncludeFilter);
            dimColResolvedFilterInfo.setFilterValues(filterInfo);
            dimColResolvedFilterInfo.setDimension(columnExpression.getDimension());
            dimColResolvedFilterInfo
                .addDimensionResolvedFilterInstance(columnExpression.getDimension(), filterInfo);

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
        try {
          dimColResolvedFilterInfo.setFilterValues(FilterUtil
              .getFilterList(absoluteTableIdentifier, exp, columnList.get(0), isIncludeFilter));
        } catch (QueryExecutionException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      } else if (!(columnList.get(0).getDimension().getDataType()
          == org.carbondata.core.carbon.metadata.datatype.DataType.STRUCT
          || columnList.get(0).getDimension().getDataType()
          == org.carbondata.core.carbon.metadata.datatype.DataType.STRUCT)) {
        try {
          dimColResolvedFilterInfo.setFilterValues(FilterUtil
              .getFilterListForAllValues(absoluteTableIdentifier, exp, columnList.get(0),
                  isIncludeFilter));
        } catch (QueryExecutionException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    }

  }

  /**
   * Left node will not be presentin this scenario
   *
   * @return left node of type FilterResolverIntf instance
   */
  public FilterResolverIntf getLeft() {
    // TODO Auto-generated method stub
    return null;
  }

  /**
   * Right node will not be presentin this scenario
   *
   * @return left node of type FilterResolverIntf instance
   */
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
    return dimColResolvedFilterInfo;
  }

  /**
   * method will get the start key based on the filter surrogates
   *
   * @return start IndexKey
   */
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

  /**
   * method will get the start key based on the filter surrogates
   *
   * @return end IndexKey
   */
  @Override public IndexKey getEndKey(AbstractIndex tableSegment,
      AbsoluteTableIdentifier absoluteTableIdentifier) {
    long[] endKey =
        new long[tableSegment.getSegmentProperties().getDimensionKeyGenerator().getDimCount()];
    IndexKey endIndexKey = null;
    if (null == dimColResolvedFilterInfo.getEndIndexKey())

    {
      try {
        endKey = FilterUtil.getEndKey(dimColResolvedFilterInfo.getDimensionResolvedFilterInstance(),
            absoluteTableIdentifier, endKey, tableSegment.getSegmentProperties().getDimensions());
      } catch (QueryExecutionException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      endIndexKey = FilterUtil.createIndexKeyFromResolvedFilterVal(endKey,
          tableSegment.getSegmentProperties().getDimensionKeyGenerator());
    } else {
      return dimColResolvedFilterInfo.getEndIndexKey();
    }
    return endIndexKey;
  }

  /**
   * Method will return the executer type for particular conditional resolver
   * basically two types of executers will be formed for the conditional query.
   *
   * @return the filter executer type
   */
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
