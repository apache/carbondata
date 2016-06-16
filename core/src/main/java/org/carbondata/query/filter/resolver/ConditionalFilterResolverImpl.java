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
import java.util.SortedMap;

import org.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.carbondata.core.carbon.datastore.block.SegmentProperties;
import org.carbondata.core.carbon.metadata.encoder.Encoding;
import org.carbondata.query.carbon.executor.exception.QueryExecutionException;
import org.carbondata.query.carbonfilterinterface.FilterExecuterType;
import org.carbondata.query.expression.ColumnExpression;
import org.carbondata.query.expression.DataType;
import org.carbondata.query.expression.Expression;
import org.carbondata.query.expression.conditional.BinaryConditionalExpression;
import org.carbondata.query.expression.conditional.ConditionalExpression;
import org.carbondata.query.expression.exception.FilterUnsupportedException;
import org.carbondata.query.filter.resolver.metadata.FilterResolverMetadata;
import org.carbondata.query.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;
import org.carbondata.query.filter.resolver.resolverinfo.visitor.FilterInfoTypeVisitorFactory;
import org.carbondata.query.filters.measurefilter.util.FilterUtil;

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
   * @throws FilterUnsupportedException
   */
  @Override public void resolve(AbsoluteTableIdentifier absoluteTableIdentifier)
      throws FilterUnsupportedException {
    FilterResolverMetadata metadata = new FilterResolverMetadata();
    metadata.setTableIdentifier(absoluteTableIdentifier);
    if ((!isExpressionResolve) && exp instanceof BinaryConditionalExpression) {
      BinaryConditionalExpression binaryConditionalExpression = (BinaryConditionalExpression) exp;
      Expression leftExp = binaryConditionalExpression.getLeft();
      Expression rightExp = binaryConditionalExpression.getRight();
      if (leftExp instanceof ColumnExpression) {
        ColumnExpression columnExpression = (ColumnExpression) leftExp;
        metadata.setColumnExpression(columnExpression);
        metadata.setExpression(rightExp);
        metadata.setIncludeFilter(isIncludeFilter);
        // If imei=imei comes in filter condition then we need to
        // skip processing of right expression.
        // This flow has reached here assuming that this is a single
        // column expression.
        // we need to check if the other expression contains column
        // expression or not in depth.
        if (FilterUtil.checkIfExpressionContainsColumn(rightExp)||
            FilterUtil.isExpressionNeedsToResolved(rightExp,isIncludeFilter) &&
            columnExpression.getDimension().hasEncoding(Encoding.DICTIONARY)){
          isExpressionResolve = true;
        } else {
          //Visitor pattern is been used in this scenario inorder to populate the
          // dimColResolvedFilterInfo
          //visitable object with filter member values based on the visitor type, currently there
          //3 types of visitors custom,direct and no dictionary, all types of visitor populate
          //the visitable instance as per its buisness logic which is different for all the
          // visitors.
          dimColResolvedFilterInfo.populateFilterInfoBasedOnColumnType(
              FilterInfoTypeVisitorFactory.getResolvedFilterInfoVisitor(columnExpression),
              metadata);
        }
      } else if (rightExp instanceof ColumnExpression) {
        ColumnExpression columnExpression = (ColumnExpression) rightExp;
        metadata.setColumnExpression(columnExpression);
        metadata.setExpression(leftExp);
        metadata.setIncludeFilter(isIncludeFilter);
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

            dimColResolvedFilterInfo.populateFilterInfoBasedOnColumnType(
                FilterInfoTypeVisitorFactory.getResolvedFilterInfoVisitor(columnExpression),
                metadata);

          }
        }
      } else {
        isExpressionResolve = true;
      }
    }
    if (isExpressionResolve && exp instanceof ConditionalExpression) {
      ConditionalExpression conditionalExpression = (ConditionalExpression) exp;
      List<ColumnExpression> columnList = conditionalExpression.getColumnList();
      metadata.setColumnExpression(columnList.get(0));
      metadata.setExpression(exp);
      metadata.setIncludeFilter(isIncludeFilter);
      if (!columnList.get(0).getDimension().hasEncoding(Encoding.DICTIONARY) || columnList.get(0)
          .getDimension().hasEncoding(Encoding.DIRECT_DICTIONARY)) {
        dimColResolvedFilterInfo.populateFilterInfoBasedOnColumnType(
            FilterInfoTypeVisitorFactory.getResolvedFilterInfoVisitor(columnList.get(0)), metadata);

      } else if (columnList.get(0).getDimension().hasEncoding(Encoding.DICTIONARY) && !(
          columnList.get(0).getDimension().getDataType()
              == org.carbondata.core.carbon.metadata.datatype.DataType.STRUCT
              || columnList.get(0).getDimension().getDataType()
              == org.carbondata.core.carbon.metadata.datatype.DataType.ARRAY)) {
        dimColResolvedFilterInfo.setFilterValues(FilterUtil
            .getFilterListForAllValues(absoluteTableIdentifier, exp, columnList.get(0),
                isIncludeFilter));

        dimColResolvedFilterInfo.setColumnIndex(columnList.get(0).getDimension().getOrdinal());
        dimColResolvedFilterInfo.setDimension(columnList.get(0).getDimension());
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
   * method will calculates the start key based on the filter surrogates
   */
  public void getStartKey(SegmentProperties segmentProperties, long[] startKey,
      SortedMap<Integer, byte[]> setOfStartKeyByteArray) {
    if (null == dimColResolvedFilterInfo.getStarIndexKey()) {
      FilterUtil.getStartKeyForNoDictionaryDimension(dimColResolvedFilterInfo, segmentProperties,
          setOfStartKeyByteArray);
    }
  }

  /**
   * method will get the start key based on the filter surrogates
   *
   * @return end IndexKey
   */
  @Override public void getEndKey(SegmentProperties segmentProperties,
      AbsoluteTableIdentifier absoluteTableIdentifier, long[] endKeys,
      SortedMap<Integer, byte[]> setOfEndKeyByteArray) {
    if (null == dimColResolvedFilterInfo.getEndIndexKey()) {
      try {
        FilterUtil.getEndKey(dimColResolvedFilterInfo.getDimensionResolvedFilterInstance(),
            absoluteTableIdentifier, endKeys, segmentProperties);
        FilterUtil.getEndKeyForNoDictionaryDimension(dimColResolvedFilterInfo, segmentProperties,
            setOfEndKeyByteArray);
      } catch (QueryExecutionException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
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

  @Override public Expression getFilterExpression() {
    // TODO Auto-generated method stub
    return exp;
  }

}
