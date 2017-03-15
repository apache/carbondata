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
package org.apache.carbondata.core.scan.filter.resolver;

import java.io.IOException;
import java.util.List;
import java.util.SortedMap;

import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.conditional.BinaryConditionalExpression;
import org.apache.carbondata.core.scan.expression.conditional.ConditionalExpression;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.expression.logical.RangeExpression;
import org.apache.carbondata.core.scan.filter.FilterUtil;
import org.apache.carbondata.core.scan.filter.intf.FilterExecuterType;
import org.apache.carbondata.core.scan.filter.resolver.metadata.FilterResolverMetadata;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.visitor.FilterInfoTypeVisitorFactory;

public class ConditionalFilterResolverImpl implements FilterResolverIntf {

  private static final long serialVersionUID = 1838955268462201691L;
  protected Expression exp;
  protected boolean isExpressionResolve;
  protected boolean isIncludeFilter;
  private DimColumnResolvedFilterInfo dimColResolvedFilterInfo;
  private AbsoluteTableIdentifier tableIdentifier;

  public ConditionalFilterResolverImpl(Expression exp, boolean isExpressionResolve,
      boolean isIncludeFilter, AbsoluteTableIdentifier tableIdentifier) {
    this.exp = exp;
    this.isExpressionResolve = isExpressionResolve;
    this.isIncludeFilter = isIncludeFilter;
    this.tableIdentifier = tableIdentifier;
    this.dimColResolvedFilterInfo = new DimColumnResolvedFilterInfo();

  }


  /**
   * This API will resolve the filter expression and generates the
   * dictionaries for executing/evaluating the filter expressions in the
   * executer layer.
   *
   * @throws FilterUnsupportedException
   */
  @Override public void resolve(AbsoluteTableIdentifier absoluteTableIdentifier)
      throws FilterUnsupportedException, IOException {
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
        CarbonDimension dimension = columnExpression.getDimension();
        if (FilterUtil.checkIfExpressionContainsColumn(rightExp)
            || FilterUtil.isExpressionNeedsToResolved(rightExp, isIncludeFilter) &&
            dimension.hasEncoding(Encoding.DICTIONARY) && !dimension
            .hasEncoding(Encoding.DIRECT_DICTIONARY)) {
          isExpressionResolve = true;
        } else {
          //Visitor pattern is been used in this scenario inorder to populate the
          // dimColResolvedFilterInfo
          //visitable object with filter member values based on the visitor type, currently there
          //3 types of visitors custom,direct and no dictionary, all types of visitor populate
          //the visitable instance as per its buisness logic which is different for all the
          // visitors.
          dimColResolvedFilterInfo.populateFilterInfoBasedOnColumnType(
              FilterInfoTypeVisitorFactory.getResolvedFilterInfoVisitor(columnExpression, exp),
              metadata);
        }
      } else if (rightExp instanceof ColumnExpression) {
        ColumnExpression columnExpression = (ColumnExpression) rightExp;
        metadata.setColumnExpression(columnExpression);
        metadata.setExpression(leftExp);
        metadata.setIncludeFilter(isIncludeFilter);
        if (columnExpression.getDataType().equals(DataType.TIMESTAMP) ||
            columnExpression.getDataType().equals(DataType.DATE)) {
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
                FilterInfoTypeVisitorFactory.getResolvedFilterInfoVisitor(columnExpression, exp),
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
          .getDimension().hasEncoding(Encoding.DIRECT_DICTIONARY)
          || (exp instanceof RangeExpression)) {
        dimColResolvedFilterInfo.populateFilterInfoBasedOnColumnType(
            FilterInfoTypeVisitorFactory.getResolvedFilterInfoVisitor(columnList.get(0), exp),
            metadata);

      } else if (columnList.get(0).getDimension().hasEncoding(Encoding.DICTIONARY) && !(
          columnList.get(0).getDimension().getDataType()
              == org.apache.carbondata.core.metadata.datatype.DataType.STRUCT
              || columnList.get(0).getDimension().getDataType()
              == org.apache.carbondata.core.metadata.datatype.DataType.ARRAY)) {
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

  public AbsoluteTableIdentifier getTableIdentifier() {
    return tableIdentifier;
  }

  /**
   * method will calculates the start key based on the filter surrogates
   */
  public void getStartKey(SegmentProperties segmentProperties, long[] startKey,
      SortedMap<Integer, byte[]> setOfStartKeyByteArray, List<long[]> startKeyList) {
    FilterUtil.getStartKey(dimColResolvedFilterInfo.getDimensionResolvedFilterInstance(),
        segmentProperties, startKey, startKeyList);
    FilterUtil.getStartKeyForNoDictionaryDimension(dimColResolvedFilterInfo,
        segmentProperties, setOfStartKeyByteArray);
  }

  /**
   * get the start key based on the filter surrogates
   */
  @Override public void getEndKey(SegmentProperties segmentProperties, long[] endKeys,
      SortedMap<Integer, byte[]> setOfEndKeyByteArray, List<long[]> endKeyList) {
    FilterUtil.getEndKey(dimColResolvedFilterInfo.getDimensionResolvedFilterInstance(), endKeys,
        segmentProperties, endKeyList);
    FilterUtil.getEndKeyForNoDictionaryDimension(dimColResolvedFilterInfo, segmentProperties,
        setOfEndKeyByteArray);
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
      case RANGE:
        if (isColDictionary()) {
          return FilterExecuterType.INCLUDE;
        } else {
          return FilterExecuterType.RANGE;
        }
      default:
        return FilterExecuterType.INCLUDE;
    }

  }

  private boolean isColDictionary() {
    RangeExpression condExp = (RangeExpression) exp;
    List<ColumnExpression> columnList = condExp.getColumnList();
    if (columnList.get(0).getDimension().hasEncoding(Encoding.DICTIONARY)) {
      if (columnList.get(0).getDimension().hasEncoding(Encoding.DIRECT_DICTIONARY)) {
        return false;
      } else {
        return true;
      }
    } else {
      return false;
    }
  }

  /**
   * This method will return the filter values which is present in the range level
   * conditional expressions.
   *
   * @return
   */
  public byte[][] getFilterRangeValues(SegmentProperties segmentProperties) {

    if (null != dimColResolvedFilterInfo.getFilterValues() && !dimColResolvedFilterInfo
        .getDimension().hasEncoding(Encoding.DICTIONARY)) {
      List<byte[]> noDictFilterValuesList =
          dimColResolvedFilterInfo.getFilterValues().getNoDictionaryFilterValuesList();
      return noDictFilterValuesList.toArray((new byte[noDictFilterValuesList.size()][]));
    } else if (null != dimColResolvedFilterInfo.getFilterValues() && dimColResolvedFilterInfo
        .getDimension().hasEncoding(Encoding.DIRECT_DICTIONARY)) {
      return FilterUtil.getKeyArray(this.dimColResolvedFilterInfo.getFilterValues(),
          this.dimColResolvedFilterInfo.getDimension(), segmentProperties);
    }
    return null;

  }


  @Override public Expression getFilterExpression() {
    // TODO Auto-generated method stub
    return exp;
  }

}
