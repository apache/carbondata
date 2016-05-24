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

import java.util.ArrayList;
import java.util.List;

import org.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonMeasure;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.query.carbonfilterinterface.FilterExecuterType;
import org.carbondata.query.expression.ColumnExpression;
import org.carbondata.query.expression.Expression;
import org.carbondata.query.expression.conditional.ConditionalExpression;
import org.carbondata.query.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;
import org.carbondata.query.filter.resolver.resolverinfo.MeasureColumnResolvedFilterInfo;

public class RowLevelFilterResolverImpl extends ConditionalFilterResolverImpl {

  /**
   *
   */
  private static final long serialVersionUID = 176122729713729929L;
  protected boolean isExpressionResolve;
  protected boolean isIncludeFilter;

  private List<DimColumnResolvedFilterInfo> dimColEvaluatorInfoList;
  private List<MeasureColumnResolvedFilterInfo> msrColEvalutorInfoList;
  private AbsoluteTableIdentifier tableIdentifier;

  public RowLevelFilterResolverImpl(Expression exp, boolean isExpressionResolve,
      boolean isIncludeFilter, AbsoluteTableIdentifier tableIdentifier) {
    super(exp, isExpressionResolve, isIncludeFilter);
    dimColEvaluatorInfoList =
        new ArrayList<DimColumnResolvedFilterInfo>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    msrColEvalutorInfoList = new ArrayList<MeasureColumnResolvedFilterInfo>(
        CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    this.tableIdentifier = tableIdentifier;
  }

  /**
   * Method which will resolve the filter expression by converting the filter member
   * to its assigned dictionary values.
   */
  public void resolve(AbsoluteTableIdentifier absoluteTableIdentifier) {
    DimColumnResolvedFilterInfo dimColumnEvaluatorInfo = null;
    MeasureColumnResolvedFilterInfo msrColumnEvalutorInfo = null;
    int index = 0;
    if (exp instanceof ConditionalExpression) {
      ConditionalExpression conditionalExpression = (ConditionalExpression) exp;
      List<ColumnExpression> columnList = conditionalExpression.getColumnList();
      for (ColumnExpression columnExpression : columnList) {
        if (columnExpression.isDimension()) {
          dimColumnEvaluatorInfo = new DimColumnResolvedFilterInfo();
          dimColumnEvaluatorInfo.setColumnIndex(columnExpression.getCarbonColumn().getOrdinal());
          dimColumnEvaluatorInfo.setRowIndex(index++);
          dimColumnEvaluatorInfo.setDimension(columnExpression.getDimension());
          dimColumnEvaluatorInfo.setDimensionExistsInCurrentSilce(false);
          dimColEvaluatorInfoList.add(dimColumnEvaluatorInfo);
        } else {
          msrColumnEvalutorInfo = new MeasureColumnResolvedFilterInfo();
          msrColumnEvalutorInfo.setRowIndex(index++);
          msrColumnEvalutorInfo.setAggregator(
              ((CarbonMeasure) columnExpression.getCarbonColumn()).getAggregateFunction());
          msrColumnEvalutorInfo
              .setColumnIndex(((CarbonMeasure) columnExpression.getCarbonColumn()).getOrdinal());
          msrColumnEvalutorInfo.setType(columnExpression.getCarbonColumn().getDataType());
          msrColEvalutorInfoList.add(msrColumnEvalutorInfo);
        }
      }
    }
  }

  /**
   * This method will provide the executer type to the callee inorder to identify
   * the executer type for the filter resolution, Row level filter executer is a
   * special executer since it get all the rows of the specified filter dimension
   * and will be send to the spark for processing
   */
  @Override public FilterExecuterType getFilterExecuterType() {
    return FilterExecuterType.ROWLEVEL;
  }

  /**
   * Method will the read filter expression corresponding to the resolver.
   * This method is required in row level executer inorder to evaluate the filter
   * expression against spark, as mentioned above row level is a special type
   * filter resolver.
   *
   * @return Expression
   */
  public Expression getFilterExpresion() {
    return exp;
  }

  /**
   * Method will return the DimColumnResolvedFilterInfo instance which consists
   * the mapping of the respective dimension and its surrogates involved in
   * filter expression.
   *
   * @return DimColumnResolvedFilterInfo
   */
  public List<DimColumnResolvedFilterInfo> getDimColEvaluatorInfoList() {
    return dimColEvaluatorInfoList;
  }

  /**
   * Method will return the DimColumnResolvedFilterInfo instance which containts
   * measure level details.
   *
   * @return MeasureColumnResolvedFilterInfo
   */
  public List<MeasureColumnResolvedFilterInfo> getMsrColEvalutorInfoList() {
    return msrColEvalutorInfoList;
  }

  /**
   * Method will return table information which will be required for retrieving
   * dictionary cache inorder to read all the members of respective dimension.
   *
   * @return AbsoluteTableIdentifier
   */
  public AbsoluteTableIdentifier getTableIdentifier() {
    return tableIdentifier;
  }

}
