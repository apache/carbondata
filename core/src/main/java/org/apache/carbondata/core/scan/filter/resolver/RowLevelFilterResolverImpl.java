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

import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.conditional.ConditionalExpression;
import org.apache.carbondata.core.scan.filter.TableProvider;
import org.apache.carbondata.core.scan.filter.intf.FilterExecuterType;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.MeasureColumnResolvedFilterInfo;

public class RowLevelFilterResolverImpl extends ConditionalFilterResolverImpl {

  private static final long serialVersionUID = 176122729713729929L;

  private List<DimColumnResolvedFilterInfo> dimColEvaluatorInfoList;
  private List<MeasureColumnResolvedFilterInfo> msrColEvalutorInfoList;
  private AbsoluteTableIdentifier tableIdentifier;

  public RowLevelFilterResolverImpl(Expression exp, boolean isExpressionResolve,
      boolean isIncludeFilter, AbsoluteTableIdentifier tableIdentifier) {
    super(exp, isExpressionResolve, isIncludeFilter, false);
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
  public void resolve(AbsoluteTableIdentifier absoluteTableIdentifier,
      TableProvider tableProvider) {
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
          msrColumnEvalutorInfo.setCarbonColumn(columnExpression.getCarbonColumn());
          msrColumnEvalutorInfo.setRowIndex(index++);
          msrColumnEvalutorInfo
              .setColumnIndex(columnExpression.getCarbonColumn().getOrdinal());
          msrColumnEvalutorInfo.setMeasure(columnExpression.getMeasure());
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
