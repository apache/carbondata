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

package org.carbondata.query.evaluators;

import java.util.ArrayList;
import java.util.List;

import org.carbondata.core.carbon.SqlStatement.Type;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.vo.HybridStoreModel;
import org.carbondata.query.expression.ColumnExpression;
import org.carbondata.query.expression.DataType;
import org.carbondata.query.expression.Expression;
import org.carbondata.query.expression.conditional.BinaryConditionalExpression;
import org.carbondata.query.expression.conditional.ConditionalExpression;
import org.carbondata.query.filters.measurefilter.util.FilterUtil;
import org.carbondata.query.schema.metadata.FilterEvaluatorInfo;

@Deprecated public abstract class AbstractConditionalEvalutor implements FilterEvaluator {
  protected List<DimColumnEvaluatorInfo> dimColEvaluatorInfoList;

  protected List<MsrColumnEvalutorInfo> msrColEvalutorInfoList;

  protected Expression exp;

  protected boolean isExpressionResolve;

  protected boolean isIncludeFilter;

  public AbstractConditionalEvalutor(Expression exp, boolean isExpressionResolve,
      boolean isIncludeFilter) {
    this.dimColEvaluatorInfoList =
        new ArrayList<DimColumnEvaluatorInfo>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    this.msrColEvalutorInfoList =
        new ArrayList<MsrColumnEvalutorInfo>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    this.exp = exp;
    this.isExpressionResolve = isExpressionResolve;
    this.isIncludeFilter = isIncludeFilter;
  }

  @Override public void resolve(FilterEvaluatorInfo info) {
    DimColumnEvaluatorInfo dimColumnEvaluatorInfo = new DimColumnEvaluatorInfo();
    if ((!isExpressionResolve) && exp instanceof BinaryConditionalExpression) {
      BinaryConditionalExpression binaryConditionalExpression = (BinaryConditionalExpression) exp;
      Expression leftExp = binaryConditionalExpression.getLeft();
      Expression rightExp = binaryConditionalExpression.getRight();

      if (leftExp instanceof ColumnExpression) {
        ColumnExpression columnExpression = (ColumnExpression) leftExp;
        if (columnExpression.getDataType().equals(DataType.TimestampType)) {
          isExpressionResolve = true;
        } else {
          // If imei=imei comes in filter condition then we need to skip processing of right
          // expression.
          // This flow has reached here assuming that this is a single column expression.
          // We need to check if the other expression contains column expression or not in depth.
          if (FilterUtil.checkIfExpressionContainsColumn(rightExp)) {
            isExpressionResolve = true;
          } else {
            dimColumnEvaluatorInfo.setColumnIndex(
                getColumnStoreIndex(columnExpression.getDim().getOrdinal(),
                    info.getHybridStoreModel()));
            if (!columnExpression.getDim().isNoDictionaryDim()) {
              dimColumnEvaluatorInfo.setNeedCompressedData(
                  info.getSlices().get(info.getCurrentSliceIndex())
                      .getDataCache(info.getFactTableName()).getAggKeyBlock()[getColumnStoreIndex(
                      columnExpression.getDim().getOrdinal(), info.getHybridStoreModel())]);
            }
            // dimColumnEvaluatorInfo.setFilterValues(
            //    FilterUtil.getFilterList(info, rightExp, columnExpression, this.isIncludeFilter));
            dimColumnEvaluatorInfo.setDims(columnExpression.getDim());
          }
        }
      } else if (rightExp instanceof ColumnExpression) {
        ColumnExpression columnExpression = (ColumnExpression) rightExp;
        if (columnExpression.getDataType().equals(DataType.TimestampType)) {
          isExpressionResolve = true;
        } else {
          // if imei=imei comes in filter condition then we need to skip processing of right
          // expression.
          // This flow has reached here assuming that this is a single column expression.
          // We need to check if the other expression contains column expression or not in depth.
          if (FilterUtil.checkIfExpressionContainsColumn(leftExp)) {
            isExpressionResolve = true;
          } else {
            dimColumnEvaluatorInfo.setColumnIndex(columnExpression.getDim().getOrdinal());
            if (!columnExpression.getDim().isNoDictionaryDim()) {
              dimColumnEvaluatorInfo.setNeedCompressedData(
                  info.getSlices().get(info.getCurrentSliceIndex())
                      .getDataCache(info.getFactTableName()).getAggKeyBlock()[columnExpression
                      .getDim().getOrdinal()]);
            }
            // dimColumnEvaluatorInfo.setFilterValues(
            // FilterUtil.getFilterList(info, leftExp, columnExpression, isIncludeFilter));
            dimColumnEvaluatorInfo.setDims(columnExpression.getDim());
          }
        }
      } else {
        isExpressionResolve = true;
      }
    }

    if (isExpressionResolve && exp instanceof ConditionalExpression) {
      ConditionalExpression conditionalExpression = (ConditionalExpression) exp;
      List<ColumnExpression> columnList = conditionalExpression.getColumnList();
      dimColumnEvaluatorInfo.setColumnIndex(columnList.get(0).getDim().getOrdinal());
      if (!columnList.get(0).getDim().isNoDictionaryDim()) {
        dimColumnEvaluatorInfo.setNeedCompressedData(
            info.getSlices().get(info.getCurrentSliceIndex()).getDataCache(info.getFactTableName())
                .getAggKeyBlock()[columnList.get(0).getDim().getOrdinal()]);
      }
      if (columnList.get(0).getDim().isNoDictionaryDim()) {
        // dimColumnEvaluatorInfo.setFilterValues(
        // FilterUtil.getFilterList(info, exp, columnList.get(0), isIncludeFilter));
      } else if (!(columnList.get(0).getDim().getDataType() == Type.ARRAY
          || columnList.get(0).getDim().getDataType() == Type.STRUCT)) {
        //  dimColumnEvaluatorInfo.setFilterValues(
        // FilterUtil.getFilterListForAllMembers(info, exp, columnList.get(0), isIncludeFilter));
      }
    }
    dimColEvaluatorInfoList.add(dimColumnEvaluatorInfo);
  }

  protected int getColumnStoreIndex(int ordinal, HybridStoreModel hybridStoreModel) {
    if (!hybridStoreModel.isHybridStore()) {
      return ordinal;
    }
    return hybridStoreModel.getStoreIndex(ordinal);

  }

  /**
   * This method will check if a given expression contains a column expression recursively.
   *
   * @param right
   * @return
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

  public FilterEvaluator getLeft() {
    return null;
  }

  public FilterEvaluator getRight() {
    return null;
  }
}
