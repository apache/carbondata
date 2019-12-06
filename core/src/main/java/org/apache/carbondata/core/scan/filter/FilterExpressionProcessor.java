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

package org.apache.carbondata.core.scan.filter;

import java.io.IOException;
import java.util.BitSet;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.scan.expression.BinaryExpression;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.LiteralExpression;
import org.apache.carbondata.core.scan.expression.conditional.BinaryConditionalExpression;
import org.apache.carbondata.core.scan.expression.conditional.ConditionalExpression;
import org.apache.carbondata.core.scan.expression.conditional.GreaterThanEqualToExpression;
import org.apache.carbondata.core.scan.expression.conditional.InExpression;
import org.apache.carbondata.core.scan.expression.conditional.LessThanExpression;
import org.apache.carbondata.core.scan.expression.conditional.StartsWithExpression;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.expression.logical.AndExpression;
import org.apache.carbondata.core.scan.expression.logical.OrExpression;
import org.apache.carbondata.core.scan.expression.logical.TrueExpression;
import org.apache.carbondata.core.scan.filter.executer.FilterExecuter;
import org.apache.carbondata.core.scan.filter.executer.ImplicitColumnFilterExecutor;
import org.apache.carbondata.core.scan.filter.intf.ExpressionType;
import org.apache.carbondata.core.scan.filter.resolver.ConditionalFilterResolverImpl;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.scan.filter.resolver.LogicalFilterResolverImpl;
import org.apache.carbondata.core.scan.filter.resolver.RowLevelFilterResolverImpl;
import org.apache.carbondata.core.scan.filter.resolver.RowLevelRangeFilterResolverImpl;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.FalseConditionalResolverImpl;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.TrueConditionalResolverImpl;

import org.apache.log4j.Logger;

public class FilterExpressionProcessor implements FilterProcessor {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(FilterExpressionProcessor.class.getName());

  /**
   * Implementation will provide the resolved form of filters based on the
   * filter expression tree which is been passed in Expression instance.
   *
   * @param expressionTree  , filter expression tree
   * @param tableIdentifier ,contains carbon store informations
   * @return a filter resolver tree
   */
  public FilterResolverIntf getFilterResolver(Expression expressionTree,
      AbsoluteTableIdentifier tableIdentifier)
      throws FilterUnsupportedException, IOException {
    if (null != expressionTree && null != tableIdentifier) {
      return getFilterResolvertree(expressionTree, tableIdentifier);
    }
    return null;
  }

  /**
   * API will return a filter resolver instance which will be used by
   * executers to evaluate or execute the filters.
   *
   * @param expressionTree , resolver tree which will hold the resolver tree based on
   *                       filter expression.
   * @return FilterResolverIntf type.
   */
  private FilterResolverIntf getFilterResolvertree(Expression expressionTree,
      AbsoluteTableIdentifier tableIdentifier)
      throws FilterUnsupportedException, IOException {
    FilterResolverIntf filterEvaluatorTree =
        createFilterResolverTree(expressionTree, tableIdentifier);
    traverseAndResolveTree(filterEvaluatorTree, tableIdentifier);
    return filterEvaluatorTree;
  }

  /**
   * constructing the filter resolver tree based on filter expression.
   * this method will visit each node of the filter resolver and prepares
   * the surrogates of the filter members which are involved filter
   * expression.
   *
   * @param filterResolverTree
   * @param tableIdentifier
   */
  private void traverseAndResolveTree(FilterResolverIntf filterResolverTree,
      AbsoluteTableIdentifier tableIdentifier)
      throws FilterUnsupportedException, IOException {
    if (null == filterResolverTree) {
      return;
    }
    traverseAndResolveTree(filterResolverTree.getLeft(), tableIdentifier);
    filterResolverTree.resolve(tableIdentifier);
    traverseAndResolveTree(filterResolverTree.getRight(), tableIdentifier);
  }

  /**
   * Pattern used : Visitor Pattern
   * Method will create filter resolver tree based on the filter expression tree,
   * in this algorithm based on the expression instance the resolvers will created
   *
   * @param expressionTree
   * @param tableIdentifier
   * @return
   */
  private FilterResolverIntf createFilterResolverTree(Expression expressionTree,
      AbsoluteTableIdentifier tableIdentifier) {
    ExpressionType filterExpressionType = expressionTree.getFilterExpressionType();
    BinaryExpression currentExpression = null;
    switch (filterExpressionType) {
      case OR:
      case AND:
        currentExpression = (BinaryExpression) expressionTree;
        return new LogicalFilterResolverImpl(
            createFilterResolverTree(currentExpression.getLeft(), tableIdentifier),
            createFilterResolverTree(currentExpression.getRight(), tableIdentifier),
            currentExpression);
      case RANGE:
        return getFilterResolverBasedOnExpressionType(ExpressionType.RANGE, true,
            expressionTree, tableIdentifier, expressionTree);
      case EQUALS:
      case IN:
        return getFilterResolverBasedOnExpressionType(ExpressionType.EQUALS,
            ((BinaryConditionalExpression) expressionTree).isNull, expressionTree,
            tableIdentifier, expressionTree);
      case GREATERTHAN:
      case GREATERTHAN_EQUALTO:
      case LESSTHAN:
      case LESSTHAN_EQUALTO:
        return getFilterResolverBasedOnExpressionType(ExpressionType.EQUALS, true, expressionTree,
            tableIdentifier, expressionTree);
      case STARTSWITH:
        assert (expressionTree instanceof StartsWithExpression);
        currentExpression = (StartsWithExpression) expressionTree;
        Expression re = currentExpression.getRight();
        assert (re instanceof LiteralExpression);
        LiteralExpression literal = (LiteralExpression) re;
        String value = literal.getLiteralExpValue().toString();
        Expression left = new GreaterThanEqualToExpression(currentExpression.getLeft(), literal);
        String maxValueLimit = value.substring(0, value.length() - 1) + (char) (
            ((int) value.charAt(value.length() - 1)) + 1);
        Expression right = new LessThanExpression(currentExpression.getLeft(),
            new LiteralExpression(maxValueLimit, literal.getLiteralExpDataType()));
        currentExpression = new AndExpression(left, right);
        return new LogicalFilterResolverImpl(
            createFilterResolverTree(currentExpression.getLeft(), tableIdentifier),
            createFilterResolverTree(currentExpression.getRight(), tableIdentifier),
            currentExpression);
      case NOT_EQUALS:
      case NOT_IN:
        return getFilterResolverBasedOnExpressionType(ExpressionType.NOT_EQUALS, false,
            expressionTree, tableIdentifier, expressionTree);
      case FALSE:
        return getFilterResolverBasedOnExpressionType(ExpressionType.FALSE, false,
            expressionTree, tableIdentifier, expressionTree);
      case TRUE:
        return getFilterResolverBasedOnExpressionType(ExpressionType.TRUE, false,
            expressionTree, tableIdentifier, expressionTree);
      default:
        return getFilterResolverBasedOnExpressionType(ExpressionType.UNKNOWN, false, expressionTree,
            tableIdentifier, expressionTree);
    }
  }

  /**
   * Factory method which will return the resolver instance based on filter expression
   * expressions.
   */
  private FilterResolverIntf getFilterResolverBasedOnExpressionType(
      ExpressionType filterExpressionType, boolean isExpressionResolve, Expression expression,
      AbsoluteTableIdentifier tableIdentifier, Expression expressionTree) {
    BinaryConditionalExpression currentCondExpression = null;
    ConditionalExpression condExpression = null;
    switch (filterExpressionType) {
      case FALSE:
        return new FalseConditionalResolverImpl(expression, false, false);
      case TRUE:
        return new TrueConditionalResolverImpl(expression, false, false);
      case EQUALS:
        currentCondExpression = (BinaryConditionalExpression) expression;
        // check for implicit column in the expression
        if (currentCondExpression instanceof InExpression) {
          CarbonColumn carbonColumn =
              currentCondExpression.getColumnList().get(0).getCarbonColumn();
          if (carbonColumn.hasEncoding(Encoding.IMPLICIT)) {
            return new ConditionalFilterResolverImpl(expression, isExpressionResolve, true,
                currentCondExpression.getColumnList().get(0).getCarbonColumn().isMeasure());
          }
        }

        CarbonColumn column = currentCondExpression.getColumnList().get(0).getCarbonColumn();
        if (currentCondExpression.isSingleColumn() && !column.getDataType().isComplexType()) {
          if (column.isMeasure()) {
            if (FilterUtil.checkIfExpressionContainsColumn(currentCondExpression.getLeft())
                && FilterUtil.checkIfExpressionContainsColumn(currentCondExpression.getRight()) || (
                FilterUtil.checkIfRightExpressionRequireEvaluation(currentCondExpression.getRight())
                    || FilterUtil
                    .checkIfLeftExpressionRequireEvaluation(currentCondExpression.getLeft()))) {
              return new RowLevelFilterResolverImpl(expression, isExpressionResolve, true,
                  tableIdentifier);
            }
            if (currentCondExpression.getFilterExpressionType() == ExpressionType.GREATERTHAN
                || currentCondExpression.getFilterExpressionType() == ExpressionType.LESSTHAN
                || currentCondExpression.getFilterExpressionType()
                == ExpressionType.GREATERTHAN_EQUALTO
                || currentCondExpression.getFilterExpressionType()
                == ExpressionType.LESSTHAN_EQUALTO) {
              return new RowLevelRangeFilterResolverImpl(expression, isExpressionResolve, true,
                  tableIdentifier);
            }
            return new ConditionalFilterResolverImpl(expression, isExpressionResolve, true,
                currentCondExpression.getColumnList().get(0).getCarbonColumn().isMeasure());
          }
          // getting new dim index.
          if (!currentCondExpression.getColumnList().get(0).getCarbonColumn()
              .hasEncoding(Encoding.DICTIONARY) || currentCondExpression.getColumnList().get(0)
              .getCarbonColumn().hasEncoding(Encoding.DIRECT_DICTIONARY) || currentCondExpression
              .isAlreadyResolved()) {
            // In case of Range Column Dictionary Include we do not need to resolve the range
            // expression as it is already resolved and has the surrogates in the filter value
            if (FilterUtil.checkIfExpressionContainsColumn(currentCondExpression.getLeft())
                && FilterUtil.checkIfExpressionContainsColumn(currentCondExpression.getRight()) || (
                FilterUtil.checkIfRightExpressionRequireEvaluation(currentCondExpression.getRight())
                    || FilterUtil
                    .checkIfLeftExpressionRequireEvaluation(currentCondExpression.getLeft()))) {
              return new RowLevelFilterResolverImpl(expression, isExpressionResolve, true,
                  tableIdentifier);
            }
            if (currentCondExpression.getFilterExpressionType() == ExpressionType.GREATERTHAN
                || currentCondExpression.getFilterExpressionType() == ExpressionType.LESSTHAN
                || currentCondExpression.getFilterExpressionType()
                == ExpressionType.GREATERTHAN_EQUALTO
                || currentCondExpression.getFilterExpressionType()
                == ExpressionType.LESSTHAN_EQUALTO) {
              return new RowLevelRangeFilterResolverImpl(expression, isExpressionResolve, true,
                  tableIdentifier);
            }
          }
          return new ConditionalFilterResolverImpl(expression, isExpressionResolve, true,
              currentCondExpression.getColumnList().get(0).getCarbonColumn().isMeasure());

        }
        break;
      case RANGE:
        return new ConditionalFilterResolverImpl(expression, isExpressionResolve, true, false);
      case NOT_EQUALS:
        currentCondExpression = (BinaryConditionalExpression) expression;
        column = currentCondExpression.getColumnList().get(0).getCarbonColumn();
        if (currentCondExpression.isSingleColumn() && !column.getDataType().isComplexType()) {
          if (column.isMeasure()) {
            if (FilterUtil.checkIfExpressionContainsColumn(currentCondExpression.getLeft())
                && FilterUtil.checkIfExpressionContainsColumn(currentCondExpression.getRight()) || (
                FilterUtil.checkIfRightExpressionRequireEvaluation(currentCondExpression.getRight())
                    || FilterUtil
                    .checkIfLeftExpressionRequireEvaluation(currentCondExpression.getLeft()))) {
              return new RowLevelFilterResolverImpl(expression, isExpressionResolve, false,
                  tableIdentifier);
            }
            if (currentCondExpression.getFilterExpressionType() == ExpressionType.GREATERTHAN
                || currentCondExpression.getFilterExpressionType() == ExpressionType.LESSTHAN
                || currentCondExpression.getFilterExpressionType()
                == ExpressionType.GREATERTHAN_EQUALTO
                || currentCondExpression.getFilterExpressionType()
                == ExpressionType.LESSTHAN_EQUALTO) {
              return new RowLevelRangeFilterResolverImpl(expression, isExpressionResolve, false,
                  tableIdentifier);
            }
            return new ConditionalFilterResolverImpl(expression, isExpressionResolve, false, true);
          }

          if (!currentCondExpression.getColumnList().get(0).getCarbonColumn()
              .hasEncoding(Encoding.DICTIONARY) || currentCondExpression.getColumnList().get(0)
              .getCarbonColumn().hasEncoding(Encoding.DIRECT_DICTIONARY)) {
            if (FilterUtil.checkIfExpressionContainsColumn(currentCondExpression.getLeft())
                && FilterUtil.checkIfExpressionContainsColumn(currentCondExpression.getRight()) || (
                FilterUtil.checkIfRightExpressionRequireEvaluation(currentCondExpression.getRight())
                    || FilterUtil
                    .checkIfLeftExpressionRequireEvaluation(currentCondExpression.getLeft()))) {
              return new RowLevelFilterResolverImpl(expression, isExpressionResolve, false,
                  tableIdentifier);
            }
            if (expressionTree.getFilterExpressionType() == ExpressionType.GREATERTHAN
                || expressionTree.getFilterExpressionType() == ExpressionType.LESSTHAN
                || expressionTree.getFilterExpressionType() == ExpressionType.GREATERTHAN_EQUALTO
                || expressionTree.getFilterExpressionType() == ExpressionType.LESSTHAN_EQUALTO) {

              return new RowLevelRangeFilterResolverImpl(expression, isExpressionResolve, false,
                  tableIdentifier);
            }

            return new ConditionalFilterResolverImpl(expression, isExpressionResolve, false, false);
          }
          return new ConditionalFilterResolverImpl(expression, isExpressionResolve, false, false);
        }
        break;

      default:
        if (expression instanceof ConditionalExpression) {
          condExpression = (ConditionalExpression) expression;
          column = condExpression.getColumnList().get(0).getCarbonColumn();
          if (condExpression.isSingleColumn() && !column.isComplex()) {
            condExpression = (ConditionalExpression) expression;
            if ((condExpression.getColumnList().get(0).getCarbonColumn()
                .hasEncoding(Encoding.DICTIONARY) && !condExpression.getColumnList().get(0)
                .getCarbonColumn().hasEncoding(Encoding.DIRECT_DICTIONARY))
                || (condExpression.getColumnList().get(0).getCarbonColumn().isMeasure())) {
              return new ConditionalFilterResolverImpl(expression, true, true,
                  condExpression.getColumnList().get(0).getCarbonColumn().isMeasure());
            }
          }
        }
    }
    return new RowLevelFilterResolverImpl(expression, false, false, tableIdentifier);
  }

  public static boolean isScanRequired(FilterExecuter filterExecuter, byte[][] maxValue,
      byte[][] minValue, boolean[] isMinMaxSet) {
    if (filterExecuter instanceof ImplicitColumnFilterExecutor) {
      return ((ImplicitColumnFilterExecutor) filterExecuter)
          .isFilterValuesPresentInAbstractIndex(maxValue, minValue, isMinMaxSet);
    } else {
      // otherwise decide based on min/max value
      BitSet bitSet = filterExecuter.isScanRequired(maxValue, minValue, isMinMaxSet);
      return !bitSet.isEmpty();
    }
  }

  /**
   * Remove UnknownExpression and change to TrueExpression
   *
   * @param expressionTree
   * @return expressionTree without UnknownExpression
   */
  public Expression removeUnknownExpression(Expression expressionTree) {
    ExpressionType filterExpressionType = expressionTree.getFilterExpressionType();
    BinaryExpression currentExpression = null;
    switch (filterExpressionType) {
      case OR:
        currentExpression = (BinaryExpression) expressionTree;
        return new OrExpression(
                removeUnknownExpression(currentExpression.getLeft()),
                removeUnknownExpression(currentExpression.getRight())
        );
      case AND:
        currentExpression = (BinaryExpression) expressionTree;
        return new AndExpression(
                removeUnknownExpression(currentExpression.getLeft()),
                removeUnknownExpression(currentExpression.getRight())
        );
      case UNKNOWN:
        return new TrueExpression(null);
      default:
        return expressionTree;
    }
  }

  /**
   * Change UnknownReslover to TrueExpression Reslover.
   *
   * @param tableIdentifier
   * @return
   */
  public FilterResolverIntf changeUnknownResloverToTrue(AbsoluteTableIdentifier tableIdentifier) {
    return getFilterResolverBasedOnExpressionType(ExpressionType.TRUE, false,
        new TrueExpression(null), tableIdentifier, new TrueExpression(null));

  }
}
