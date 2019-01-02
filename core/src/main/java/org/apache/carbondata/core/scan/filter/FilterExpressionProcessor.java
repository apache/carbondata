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
import java.util.List;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.DataRefNode;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.PartitionInfo;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.scan.expression.BinaryExpression;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.LiteralExpression;
import org.apache.carbondata.core.scan.expression.conditional.BinaryConditionalExpression;
import org.apache.carbondata.core.scan.expression.conditional.ConditionalExpression;
import org.apache.carbondata.core.scan.expression.conditional.EqualToExpression;
import org.apache.carbondata.core.scan.expression.conditional.GreaterThanEqualToExpression;
import org.apache.carbondata.core.scan.expression.conditional.GreaterThanExpression;
import org.apache.carbondata.core.scan.expression.conditional.InExpression;
import org.apache.carbondata.core.scan.expression.conditional.LessThanEqualToExpression;
import org.apache.carbondata.core.scan.expression.conditional.LessThanExpression;
import org.apache.carbondata.core.scan.expression.conditional.ListExpression;
import org.apache.carbondata.core.scan.expression.conditional.StartsWithExpression;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.expression.logical.AndExpression;
import org.apache.carbondata.core.scan.filter.executer.FilterExecuter;
import org.apache.carbondata.core.scan.filter.executer.ImplicitColumnFilterExecutor;
import org.apache.carbondata.core.scan.filter.intf.ExpressionType;
import org.apache.carbondata.core.scan.filter.partition.AndFilterImpl;
import org.apache.carbondata.core.scan.filter.partition.EqualToFilterImpl;
import org.apache.carbondata.core.scan.filter.partition.InFilterImpl;
import org.apache.carbondata.core.scan.filter.partition.KeepAllPartitionFilterImpl;
import org.apache.carbondata.core.scan.filter.partition.OrFilterImpl;
import org.apache.carbondata.core.scan.filter.partition.PartitionFilterIntf;
import org.apache.carbondata.core.scan.filter.partition.PruneAllPartitionFilterImpl;
import org.apache.carbondata.core.scan.filter.partition.RangeFilterImpl;
import org.apache.carbondata.core.scan.filter.resolver.ConditionalFilterResolverImpl;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.scan.filter.resolver.LogicalFilterResolverImpl;
import org.apache.carbondata.core.scan.filter.resolver.RowLevelFilterResolverImpl;
import org.apache.carbondata.core.scan.filter.resolver.RowLevelRangeFilterResolverImpl;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.FalseConditionalResolverImpl;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.TrueConditionalResolverImpl;
import org.apache.carbondata.core.scan.partition.PartitionUtil;
import org.apache.carbondata.core.scan.partition.Partitioner;

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
   * Get the map of required partitions
   * The value of "1" in BitSet represent the required partition
   * @param expressionTree
   * @param partitionInfo
   * @return
   */
  @Override public BitSet getFilteredPartitions(Expression expressionTree,
      PartitionInfo partitionInfo) {
    Partitioner partitioner = PartitionUtil.getPartitioner(partitionInfo);
    return createPartitionFilterTree(expressionTree, partitionInfo).applyFilter(partitioner);
  }

  /**
   * create partition filter by basing on pushed-down filter
   * @param expressionTree
   * @param partitionInfo
   * @return
   */
  private PartitionFilterIntf createPartitionFilterTree(Expression expressionTree,
      PartitionInfo partitionInfo) {
    ExpressionType filterExpressionType = expressionTree.getFilterExpressionType();
    String partitionColumnName = partitionInfo.getColumnSchemaList().get(0).getColumnName();
    BinaryExpression currentExpression = null;
    ColumnExpression left = null;
    switch (filterExpressionType) {
      case OR:
        currentExpression = (BinaryExpression) expressionTree;
        return new OrFilterImpl(
            createPartitionFilterTree(currentExpression.getLeft(), partitionInfo),
            createPartitionFilterTree(currentExpression.getRight(), partitionInfo));
      case RANGE:
      case AND:
        currentExpression = (BinaryExpression) expressionTree;
        return new AndFilterImpl(
            createPartitionFilterTree(currentExpression.getLeft(), partitionInfo),
            createPartitionFilterTree(currentExpression.getRight(), partitionInfo));
      case EQUALS:
        EqualToExpression equalTo = (EqualToExpression) expressionTree;
        if (equalTo.getLeft() instanceof ColumnExpression &&
            equalTo.getRight() instanceof LiteralExpression) {
          left = (ColumnExpression) equalTo.getLeft();
          if (partitionColumnName.equals(left.getCarbonColumn().getColName())) {
            return new EqualToFilterImpl(equalTo, partitionInfo);
          }
        }
        return new KeepAllPartitionFilterImpl();
      case IN:
        InExpression in = (InExpression) expressionTree;
        if (in.getLeft() instanceof ColumnExpression &&
            in.getRight() instanceof ListExpression) {
          left = (ColumnExpression) in.getLeft();
          if (partitionColumnName.equals(left.getCarbonColumn().getColName())) {
            return new InFilterImpl(in, partitionInfo);
          }
        }
        return new KeepAllPartitionFilterImpl();
      case FALSE:
        return new PruneAllPartitionFilterImpl();
      case TRUE:
        return new KeepAllPartitionFilterImpl();
      case GREATERTHAN:
        GreaterThanExpression greaterThan = (GreaterThanExpression) expressionTree;
        if (greaterThan.getLeft() instanceof ColumnExpression &&
            greaterThan.getRight() instanceof LiteralExpression) {
          left = (ColumnExpression) greaterThan.getLeft();
          if (partitionColumnName.equals(left.getCarbonColumn().getColName())) {
            return new RangeFilterImpl((LiteralExpression) greaterThan.getRight(), true, false,
                partitionInfo);
          }
        }
        return new KeepAllPartitionFilterImpl();
      case GREATERTHAN_EQUALTO:
        GreaterThanEqualToExpression greaterThanEqualTo =
            (GreaterThanEqualToExpression) expressionTree;
        if (greaterThanEqualTo.getLeft() instanceof ColumnExpression &&
            greaterThanEqualTo.getRight() instanceof LiteralExpression) {
          left = (ColumnExpression) greaterThanEqualTo.getLeft();
          if (partitionColumnName.equals(left.getCarbonColumn().getColName())) {
            return new RangeFilterImpl((LiteralExpression) greaterThanEqualTo.getRight(), true,
                true, partitionInfo);
          }
        }
        return new KeepAllPartitionFilterImpl();
      case LESSTHAN:
        LessThanExpression lessThan = (LessThanExpression) expressionTree;
        if (lessThan.getLeft() instanceof ColumnExpression &&
            lessThan.getRight() instanceof LiteralExpression) {
          left = (ColumnExpression) lessThan.getLeft();
          if (partitionColumnName.equals(left.getCarbonColumn().getColName())) {
            return new RangeFilterImpl((LiteralExpression) lessThan.getRight(), false, false,
                partitionInfo);
          }
        }
        return new KeepAllPartitionFilterImpl();
      case LESSTHAN_EQUALTO:
        LessThanEqualToExpression lessThanEqualTo = (LessThanEqualToExpression) expressionTree;
        if (lessThanEqualTo.getLeft() instanceof ColumnExpression &&
            lessThanEqualTo.getRight() instanceof LiteralExpression) {
          left = (ColumnExpression) lessThanEqualTo.getLeft();
          if (partitionColumnName.equals(left.getCarbonColumn().getColName())) {
            return new RangeFilterImpl((LiteralExpression) lessThanEqualTo.getRight(), false, true,
                partitionInfo);
          }
        }
        return new KeepAllPartitionFilterImpl();
      case NOT_IN:
      case NOT_EQUALS:
      default:
        return new KeepAllPartitionFilterImpl();
    }
  }

  /**
   * Selects the blocks based on col max and min value.
   *
   * @param listOfDataBlocksToScan
   * @param dataRefNode
   */
  private void addBlockBasedOnMinMaxValue(FilterExecuter filterExecuter,
      List<DataRefNode> listOfDataBlocksToScan, DataRefNode dataRefNode, boolean[] isMinMaxSet) {
    if (null == dataRefNode.getColumnsMinValue() || null == dataRefNode.getColumnsMaxValue()) {
      listOfDataBlocksToScan.add(dataRefNode);
      return;
    }
    BitSet bitSet = filterExecuter
        .isScanRequired(dataRefNode.getColumnsMaxValue(), dataRefNode.getColumnsMinValue(),
            isMinMaxSet);
    if (!bitSet.isEmpty()) {
      listOfDataBlocksToScan.add(dataRefNode);
    }
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
        if (currentCondExpression.isSingleColumn() && ! column.getDataType().isComplexType()) {
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
              .getCarbonColumn().hasEncoding(Encoding.DIRECT_DICTIONARY)) {
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
        if (currentCondExpression.isSingleColumn() && ! column.getDataType().isComplexType()) {
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
          if (condExpression.isSingleColumn() && ! column.isComplex()) {
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
}
