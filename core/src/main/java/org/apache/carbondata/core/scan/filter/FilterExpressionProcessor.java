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
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.DataRefNode;
import org.apache.carbondata.core.datastore.DataRefNodeFinder;
import org.apache.carbondata.core.datastore.IndexKey;
import org.apache.carbondata.core.datastore.block.AbstractIndex;
import org.apache.carbondata.core.datastore.impl.btree.BTreeDataRefNodeFinder;
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.scan.expression.BinaryExpression;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.conditional.BinaryConditionalExpression;
import org.apache.carbondata.core.scan.expression.conditional.ConditionalExpression;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.filter.executer.FilterExecuter;
import org.apache.carbondata.core.scan.filter.intf.ExpressionType;
import org.apache.carbondata.core.scan.filter.resolver.ConditionalFilterResolverImpl;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.scan.filter.resolver.LogicalFilterResolverImpl;
import org.apache.carbondata.core.scan.filter.resolver.RowLevelFilterResolverImpl;
import org.apache.carbondata.core.scan.filter.resolver.RowLevelRangeFilterResolverImpl;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.TrueConditionalResolverImpl;

public class FilterExpressionProcessor implements FilterProcessor {

  private static final LogService LOGGER =
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
      AbsoluteTableIdentifier tableIdentifier) throws FilterUnsupportedException, IOException {
    if (null != expressionTree && null != tableIdentifier) {
      return getFilterResolvertree(expressionTree, tableIdentifier);
    }
    return null;
  }

  /**
   * This API will scan the Segment level all btrees and selects the required
   * block reference  nodes inorder to push the same to executer for applying filters
   * on the respective data reference node.
   * Following Algorithm is followed in below API
   * Step:1 Get the start end key based on the filter tree resolver information
   * Step:2 Prepare the IndexKeys inorder to scan the tree and get the start and end reference
   * node(block)
   * Step:3 Once data reference node ranges retrieved traverse the node within this range
   * and select the node based on the block min and max value and the filter value.
   * Step:4 The selected blocks will be send to executers for applying the filters with the help
   * of Filter executers.
   *
   */
  public List<DataRefNode> getFilterredBlocks(DataRefNode btreeNode,
      FilterResolverIntf filterResolver, AbstractIndex tableSegment,
      AbsoluteTableIdentifier tableIdentifier) {
    // Need to get the current dimension tables
    List<DataRefNode> listOfDataBlocksToScan = new ArrayList<DataRefNode>();
    // getting the start and end index key based on filter for hitting the
    // selected block reference nodes based on filter resolver tree.
    LOGGER.debug("preparing the start and end key for finding"
        + "start and end block as per filter resolver");
    List<IndexKey> listOfStartEndKeys = new ArrayList<IndexKey>(2);
    FilterUtil.traverseResolverTreeAndGetStartAndEndKey(tableSegment.getSegmentProperties(),
        filterResolver, listOfStartEndKeys);
    // reading the first value from list which has start key
    IndexKey searchStartKey = listOfStartEndKeys.get(0);
    // reading the last value from list which has end key
    IndexKey searchEndKey = listOfStartEndKeys.get(1);
    if (null == searchStartKey && null == searchEndKey) {
      try {
        // TODO need to handle for no dictionary dimensions
        searchStartKey =
            FilterUtil.prepareDefaultStartIndexKey(tableSegment.getSegmentProperties());
        // TODO need to handle for no dictionary dimensions
        searchEndKey = FilterUtil.prepareDefaultEndIndexKey(tableSegment.getSegmentProperties());
      } catch (KeyGenException e) {
        return listOfDataBlocksToScan;
      }
    }

    LOGGER.debug(
        "Successfully retrieved the start and end key" + "Dictionary Start Key: " + searchStartKey
            .getDictionaryKeys() + "No Dictionary Start Key " + searchStartKey.getNoDictionaryKeys()
            + "Dictionary End Key: " + searchEndKey.getDictionaryKeys() + "No Dictionary End Key "
            + searchEndKey.getNoDictionaryKeys());
    long startTimeInMillis = System.currentTimeMillis();
    DataRefNodeFinder blockFinder = new BTreeDataRefNodeFinder(
        tableSegment.getSegmentProperties().getEachDimColumnValueSize());
    DataRefNode startBlock = blockFinder.findFirstDataBlock(btreeNode, searchStartKey);
    DataRefNode endBlock = blockFinder.findLastDataBlock(btreeNode, searchEndKey);
    FilterExecuter filterExecuter =
        FilterUtil.getFilterExecuterTree(filterResolver, tableSegment.getSegmentProperties(),null);
    while (startBlock != endBlock) {
      addBlockBasedOnMinMaxValue(filterExecuter, listOfDataBlocksToScan, startBlock);
      startBlock = startBlock.getNextDataRefNode();
    }
    addBlockBasedOnMinMaxValue(filterExecuter, listOfDataBlocksToScan, endBlock);
    LOGGER.info("Total Time in retrieving the data reference node" + "after scanning the btree " + (
        System.currentTimeMillis() - startTimeInMillis)
        + " Total number of data reference node for executing filter(s) " + listOfDataBlocksToScan
        .size());

    return listOfDataBlocksToScan;
  }

  /**
   * Selects the blocks based on col max and min value.
   *
   * @param listOfDataBlocksToScan
   * @param dataRefNode
   */
  private void addBlockBasedOnMinMaxValue(FilterExecuter filterExecuter,
      List<DataRefNode> listOfDataBlocksToScan, DataRefNode dataRefNode) {

    BitSet bitSet = filterExecuter
        .isScanRequired(dataRefNode.getColumnsMaxValue(), dataRefNode.getColumnsMinValue());
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
      AbsoluteTableIdentifier tableIdentifier) throws FilterUnsupportedException, IOException {
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
      AbsoluteTableIdentifier tableIdentifier) throws FilterUnsupportedException, IOException {
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
        currentExpression = (BinaryExpression) expressionTree;
        return new LogicalFilterResolverImpl(
            createFilterResolverTree(currentExpression.getLeft(), tableIdentifier),
            createFilterResolverTree(currentExpression.getRight(), tableIdentifier),
            currentExpression);
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
        return new RowLevelFilterResolverImpl(expression, false, false, tableIdentifier);
      case TRUE:
        return new TrueConditionalResolverImpl(expression, false, false, tableIdentifier);
      case EQUALS:
        currentCondExpression = (BinaryConditionalExpression) expression;
        if (currentCondExpression.isSingleDimension()
            && currentCondExpression.getColumnList().get(0).getCarbonColumn().getDataType()
            != DataType.ARRAY
            && currentCondExpression.getColumnList().get(0).getCarbonColumn().getDataType()
            != DataType.STRUCT) {
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
              tableIdentifier);

        }
        break;
      case RANGE:
        return new ConditionalFilterResolverImpl(expression, isExpressionResolve, true,
            tableIdentifier);
      case NOT_EQUALS:
        currentCondExpression = (BinaryConditionalExpression) expression;
        if (currentCondExpression.isSingleDimension()
            && currentCondExpression.getColumnList().get(0).getCarbonColumn().getDataType()
            != DataType.ARRAY
            && currentCondExpression.getColumnList().get(0).getCarbonColumn().getDataType()
            != DataType.STRUCT) {
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

            return new ConditionalFilterResolverImpl(expression, isExpressionResolve, false,
                tableIdentifier);
          }
          return new ConditionalFilterResolverImpl(expression, isExpressionResolve, false,
              tableIdentifier);
        }
        break;

      default:
        if (expression instanceof ConditionalExpression) {
          condExpression = (ConditionalExpression) expression;
          if (condExpression.isSingleDimension()
              && condExpression.getColumnList().get(0).getCarbonColumn().getDataType()
              != DataType.ARRAY
              && condExpression.getColumnList().get(0).getCarbonColumn().getDataType()
              != DataType.STRUCT) {
            condExpression = (ConditionalExpression) expression;
            if (condExpression.getColumnList().get(0).getCarbonColumn()
                .hasEncoding(Encoding.DICTIONARY) && !condExpression.getColumnList().get(0)
                .getCarbonColumn().hasEncoding(Encoding.DIRECT_DICTIONARY)) {
              return new ConditionalFilterResolverImpl(expression, true, true, tableIdentifier);
            }
          }
        }
    }
    return new RowLevelFilterResolverImpl(expression, false, false, tableIdentifier);
  }

}
