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
package org.carbondata.query.filters;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.carbondata.core.carbon.datastore.DataRefNode;
import org.carbondata.core.carbon.datastore.DataRefNodeFinder;
import org.carbondata.core.carbon.datastore.IndexKey;
import org.carbondata.core.carbon.datastore.block.AbstractIndex;
import org.carbondata.core.carbon.datastore.block.SegmentProperties;
import org.carbondata.core.carbon.datastore.impl.btree.BTreeDataRefNodeFinder;
import org.carbondata.core.carbon.metadata.datatype.DataType;
import org.carbondata.core.carbon.metadata.encoder.Encoding;
import org.carbondata.core.keygenerator.KeyGenException;
import org.carbondata.query.carbon.executor.exception.QueryExecutionException;
import org.carbondata.query.carbonfilterinterface.ExpressionType;
import org.carbondata.query.expression.BinaryExpression;
import org.carbondata.query.expression.Expression;
import org.carbondata.query.expression.conditional.BinaryConditionalExpression;
import org.carbondata.query.expression.conditional.ConditionalExpression;
import org.carbondata.query.expression.exception.FilterUnsupportedException;
import org.carbondata.query.expression.logical.BinaryLogicalExpression;
import org.carbondata.query.filter.executer.FilterExecuter;
import org.carbondata.query.filter.resolver.ConditionalFilterResolverImpl;
import org.carbondata.query.filter.resolver.FilterResolverIntf;
import org.carbondata.query.filter.resolver.LogicalFilterResolverImpl;
import org.carbondata.query.filter.resolver.RowLevelFilterResolverImpl;
import org.carbondata.query.filter.resolver.RowLevelRangeFilterResolverImpl;
import org.carbondata.query.filters.measurefilter.util.FilterUtil;

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
   * @throws QueryExecutionException
   * @throws FilterUnsupportedException
   */
  public FilterResolverIntf getFilterResolver(Expression expressionTree,
      AbsoluteTableIdentifier tableIdentifier) throws FilterUnsupportedException {
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
   * @throws QueryExecutionException
   */
  public List<DataRefNode> getFilterredBlocks(DataRefNode btreeNode,
      FilterResolverIntf filterResolver, AbstractIndex tableSegment,
      AbsoluteTableIdentifier tableIdentifier) throws QueryExecutionException {
    // Need to get the current dimension tables
    List<DataRefNode> listOfDataBlocksToScan = new ArrayList<DataRefNode>();
    // getting the start and end index key based on filter for hitting the
    // selected block reference nodes based on filter resolver tree.
    LOGGER.info("preparing the start and end key for finding"
        + "start and end block as per filter resolver");
    List<IndexKey> listOfStartEndKeys = new ArrayList<IndexKey>(2);
    FilterUtil.traverseResolverTreeAndGetStartAndEndKey(tableSegment.getSegmentProperties(),
        tableIdentifier, filterResolver, listOfStartEndKeys);
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

    LOGGER.info("Successfully retrieved the start and end key");
    long startTimeInMillis = System.currentTimeMillis();
    DataRefNodeFinder blockFinder = new BTreeDataRefNodeFinder(
        tableSegment.getSegmentProperties().getDimensionColumnsValueSize());
    DataRefNode startBlock = blockFinder.findFirstDataBlock(btreeNode, searchStartKey);
    DataRefNode endBlock = blockFinder.findLastDataBlock(btreeNode, searchEndKey);
    FilterExecuter filterExecuter =
        FilterUtil.getFilterExecuterTree(filterResolver, tableSegment.getSegmentProperties());
    while (startBlock != endBlock) {
      addBlockBasedOnMinMaxValue(filterExecuter, listOfDataBlocksToScan, startBlock,
          tableSegment.getSegmentProperties());
      startBlock = startBlock.getNextDataRefNode();
    }
    addBlockBasedOnMinMaxValue(filterExecuter, listOfDataBlocksToScan, endBlock,
        tableSegment.getSegmentProperties());
    LOGGER.info("Total Time in retrieving the data reference node" + "after scanning the btree " + (
        System.currentTimeMillis() - startTimeInMillis)
        + " Total number of data reference node for executing filter(s) " + listOfDataBlocksToScan
        .size());

    return listOfDataBlocksToScan;
  }

  /**
   * Selects the blocks based on col max and min value.
   *
   * @param filterResolver
   * @param listOfDataBlocksToScan
   * @param dataRefNode
   * @param segmentProperties
   */
  private void addBlockBasedOnMinMaxValue(FilterExecuter filterExecuter,
      List<DataRefNode> listOfDataBlocksToScan, DataRefNode dataRefNode,
      SegmentProperties segmentProperties) {

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
   * @throws QueryExecutionException
   * @throws FilterUnsupportedException
   */
  private FilterResolverIntf getFilterResolvertree(Expression expressionTree,
      AbsoluteTableIdentifier tableIdentifier) throws FilterUnsupportedException {
    FilterResolverIntf filterEvaluatorTree =
        createFilterResolverTree(expressionTree, tableIdentifier, null);
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
   * @throws FilterUnsupportedException
   * @throws QueryExecutionException
   */
  private void traverseAndResolveTree(FilterResolverIntf filterResolverTree,
      AbsoluteTableIdentifier tableIdentifier) throws FilterUnsupportedException {
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
      AbsoluteTableIdentifier tableIdentifier, Expression intermediateExpression) {
    ExpressionType filterExpressionType = expressionTree.getFilterExpressionType();
    BinaryExpression currentExpression = null;
    BinaryLogicalExpression logicalExpression = null;
    switch (filterExpressionType) {
      case OR:
        currentExpression = (BinaryExpression) expressionTree;
        return new LogicalFilterResolverImpl(
            createFilterResolverTree(currentExpression.getLeft(), tableIdentifier,
                currentExpression),
            createFilterResolverTree(currentExpression.getRight(), tableIdentifier,
                currentExpression), filterExpressionType);
      case AND:
        logicalExpression = (BinaryLogicalExpression) expressionTree;
        return new LogicalFilterResolverImpl(
            createFilterResolverTree(logicalExpression.getLeft(), tableIdentifier,
                currentExpression),
            createFilterResolverTree(logicalExpression.getRight(), tableIdentifier,
                currentExpression), filterExpressionType);
      case EQUALS:
      case IN:
        return getFilterResolverBasedOnExpressionType(ExpressionType.EQUALS, false, expressionTree,
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
                FilterUtil.checkIfExpressionContainsUnknownExp(currentCondExpression.getRight())
                    || FilterUtil
                    .checkIfExpressionContainsUnknownExp(currentCondExpression.getLeft()))) {
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
          return new ConditionalFilterResolverImpl(expression, isExpressionResolve, true);

        }
        break;
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
                FilterUtil.checkIfExpressionContainsUnknownExp(currentCondExpression.getRight())
                    || FilterUtil
                    .checkIfExpressionContainsUnknownExp(currentCondExpression.getLeft()))) {
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

            return new ConditionalFilterResolverImpl(expression, isExpressionResolve, false);
          }
          return new ConditionalFilterResolverImpl(expression, isExpressionResolve, false);
        }
        break;
      default:
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
            return new ConditionalFilterResolverImpl(expression, true, true);
          } else {
            return new RowLevelFilterResolverImpl(expression, false, false, tableIdentifier);
          }
        } else {
          return new RowLevelFilterResolverImpl(expression, false, false, tableIdentifier);
        }
    }
    return new RowLevelFilterResolverImpl(expression, false, false, tableIdentifier);
  }

}
