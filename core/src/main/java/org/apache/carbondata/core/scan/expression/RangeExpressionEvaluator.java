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

package org.apache.carbondata.core.scan.expression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.scan.expression.conditional.GreaterThanEqualToExpression;
import org.apache.carbondata.core.scan.expression.conditional.GreaterThanExpression;
import org.apache.carbondata.core.scan.expression.conditional.LessThanEqualToExpression;
import org.apache.carbondata.core.scan.expression.conditional.LessThanExpression;
import org.apache.carbondata.core.scan.expression.logical.AndExpression;
import org.apache.carbondata.core.scan.expression.logical.OrExpression;
import org.apache.carbondata.core.scan.expression.logical.RangeExpression;
import org.apache.carbondata.core.scan.expression.logical.TrueExpression;
import org.apache.carbondata.core.scan.filter.intf.ExpressionType;

import static org.apache.carbondata.core.scan.filter.intf.ExpressionType.FALSE;
import static org.apache.carbondata.core.scan.filter.intf.ExpressionType.GREATERTHAN;
import static org.apache.carbondata.core.scan.filter.intf.ExpressionType.GREATERTHAN_EQUALTO;
import static org.apache.carbondata.core.scan.filter.intf.ExpressionType.LESSTHAN;
import static org.apache.carbondata.core.scan.filter.intf.ExpressionType.LESSTHAN_EQUALTO;

public class RangeExpressionEvaluator {
  private Expression expr;
  private Expression srcNode;
  private Expression srcParentNode;
  private Expression tarNode;
  private Expression tarParentNode;

  public RangeExpressionEvaluator(Expression expr) {
    this.expr = expr;
  }

  public Expression getExpr() {
    return expr;
  }

  public void setExpr(Expression expr) {
    this.expr = expr;
  }

  public Expression getSrcNode() {
    return srcNode;
  }

  public void setTarNode(Expression expr) {
    this.tarNode = expr;
  }

  public void setTarParentNode(Expression expr) {
    this.tarParentNode = expr;
  }

  /**
   * This method evaluates is any greaterthan or less than expression can be transformed
   * into a single RANGE filter.
   */
  public void rangeExpressionEvaluatorMapBased() {
    // The algorithm :
    // Get all the nodes of the Expression Tree and fill it into a MAP.
    // The Map structure will be currentNode, ColumnName, LessThanOrgreaterThan, Value, ParentNode
    // Group the rows in MAP according to the columns and then evaluate if it can be transformed
    // into a RANGE or not.
    //
    //            AND                                           AND
    //             |                                             |
    //            / \                                           / \
    //           /   \                                         /   \
    //        Less   Greater         =>                    TRUE   Range
    //         / \    / \                                         / \
    //        /   \  /   \                                       /   \
    //       a    10 a   5                                   Less   greater
    //                                                        /\      /\
    //                                                       /  \    /  \
    //                                                      a   10  a    5
    //

    Map<String, List<FilterModificationNode>> filterExpressionMap;
    filterExpressionMap = convertFilterTreeToMap();
    replaceWithRangeExpression(filterExpressionMap);
    filterExpressionMap.clear();
  }

  public void replaceWithRangeExpression(
      Map<String, List<FilterModificationNode>> filterExpressionMap) {

    List<FilterModificationNode> deleteExp = new ArrayList<>();
    for (String colName : filterExpressionMap.keySet()) {
      // Check is there are multiple list for this Column.
      List<FilterModificationNode> filterExp = filterExpressionMap.get(colName);
      if (filterExp.size() > 1) {
        // There are multiple Expression for the same column traverse and check if they can
        // form a range.
        FilterModificationNode startMin = null;
        FilterModificationNode endMax = null;

        for (FilterModificationNode exp : filterExp) {
          if ((exp.getExpType() == GREATERTHAN) || (exp.getExpType() == GREATERTHAN_EQUALTO)) {
            if ((null == endMax) || ((null != endMax) && (checkLiteralValue(exp.getCurrentExp(),
                endMax.getCurrentExp())))) {
              if (null == startMin) {
                startMin = exp;
              } else {
                // There is already some value in startMin so check which one is greater.
                LiteralExpression srcLiteral = getChildLiteralExpression(startMin.getCurrentExp());
                LiteralExpression tarLiteral = getChildLiteralExpression(exp.getCurrentExp());

                ExpressionResult srcExpResult = srcLiteral.evaluate(null);
                ExpressionResult tarExpResult = tarLiteral.evaluate(null);
                if (srcExpResult.compareTo(tarExpResult) < 0) {
                  // Before replacing the startMin add the current StartMin into deleteExp List
                  // as they will be replaced with TRUE expression after RANGE formation.
                  deleteExp.add(startMin);
                  startMin = exp;
                }
              }
            }
          }
          if ((exp.getExpType() == LESSTHAN) || (exp.getExpType() == LESSTHAN_EQUALTO)) {
            if ((null == startMin) || ((null != startMin) && (checkLiteralValue(exp.getCurrentExp(),
                startMin.getCurrentExp())))) {
              if (null == endMax) {
                endMax = exp;
              } else {
                // There is already some value in endMax so check which one is less.
                LiteralExpression srcLiteral = getChildLiteralExpression(endMax.getCurrentExp());
                LiteralExpression tarLiteral = getChildLiteralExpression(exp.getCurrentExp());

                ExpressionResult srcExpResult = srcLiteral.evaluate(null);
                ExpressionResult tarExpResult = tarLiteral.evaluate(null);
                if (srcExpResult.compareTo(tarExpResult) > 0) {
                  // Before replacing the endMax add the current endMax into deleteExp List
                  // as they will be replaced with TRUE expression after RANGE formation.
                  deleteExp.add(endMax);
                  endMax = exp;
                }
              }
            }
          }
        }

        if ((null != startMin) && (null != endMax)) {
          // the node can be converted to RANGE.
          Expression n1 = startMin.getCurrentExp();
          Expression n2 = endMax.getCurrentExp();

          RangeExpression rangeTree = new RangeExpression(n1, n2);
          Expression srcParentNode = startMin.getParentExp();
          Expression tarParentNode = endMax.getParentExp();

          srcParentNode.findAndSetChild(startMin.getCurrentExp(), rangeTree);
          tarParentNode.findAndSetChild(endMax.getCurrentExp(), new TrueExpression(null));

          if (deleteExp.size() > 0) {
            // There are some expression to Delete as they are Redundant after Range Formation.
            for (FilterModificationNode trueExp : deleteExp) {
              trueExp.getParentExp()
                  .findAndSetChild(trueExp.getCurrentExp(), new TrueExpression(null));
            }
          }
        }
      }
    }
  }


  private Map<String, List<FilterModificationNode>> convertFilterTreeToMap() {
    // Traverse the Filter Tree and add the nodes in filterExpressionMap.
    // Only those nodes will be added which has got LessThan, LessThanEqualTo
    // GreaterThan, GreaterThanEqualTo Expression Only.
    Map<String, List<FilterModificationNode>> filterExpressionMap =
        new HashMap<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    // Traverse the Tree.
    fillExpressionMap(filterExpressionMap, null, null);

    return filterExpressionMap;
  }


  private void fillExpressionMap(Map<String, List<FilterModificationNode>> filterExpressionMap,
      Expression currentNode, Expression parentNode) {
    if (null == currentNode) {
      currentNode = this.getExpr();
      parentNode  = currentNode;
    }

    // if the parentNode is a ANDExpression and the current node is LessThan, GreaterThan
    // then add the node into filterExpressionMap.
    if ((parentNode instanceof AndExpression) && (isLessThanGreaterThanExp(currentNode)
        && eligibleForRangeExpConv(currentNode))) {
      addFilterExpressionMap(filterExpressionMap, currentNode, parentNode);
    }

    for (Expression exp : currentNode.getChildren()) {
      if (null != exp) {
        fillExpressionMap(filterExpressionMap, exp, currentNode);
        if (exp instanceof OrExpression) {
          replaceWithRangeExpression(filterExpressionMap);
          filterExpressionMap.clear();
        }
      }
    }
  }

  private void addFilterExpressionMap(Map<String, List<FilterModificationNode>> filterExpressionMap,
      Expression currentNode, Expression parentNode) {
    String colName = getColumnName(currentNode);
    DataType dataType = getLiteralDataType(currentNode);
    Object literalVal = getLiteralValue(currentNode);
    ExpressionType expType = getExpressionType(currentNode);

    FilterModificationNode filterExpression =
        new FilterModificationNode(currentNode, parentNode, expType, dataType, literalVal, colName);

    if (null == filterExpressionMap.get(colName)) {
      filterExpressionMap.put(colName, new ArrayList<FilterModificationNode>());
    }
    filterExpressionMap.get(colName).add(filterExpression);
  }

  /**
   * This method checks if the Expression is among LessThan, LessThanEqualTo,
   * GreaterThan or GreaterThanEqualTo
   *
   * @param expr
   * @return
   */
  private boolean isLessThanGreaterThanExp(Expression expr) {
    if ((expr instanceof LessThanEqualToExpression) || (expr instanceof LessThanExpression)
        || (expr instanceof GreaterThanEqualToExpression)
        || (expr instanceof GreaterThanExpression)) {
      return true;
    } else {
      return false;
    }
  }

  /**
   * This method verifies if the Expression is qualified for Range Expression conversion.
   *
   * @param expChild
   * @return
   */
  private boolean eligibleForRangeExpConv(Expression expChild) {
    for (Expression exp : expChild.getChildren()) {
      if (exp instanceof ColumnExpression) {
        if (((ColumnExpression) exp).isDimension() == false) {
          return false;
        }
        if ((((ColumnExpression) exp).getDimension().getDataType() == DataType.ARRAY) || (
            ((ColumnExpression) exp).getDimension().getDataType() == DataType.STRUCT)) {
          return false;
        } else {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * This method returns the Column name from the ColumnExpression ExpressionType.
   *
   * @param andNode
   * @return
   */
  private String getColumnName(Expression andNode) {
    // returns the Column Name from Column Expression.
    for (Expression exp : andNode.getChildren()) {
      if (exp instanceof ColumnExpression) {
        return ((ColumnExpression) exp).getColumnName();
      }
    }
    return null;
  }

  /**
   * This method returns the Value from the Literal ExpressionType.
   *
   * @param exp
   * @return
   */
  private Object getLiteralValue(Expression exp) {
    for (Expression expr : exp.getChildren()) {
      if (expr instanceof LiteralExpression) {
        return (((LiteralExpression) expr).getLiteralExpValue());
      }
    }
    return null;
  }

  /**
   * This method returns the DataType of the Literal Expression.
   *
   * @param exp
   * @return
   */
  private DataType getLiteralDataType(Expression exp) {
    for (Expression expr : exp.getChildren()) {
      if (expr instanceof LiteralExpression) {
        return (((LiteralExpression) expr).getLiteralExpDataType());
      }
    }
    return null;
  }

  /**
   * This method returns the DataType of the Literal Expression.
   *
   * @param exp
   * @return
   */
  private LiteralExpression getChildLiteralExpression(Expression exp) {
    for (Expression expr : exp.getChildren()) {
      if (expr instanceof LiteralExpression) {
        return ((LiteralExpression) expr);
      }
    }
    return null;
  }

  /**
   * This method returns the ExpressionType based on the Expression.
   *
   * @param exp
   * @return
   */
  private ExpressionType getExpressionType(Expression exp) {
    // return the expressionType. Note among the children of the
    // andNode one should be columnExpression others should be
    // LessThan, LessThanEqualTo, GreaterThan, GreaterThanEqualTo.
    //
    if (exp instanceof LessThanExpression) {
      return LESSTHAN;
    } else if (exp instanceof LessThanEqualToExpression) {
      return LESSTHAN_EQUALTO;
    } else if (exp instanceof GreaterThanExpression) {
      return GREATERTHAN;
    } else if (exp instanceof GreaterThanEqualToExpression) {
      return GREATERTHAN_EQUALTO;
    } else {
      return FALSE;
    }
  }

  /**
   * This method checks if the Source Expression matches with Target Expression.
   *
   * @param src
   * @param tar
   * @return
   */
  private boolean matchExpType(ExpressionType src, ExpressionType tar) {
    if ((((src == LESSTHAN) || (src == LESSTHAN_EQUALTO)) && ((tar == GREATERTHAN) || (tar
        == GREATERTHAN_EQUALTO))) || (((src == GREATERTHAN) || (src == GREATERTHAN_EQUALTO)) && (
        (tar == LESSTHAN) || (tar == LESSTHAN_EQUALTO)))) {
      return true;
    } else {
      return false;
    }
  }

  /**
   * This Method Traverses the Expression Tree to find the corresponding node of the Range
   * Expression. If one node of Range Expression is LessThan then a corresponding GreaterThan
   * will be choosen or vice versa.
   *
   * @param currentNode
   * @param parentNode
   * @return
   */
  private Expression traverseTree(Expression currentNode, Expression parentNode) {
    Expression result = null;

    if (null == parentNode) {
      currentNode = this.getExpr();
      parentNode = currentNode;
    }

    if (!this.getSrcNode().equals(currentNode) && isLessThanGreaterThanExp(currentNode)) {
      String srcColumnName = getColumnName(this.getSrcNode());
      String tarColumnName = getColumnName(currentNode);
      ExpressionType srcExpType = getExpressionType(this.getSrcNode());
      ExpressionType tarExpType = getExpressionType(currentNode);

      if ((null != srcColumnName) && (null != tarColumnName) && (srcColumnName == tarColumnName)
          && (srcExpType != ExpressionType.FALSE) && (tarExpType != ExpressionType.FALSE) && (
          (matchExpType(srcExpType, tarExpType)) && checkLiteralValue(this.getSrcNode(),
              currentNode))) {
        this.setTarNode(currentNode);
        this.setTarParentNode(parentNode);
        return parentNode;
      }
    }

    for (Expression exp : currentNode.getChildren()) {
      if (null != exp && !(exp instanceof RangeExpression)) {
        result = traverseTree(exp, currentNode);
        if (null != result) {
          return result;
        }
      }
    }
    return null;
  }

  /**
   * This method will check if the literal values attached to GreaterThan of GreaterThanEqualTo
   * is less or equal to LessThan and LessThanEqualTo literal.
   *
   * @param src
   * @param tar
   * @return
   */
  private boolean checkLiteralValue(Expression src, Expression tar) {
    ExpressionType srcExpressionType = getExpressionType(src);
    ExpressionType tarExpressionType = getExpressionType(tar);
    LiteralExpression srcLiteral = getChildLiteralExpression(src);
    LiteralExpression tarLiteral = getChildLiteralExpression(tar);

    ExpressionResult srcExpResult = srcLiteral.evaluate(null);
    ExpressionResult tarExpResult = tarLiteral.evaluate(null);

    switch (srcExpressionType) {
      case LESSTHAN:
      case LESSTHAN_EQUALTO:
        switch (tarExpressionType) {
          case GREATERTHAN:
            if (srcExpResult.compareTo(tarExpResult) > 0) {
              return true;
            }
            break;
          case GREATERTHAN_EQUALTO:
            if (srcExpResult.compareTo(tarExpResult) >= 0) {
              return true;
            }
            break;
        }
        break;
      case GREATERTHAN:
      case GREATERTHAN_EQUALTO:
        switch (tarExpressionType) {
          case LESSTHAN:
            if (srcExpResult.compareTo(tarExpResult) < 0) {
              return true;
            }
            break;
          case LESSTHAN_EQUALTO:
            if (srcExpResult.compareTo(tarExpResult) <= 0) {
              return true;
            }
            break;
        }
        break;
    }

    return false;
  }
}
