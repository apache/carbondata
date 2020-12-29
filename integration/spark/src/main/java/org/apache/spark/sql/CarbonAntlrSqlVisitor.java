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

package org.apache.spark.sql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException;

import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.parser.ParserInterface;
import org.apache.spark.sql.execution.command.mutation.merge.DeleteAction;
import org.apache.spark.sql.execution.command.mutation.merge.InsertAction;
import org.apache.spark.sql.execution.command.mutation.merge.MergeAction;
import org.apache.spark.sql.execution.command.mutation.merge.UpdateAction;
import org.apache.spark.sql.merge.model.CarbonJoinExpression;
import org.apache.spark.sql.merge.model.CarbonMergeIntoModel;
import org.apache.spark.sql.merge.model.ColumnModel;
import org.apache.spark.sql.merge.model.TableModel;
import org.apache.spark.sql.parser.CarbonSqlBaseParser;
import org.apache.spark.util.SparkUtil;

public class CarbonAntlrSqlVisitor {

  private final ParserInterface sparkParser;

  public CarbonAntlrSqlVisitor(ParserInterface sparkParser) {
    this.sparkParser = sparkParser;
  }

  public String visitTableAlias(CarbonSqlBaseParser.TableAliasContext ctx) {
    if (null == ctx.children) {
      return null;
    }
    String res = ctx.getChild(1).getText();
    return res;
  }

  public MergeAction visitCarbonAssignmentList(CarbonSqlBaseParser.AssignmentListContext ctx)
          throws MalformedCarbonCommandException {
    //  UPDATE SET assignmentList
    Map<Column, Column> map = new HashMap<>();
    for (int currIdx = 0; currIdx < ctx.getChildCount(); currIdx++) {
      if (ctx.getChild(currIdx) instanceof CarbonSqlBaseParser.AssignmentContext) {
        //Assume the actions are all use to pass value
        String left = ctx.getChild(currIdx).getChild(0).getText();
        if (left.split("\\.").length > 1) {
          left = left.split("\\.")[1];
        }
        String right = ctx.getChild(currIdx).getChild(2).getText();
        Column rightColumn = null;
        try {
          Expression expression = sparkParser.parseExpression(right);
          rightColumn = new Column(expression);
        } catch (Exception e) {
          throw new MalformedCarbonCommandException("Parse failed: " + e.getMessage());
        }
        map.put(new Column(left), rightColumn);
      }
    }
    return new UpdateAction(SparkUtil.convertMap(map), false);
  }

  public MergeAction visitCarbonMatchedAction(CarbonSqlBaseParser.MatchedActionContext ctx)
          throws MalformedCarbonCommandException {
    int childCount = ctx.getChildCount();
    if (childCount == 1) {
      // when matched ** delete
      return new DeleteAction();
    } else {
      if (ctx.getChild(ctx.getChildCount() - 1) instanceof
              CarbonSqlBaseParser.AssignmentListContext) {
        //UPDATE SET assignmentList
        return visitCarbonAssignmentList(
            (CarbonSqlBaseParser.AssignmentListContext) ctx.getChild(ctx.getChildCount() - 1));
      } else {
        //UPDATE SET *
        return new UpdateAction(null, true);
      }
    }
  }

  public InsertAction visitCarbonNotMatchedAction(CarbonSqlBaseParser.NotMatchedActionContext ctx) {
    if (ctx.getChildCount() <= 2) {
      //INSERT *
      return InsertAction.apply(null, true);
    } else {
      return InsertAction.apply(null, false);
    }
  }

  public MergeAction visitCarbonNotMatchedClause(CarbonSqlBaseParser.NotMatchedClauseContext ctx) {
    int currIdx = 0;
    for (; currIdx < ctx.getChildCount(); currIdx++) {
      if (ctx.getChild(currIdx) instanceof CarbonSqlBaseParser.NotMatchedActionContext) {
        break;
      }
    }
    return visitCarbonNotMatchedAction(
        (CarbonSqlBaseParser.NotMatchedActionContext) ctx.getChild(currIdx));
  }

  public MergeAction visitCarbonMatchedClause(CarbonSqlBaseParser.MatchedClauseContext ctx)
          throws MalformedCarbonCommandException {
    //There will be lots of childs at ctx,
    // we need to find the predicate
    int currIdx = 0;
    for (; currIdx < ctx.getChildCount(); currIdx++) {
      if (ctx.getChild(currIdx) instanceof CarbonSqlBaseParser.MatchedActionContext) {
        break;
      }
    }
    // Throw Exception in case of no Matched Action
    return visitCarbonMatchedAction(
            (CarbonSqlBaseParser.MatchedActionContext) ctx.getChild(currIdx));
  }

  public boolean containsWhenMatchedPredicateExpression(int childCount) {
    return childCount > 4;
  }

  public boolean containsWhenNotMatchedPredicateExpression(int childCount) {
    return childCount > 5;
  }

  public CarbonMergeIntoModel visitMergeIntoCarbonTable(CarbonSqlBaseParser.MergeIntoContext ctx)
          throws MalformedCarbonCommandException {
    // handle the exception msg from base parser
    if (ctx.exception != null) {
      throw new MalformedCarbonCommandException("Parse failed!");
    }
    TableModel targetTable = visitMultipartIdentifier(ctx.target);
    TableModel sourceTable = visitMultipartIdentifier(ctx.source);

    //Once get these two table,
    //We can try to get CarbonTable

    //Build a matched clause list to store the when matched and when not matched clause
    int size = ctx.getChildCount();
    int currIdx = 0;
    Expression joinExpression = null;
    List<Expression> mergeExpressions = new ArrayList<>();
    List<MergeAction> mergeActions = new ArrayList<>();

    // There should be two List to store the result retrieve from
    // when matched / when not matched context
    while (currIdx < size) {
      if (ctx.getChild(currIdx) instanceof CarbonSqlBaseParser.PredicatedContext) {
        //This branch will visit the Join Expression
        ctx.getChild(currIdx).getChildCount();
        joinExpression = this.visitCarbonPredicated(
                (CarbonSqlBaseParser.PredicatedContext) ctx.getChild(currIdx));
      } else if (ctx.getChild(currIdx) instanceof CarbonSqlBaseParser.MatchedClauseContext) {
        //This branch will deal with the Matched Clause
        Expression whenMatchedExpression = null;
        //Get the whenMatched expression
        try {
          if (this.containsWhenMatchedPredicateExpression(ctx.getChild(currIdx).getChildCount())) {
            whenMatchedExpression = sparkParser.parseExpression(
                ((CarbonSqlBaseParser.MatchedClauseContext) ctx.getChild(currIdx))
                    .booleanExpression().getText());
          }
        } catch (ParseException e) {
          throw new MalformedCarbonCommandException("Parse failed: " + e.getMessage());
        }
        mergeExpressions.add(whenMatchedExpression);
        mergeActions.add(visitCarbonMatchedAction(
            (CarbonSqlBaseParser.MatchedActionContext) ctx.getChild(currIdx)
                .getChild(ctx.getChild(currIdx).getChildCount() - 1)));
      } else if (ctx.getChild(currIdx) instanceof CarbonSqlBaseParser.NotMatchedClauseContext) {
        //This branch will deal with the Matched Clause
        Expression whenNotMatchedExpression = null;
        //Get the whenMatched expression
        try {
          if (this
              .containsWhenNotMatchedPredicateExpression(ctx.getChild(currIdx).getChildCount())) {
            whenNotMatchedExpression = sparkParser.parseExpression(
                ((CarbonSqlBaseParser.NotMatchedClauseContext) ctx.getChild(currIdx))
                    .booleanExpression().getText());
          }
        } catch (ParseException e) {
          throw new MalformedCarbonCommandException("Parse failed: " + e.getMessage());
        }
        mergeExpressions.add(whenNotMatchedExpression);
        CarbonSqlBaseParser.NotMatchedActionContext notMatchedActionContext =
                (CarbonSqlBaseParser.NotMatchedActionContext) ctx.getChild(currIdx)
                        .getChild(ctx.getChild(currIdx).getChildCount() - 1);
        if (notMatchedActionContext.getChildCount() <= 2) {
          mergeActions.add(InsertAction.apply(null, true));
        } else if (notMatchedActionContext.ASTERISK() == null) {
          if (notMatchedActionContext.columns.multipartIdentifier().size() !=
                  notMatchedActionContext.expression().size()) {
            throw new MalformedCarbonCommandException("Parse failed: size of columns " +
                    "is not equal to size of expression in not matched action.");
          }
          Map<Column, Column> insertMap = new HashMap<>();
          for (int i = 0; i < notMatchedActionContext.columns.multipartIdentifier().size(); i++) {
            String left = visitMultipartIdentifier(
                            notMatchedActionContext.columns.multipartIdentifier().get(i), "")
                            .getColName();
            String right = notMatchedActionContext.expression().get(i).getText();
            // some times the right side is literal or expression, not table column
            // so we need to check the left side is a column or expression
            Column rightColumn = null;
            try {
              Expression expression = sparkParser.parseExpression(right);
              rightColumn = new Column(expression);
            } catch (Exception ex) {
              throw new MalformedCarbonCommandException("Parse failed: " + ex.getMessage());
            }
            insertMap.put(new Column(left), rightColumn);
          }
          mergeActions.add(InsertAction.apply(SparkUtil.convertMap(insertMap), false));
        } else {
          mergeActions.add(InsertAction.apply(null, false));
        }
      }
      currIdx++;
    }
    return new CarbonMergeIntoModel(targetTable, sourceTable, joinExpression,
            mergeExpressions, mergeActions);
  }

  public CarbonJoinExpression visitComparison(CarbonSqlBaseParser.ComparisonContext ctx) {
    // we need to get left Expression and Right Expression
    // Even get the table name and col name
    ctx.getText();
    String t1Name = ctx.left.getChild(0).getChild(0).getText();
    String c1Name = ctx.left.getChild(0).getChild(2).getText();
    String t2Name = ctx.right.getChild(0).getChild(0).getText();
    String c2Name = ctx.right.getChild(0).getChild(2).getText();
    return new CarbonJoinExpression(t1Name, c1Name, t2Name, c2Name);
  }

  public Expression visitComparison(CarbonSqlBaseParser.ComparisonContext ctx, String x) {
    Expression expression = null;
    try {
      expression = sparkParser.parseExpression(ctx.getText());
    } catch (ParseException e) {
      e.printStackTrace();
    }
    return expression;
  }

  public CarbonJoinExpression visitPredicated(CarbonSqlBaseParser.PredicatedContext ctx) {
    return visitComparison((CarbonSqlBaseParser.ComparisonContext) ctx.getChild(0));
  }

  public Expression visitCarbonPredicated(CarbonSqlBaseParser.PredicatedContext ctx) {
    return visitComparison((CarbonSqlBaseParser.ComparisonContext) ctx.getChild(0), "");
  }

  public ColumnModel visitDereference(CarbonSqlBaseParser.DereferenceContext ctx) {
    // In this part, it will return two colunm name
    int count = ctx.getChildCount();
    ColumnModel col = new ColumnModel();
    if (count == 3) {
      String tableName = ctx.getChild(0).getText();
      String colName = ctx.getChild(2).getText();
      col = new ColumnModel(tableName, colName);
    }
    return col;
  }

  public TableModel visitMultipartIdentifier(CarbonSqlBaseParser.MultipartIdentifierContext ctx) {
    TableModel table = new TableModel();
    List<CarbonSqlBaseParser.ErrorCapturingIdentifierContext> parts = ctx.parts;
    if (parts.size() == 2) {
      table.setDatabase(parts.get(0).getText());
      table.setTable(parts.get(1).getText());
    }
    if (parts.size() == 1) {
      table.setTable(parts.get(0).getText());
    }
    return table;
  }

  public ColumnModel visitMultipartIdentifier(CarbonSqlBaseParser.MultipartIdentifierContext ctx,
                                              String x) {
    ColumnModel column = new ColumnModel();
    List<CarbonSqlBaseParser.ErrorCapturingIdentifierContext> parts = ctx.parts;
    if (parts.size() == 2) {
      column.setTable(parts.get(0).getText());
      column.setColName(parts.get(1).getText());
    }
    if (parts.size() == 1) {
      column.setColName(parts.get(0).getText());
    }
    return column;
  }

  public String visitUnquotedIdentifier(CarbonSqlBaseParser.UnquotedIdentifierContext ctx) {
    String res = ctx.getChild(0).getText();
    return res;
  }

  public String visitComparisonOperator(CarbonSqlBaseParser.ComparisonOperatorContext ctx) {
    String res = ctx.getChild(0).getText();
    return res;
  }

  public String visitTableIdentifier(CarbonSqlBaseParser.TableIdentifierContext ctx) {
    return ctx.getChild(0).getText();
  }
}
