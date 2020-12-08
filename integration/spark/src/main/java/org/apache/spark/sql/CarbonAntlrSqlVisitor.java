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
import org.apache.spark.sql.parser.CarbonSqlBaseBaseVisitor;
import org.apache.spark.sql.parser.CarbonSqlBaseParser;
import org.apache.spark.util.SparkUtil;

public class CarbonAntlrSqlVisitor extends CarbonSqlBaseBaseVisitor {

  private final ParserInterface sparkParser;

  public CarbonAntlrSqlVisitor(ParserInterface sparkParser) {
    this.sparkParser = sparkParser;
  }

  @Override
  public String visitTableAlias(CarbonSqlBaseParser.TableAliasContext ctx) {
    if (null == ctx.children) {
      return null;
    }
    String res = ctx.getChild(1).getText();
    System.out.println(res);
    return res;
  }

  @Override
  public MergeAction visitAssignmentList(CarbonSqlBaseParser.AssignmentListContext ctx) {
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
        } catch (Exception ex) {
          // todo throw EX here
        }
        map.put(new Column(left), rightColumn);
      }
    }
    return new UpdateAction(SparkUtil.convertMap(map), false);
  }

  @Override
  public MergeAction visitMatchedAction(CarbonSqlBaseParser.MatchedActionContext ctx) {
    int childCount = ctx.getChildCount();
    if (childCount == 1) {
      // when matched ** delete
      return new DeleteAction();
    } else {
      if (ctx
          .getChild(ctx.getChildCount() - 1) instanceof CarbonSqlBaseParser.AssignmentListContext) {
        //UPDATE SET assignmentList
        return visitAssignmentList(
            (CarbonSqlBaseParser.AssignmentListContext) ctx.getChild(ctx.getChildCount() - 1));
      } else {
        //UPDATE SET *
        return new UpdateAction(null, true);
      }
    }
  }

  @Override
  public InsertAction visitNotMatchedAction(CarbonSqlBaseParser.NotMatchedActionContext ctx) {
    if (ctx.getChildCount() <= 2) {
      //INSERT *
      return InsertAction.apply(null, true);
    } else {
      // INSERT '(' columns=multipartIdentifierList ')'VALUES '(' expression (',' expression)* ')'
      // todo throw Exception here
      return InsertAction.apply(null, false);
    }
  }

  @Override
  public MergeAction visitNotMatchedClause(CarbonSqlBaseParser.NotMatchedClauseContext ctx) {
    int currIdx = 0;
    for (; currIdx < ctx.getChildCount(); currIdx++) {
      if (ctx.getChild(currIdx) instanceof CarbonSqlBaseParser.NotMatchedActionContext) {
        break;
      }
    }
    // Throw exception incase of not matched clause
    return visitNotMatchedAction(
        (CarbonSqlBaseParser.NotMatchedActionContext) ctx.getChild(currIdx));
  }

  @Override
  public MergeAction visitMatchedClause(CarbonSqlBaseParser.MatchedClauseContext ctx) {
    //There will be lots of childs at ctx,
    // we need to find the predicate
    int currIdx = 0;
    for (; currIdx < ctx.getChildCount(); currIdx++) {
      if (ctx.getChild(currIdx) instanceof CarbonSqlBaseParser.MatchedActionContext) {
        break;
      }
    }
    // Throw Exception in case of no Matched Action
    return visitMatchedAction((CarbonSqlBaseParser.MatchedActionContext) ctx.getChild(currIdx));
  }

  public boolean containsWhenMatchedPredicateExpression(int childCount) {
    return childCount > 4;
  }

  public boolean containsWhenNotMatchedPredicateExpression(int childCount) {
    return childCount > 5;
  }

  @Override
  public CarbonMergeIntoModel visitMergeInto(CarbonSqlBaseParser.MergeIntoContext ctx) {
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
        joinExpression =
            this.visitPredicated((CarbonSqlBaseParser.PredicatedContext) ctx.getChild(currIdx), "");
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
          e.printStackTrace();
        }
        mergeExpressions.add(whenMatchedExpression);
        mergeActions.add(visitMatchedAction(
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
          e.printStackTrace();
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
            // todo throw EX here
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
              // todo throw EX here
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

  @Override
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

  @Override
  public Object visitValueExpressionDefault(CarbonSqlBaseParser.ValueExpressionDefaultContext ctx) {
    ctx.getText();
    return super.visitValueExpressionDefault(ctx);
  }

  @Override
  public CarbonJoinExpression visitPredicated(CarbonSqlBaseParser.PredicatedContext ctx) {
    return visitComparison((CarbonSqlBaseParser.ComparisonContext) ctx.getChild(0));
  }

  public Expression visitPredicated(CarbonSqlBaseParser.PredicatedContext ctx, String type) {
    return visitComparison((CarbonSqlBaseParser.ComparisonContext) ctx.getChild(0), "");
  }

  @Override
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

  @Override
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

  @Override
  public String visitUnquotedIdentifier(CarbonSqlBaseParser.UnquotedIdentifierContext ctx) {
    String res = ctx.getChild(0).getText();
    System.out.println("ColName; " + res);
    return res;
  }

  @Override
  public String visitFromClause(CarbonSqlBaseParser.FromClauseContext ctx) {
    String tableName = visitRelation(ctx.relation(0));
    System.out.println("SQL table name: " + tableName);
    return tableName;
  }

  @Override
  public String visitRelation(CarbonSqlBaseParser.RelationContext ctx) {
    if (ctx.relationPrimary() instanceof CarbonSqlBaseParser.TableNameContext) {
      return (String) visitTableName((CarbonSqlBaseParser.TableNameContext) ctx.relationPrimary());
    }
    return "";
  }

  @Override
  public String visitComparisonOperator(CarbonSqlBaseParser.ComparisonOperatorContext ctx) {
    String res = ctx.getChild(0).getText();
    System.out.println("ComparisonOperator: " + res);
    return res;
  }

  @Override
  public String visitTableIdentifier(CarbonSqlBaseParser.TableIdentifierContext ctx) {
    return ctx.getChild(0).getText();
  }
}
