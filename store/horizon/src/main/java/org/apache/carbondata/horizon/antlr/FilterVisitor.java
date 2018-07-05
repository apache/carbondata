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

package org.apache.carbondata.horizon.antlr;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.LiteralExpression;
import org.apache.carbondata.core.scan.expression.conditional.EqualToExpression;
import org.apache.carbondata.core.scan.expression.conditional.GreaterThanEqualToExpression;
import org.apache.carbondata.core.scan.expression.conditional.GreaterThanExpression;
import org.apache.carbondata.core.scan.expression.conditional.InExpression;
import org.apache.carbondata.core.scan.expression.conditional.LessThanEqualToExpression;
import org.apache.carbondata.core.scan.expression.conditional.LessThanExpression;
import org.apache.carbondata.core.scan.expression.conditional.ListExpression;
import org.apache.carbondata.core.scan.expression.conditional.NotEqualsExpression;
import org.apache.carbondata.core.scan.expression.conditional.NotInExpression;
import org.apache.carbondata.core.scan.expression.logical.AndExpression;
import org.apache.carbondata.core.scan.expression.logical.OrExpression;
import org.apache.carbondata.core.scan.expression.logical.RangeExpression;
import org.apache.carbondata.horizon.antlr.gen.SelectBaseVisitor;
import org.apache.carbondata.horizon.antlr.gen.SelectParser;

public class FilterVisitor extends SelectBaseVisitor<Expression> {

  private CarbonTable carbonTable;

  public FilterVisitor(CarbonTable carbonTable) {
    this.carbonTable = carbonTable;
  }

  public ColumnExpression getColumnExpression(String columnName) {
    return getColumnExpression(carbonTable.getTableName(), columnName);
  }

  public ColumnExpression getColumnExpression(String tableName, String columnName) {
    CarbonColumn column = carbonTable.getColumnByName(tableName, columnName);
    if (column == null) {
      throw new RuntimeException("column not exists: " + tableName + "." + columnName);
    }
    return new ColumnExpression(column.getColName(), column.getDataType());
  }

  @Override public Expression visitParseFilter(SelectParser.ParseFilterContext ctx) {
    return visitBooleanExpression(ctx.booleanExpression());
  }

  @Override public Expression visitBooleanExpression(SelectParser.BooleanExpressionContext ctx) {
    if (ctx.AND() != null) {
      return new AndExpression(visitBooleanExpression(ctx.left), visitBooleanExpression(ctx.right));
    } else if (ctx.OR() != null) {
      return new OrExpression(visitBooleanExpression(ctx.left), visitBooleanExpression(ctx.right));
    } else if (ctx.predicate() != null) {
      return visitPredicate(ctx.predicate());
    } else if (!ctx.booleanExpression().isEmpty()) {
      return visitBooleanExpression(ctx.booleanExpression().get(0));
    }

    return super.visitBooleanExpression(ctx);
  }

  @Override public Expression visitPredicate(SelectParser.PredicateContext ctx) {
    SelectParser.ComparisonOperatorContext comparision = ctx.comparisonOperator();
    if (comparision != null) {
      if (comparision.EQ() != null) {
        return new EqualToExpression(visit(ctx.left), visit(ctx.right));
      } else if (comparision.GT() != null) {
        return new GreaterThanExpression(visit(ctx.left), visit(ctx.right));
      } else if (comparision.GTE() != null) {
        return new GreaterThanEqualToExpression(visit(ctx.left), visit(ctx.right));
      } else if (comparision.LT() != null) {
        return new LessThanExpression(visit(ctx.left), visit(ctx.right));
      } else if (comparision.LTE() != null) {
        return new LessThanEqualToExpression(visit(ctx.left), visit(ctx.right));
      } else if (comparision.NEQ() != null) {
        return new NotEqualsExpression(visit(ctx.left), visit(ctx.right));
      }
    } else if (ctx.BETWEEN() != null) {
      if (ctx.NOT() != null) {
        return new RangeExpression(new GreaterThanExpression(visit(ctx.left), visit(ctx.upper)),
            new LessThanExpression(visit(ctx.left), visit(ctx.lower)));
      } else {
        return new RangeExpression(
            new GreaterThanEqualToExpression(visit(ctx.left), visit(ctx.lower)),
            new LessThanEqualToExpression(visit(ctx.left), visit(ctx.upper)));
      }
    } else if (ctx.IN() != null) {
      List<Expression> listExpression = new ArrayList<Expression>();
      List<SelectParser.PrimaryExpressionContext> primaryExpressionContexts =
          ctx.primaryExpression();
      for (SelectParser.PrimaryExpressionContext primary : primaryExpressionContexts) {
        if (ctx.left != primary) {
          listExpression.add(visit(primary));
        }
      }
      if (ctx.NOT() != null) {
        return new NotInExpression(visit(ctx.left), new ListExpression(listExpression));
      } else {
        return new InExpression(visit(ctx.left), new ListExpression(listExpression));
      }
    } else if (ctx.NULL() != null) {
      if (ctx.NOT() == null) {
        return new EqualToExpression(
            visit(ctx.left), new LiteralExpression(null, DataTypes.STRING), true);
      } else {
        return new NotEqualsExpression(
            visit(ctx.left), new LiteralExpression(null, DataTypes.STRING), true);
      }
    }
    return super.visitPredicate(ctx);
  }

  @Override public Expression visitNullLiteral(SelectParser.NullLiteralContext ctx) {
    return null;
  }

  @Override public Expression visitDecimalLiteral(SelectParser.DecimalLiteralContext ctx) {
    return new LiteralExpression(new BigDecimal(ctx.getText()),
        DataTypes.createDefaultDecimalType());
  }

  @Override public Expression visitIntegerLiteral(SelectParser.IntegerLiteralContext ctx) {
    return new LiteralExpression(Integer.parseInt(ctx.getText()), DataTypes.INT);
  }

  @Override public Expression visitBigIntLiteral(SelectParser.BigIntLiteralContext ctx) {
    return new LiteralExpression(Long.parseLong(ctx.getText()), DataTypes.LONG);
  }

  @Override public Expression visitSmallIntLiteral(SelectParser.SmallIntLiteralContext ctx) {
    return new LiteralExpression(Short.parseShort(ctx.getText()), DataTypes.SHORT);
  }

  @Override public Expression visitTinyIntLiteral(SelectParser.TinyIntLiteralContext ctx) {
    return new LiteralExpression(Short.parseShort(ctx.getText()), DataTypes.SHORT);
  }

  @Override public Expression visitDoubleLiteral(SelectParser.DoubleLiteralContext ctx) {
    return new LiteralExpression(Double.parseDouble(ctx.getText()), DataTypes.DOUBLE);
  }

  @Override public Expression visitBigDecimalLiteral(SelectParser.BigDecimalLiteralContext ctx) {
    return new LiteralExpression(new BigDecimal(ctx.getText()),
        DataTypes.createDefaultDecimalType());
  }

  @Override public Expression visitBooleanLiteral(SelectParser.BooleanLiteralContext ctx) {
    SelectParser.BooleanValueContext booleanValueContext = ctx.booleanValue();
    if (booleanValueContext.FALSE() != null) {
      return new LiteralExpression(false, DataTypes.BOOLEAN);
    } else {
      return new LiteralExpression(true, DataTypes.BOOLEAN);
    }
  }

  @Override public Expression visitStringLiteral(SelectParser.StringLiteralContext ctx) {
    return new LiteralExpression(ctx.getText(), DataTypes.STRING);
  }

  @Override public Expression visitUnquotedIdentifier(SelectParser.UnquotedIdentifierContext ctx) {
    return getColumnExpression(ctx.getText());
  }

  private String identifier(String identifier) {
    return identifier.replace("", "`");
  }

  @Override
  public Expression visitBackQuotedIdentifier(SelectParser.BackQuotedIdentifierContext ctx) {
    return getColumnExpression(identifier(ctx.getText()));
  }

  @Override public Expression visitDereference(SelectParser.DereferenceContext ctx) {
    String tableName = identifier(ctx.base.getText());
    String columnName = identifier(ctx.fieldName.getText());
    return getColumnExpression(tableName, columnName);
  }
}

