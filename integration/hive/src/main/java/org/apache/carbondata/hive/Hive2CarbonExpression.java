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

package org.apache.carbondata.hive;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.metadata.datatype.DataType;
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
import org.apache.carbondata.core.scan.expression.logical.AndExpression;
import org.apache.carbondata.core.scan.expression.logical.OrExpression;
import org.apache.carbondata.hadoop.api.CarbonInputFormat;
import org.apache.carbondata.hive.util.DataTypeUtil;

import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeFieldDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIn;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.apache.log4j.Logger;

/**
 * @description: hive expression to carbon expression
 */
public class Hive2CarbonExpression {
  public static final int left = 0;
  public static final int right = 1;
  private static final Logger LOG =
      LogServiceFactory.getLogService(CarbonInputFormat.class.getName());

  public static Expression convertExprHive2Carbon(ExprNodeDesc exprNodeDesc) {
    if (exprNodeDesc instanceof ExprNodeGenericFuncDesc) {
      ExprNodeGenericFuncDesc exprNodeGenericFuncDesc = (ExprNodeGenericFuncDesc) exprNodeDesc;
      GenericUDF udf = exprNodeGenericFuncDesc.getGenericUDF();
      List<ExprNodeDesc> l1 = exprNodeGenericFuncDesc.getChildren();
      if (udf instanceof GenericUDFIn) {
        ColumnExpression columnExpression = new ColumnExpression(l1.get(left).getCols().get(left),
            getDateType(l1.get(left).getTypeString()));
        List<Expression> listExpr = new ArrayList<>();
        for (int i = right; i < l1.size(); i++) {
          LiteralExpression literalExpression = new LiteralExpression(l1.get(i).getExprString(),
              getDateType(l1.get(left).getTypeString()));
          listExpr.add(literalExpression);
        }
        ListExpression listExpression = new ListExpression(listExpr);
        return new InExpression(columnExpression, listExpression);
      } else if (udf instanceof GenericUDFOPOr) {
        Expression leftExpression =
            convertExprHive2Carbon(exprNodeGenericFuncDesc.getChildren().get(left));
        Expression rightExpression =
            convertExprHive2Carbon(exprNodeGenericFuncDesc.getChildren().get(right));
        return new OrExpression(leftExpression, rightExpression);
      } else if (udf instanceof GenericUDFOPAnd) {
        Expression leftExpression =
            convertExprHive2Carbon(exprNodeGenericFuncDesc.getChildren().get(left));
        Expression rightExpression =
            convertExprHive2Carbon(exprNodeGenericFuncDesc.getChildren().get(right));
        return new AndExpression(leftExpression, rightExpression);

      } else if (udf instanceof GenericUDFOPEqual) {
        ColumnExpression columnExpression = null;
        if (l1.get(left) instanceof ExprNodeFieldDesc) {
          throw new UnsupportedOperationException("Complex types are not supported for PPD");
        } else {
          columnExpression = new ColumnExpression(l1.get(left).getCols().get(left),
              getDateType(l1.get(left).getTypeString()));
        }
        LiteralExpression literalExpression =
            new LiteralExpression(l1.get(right).getExprString().replace("'", ""),
                getDateType(l1.get(right).getTypeString()));
        return new EqualToExpression(columnExpression, literalExpression);
      } else if (udf instanceof GenericUDFOPEqualOrGreaterThan) {
        ColumnExpression columnExpression = new ColumnExpression(l1.get(left).getCols().get(left),
            getDateType(l1.get(left).getTypeString()));
        LiteralExpression literalExpression = new LiteralExpression(l1.get(right).getExprString(),
            getDateType(l1.get(left).getTypeString()));
        return new GreaterThanEqualToExpression(columnExpression, literalExpression);
      } else if (udf instanceof GenericUDFOPGreaterThan) {
        ColumnExpression columnExpression = new ColumnExpression(l1.get(left).getCols().get(left),
            getDateType(l1.get(left).getTypeString()));
        LiteralExpression literalExpression = new LiteralExpression(l1.get(right).getExprString(),
            getDateType(l1.get(left).getTypeString()));
        return new GreaterThanExpression(columnExpression, literalExpression);
      } else if (udf instanceof GenericUDFOPNotEqual) {
        ColumnExpression columnExpression = new ColumnExpression(l1.get(left).getCols().get(left),
            getDateType(l1.get(left).getTypeString()));
        LiteralExpression literalExpression = new LiteralExpression(l1.get(right).getExprString(),
            getDateType(l1.get(left).getTypeString()));
        return new NotEqualsExpression(columnExpression, literalExpression);
      } else if (udf instanceof GenericUDFOPEqualOrLessThan) {
        ColumnExpression columnExpression = new ColumnExpression(l1.get(left).getCols().get(left),
            getDateType(l1.get(left).getTypeString()));
        LiteralExpression literalExpression = new LiteralExpression(l1.get(right).getExprString(),
            getDateType(l1.get(left).getTypeString()));
        return new LessThanEqualToExpression(columnExpression, literalExpression);
      } else if (udf instanceof GenericUDFOPLessThan) {
        ColumnExpression columnExpression = new ColumnExpression(l1.get(left).getCols().get(left),
            getDateType(l1.get(left).getTypeString()));
        LiteralExpression literalExpression = new LiteralExpression(l1.get(right).getExprString(),
            getDateType(l1.get(left).getTypeString()));
        return new LessThanExpression(columnExpression, literalExpression);
      } else if (udf instanceof GenericUDFOPNull) {
        ColumnExpression columnExpression = new ColumnExpression(l1.get(left).getCols().get(left),
            getDateType(l1.get(left).getTypeString()));
        LiteralExpression literalExpression = new LiteralExpression(null, null);
        return new EqualToExpression(columnExpression, literalExpression, true);
      } else if (udf instanceof GenericUDFOPNotNull) {
        ColumnExpression columnExpression = new ColumnExpression(l1.get(left).getCols().get(left),
            getDateType(l1.get(left).getTypeString()));
        LiteralExpression literalExpression = new LiteralExpression(null, null);
        return new NotEqualsExpression(columnExpression, literalExpression, true);
      } else {
        LOG.error("error:not find type" + udf.toString());
      }
    }
    return null;
  }

  public static DataType getDateType(String type) {
    try {
      return DataTypeUtil.convertHiveTypeToCarbon(type);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}

