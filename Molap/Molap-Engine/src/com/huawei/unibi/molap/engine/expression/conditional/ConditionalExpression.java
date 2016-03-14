package com.huawei.unibi.molap.engine.expression.conditional;

import java.util.List;

import com.huawei.unibi.molap.engine.expression.ColumnExpression;

public interface ConditionalExpression
{

    // Will get the column informations involved in the expressions by
    // traversing the tree
    List<ColumnExpression> getColumnList();

    boolean isSingleDimension();
}
