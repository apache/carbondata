package com.huawei.unibi.molap.engine.expression.arithmetic;

import com.huawei.unibi.molap.engine.expression.BinaryExpression;
import com.huawei.unibi.molap.engine.expression.Expression;

public abstract class BinaryArithmeticExpression extends BinaryExpression
{

    private static final long serialVersionUID = 1L;
    public BinaryArithmeticExpression(Expression left, Expression right)
    {
        super(left, right);
        // TODO Auto-generated constructor stub
    }

}
