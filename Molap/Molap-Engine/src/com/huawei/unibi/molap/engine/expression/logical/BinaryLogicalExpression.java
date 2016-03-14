package com.huawei.unibi.molap.engine.expression.logical;



import com.huawei.unibi.molap.engine.expression.BinaryExpression;
import com.huawei.unibi.molap.engine.expression.Expression;



public abstract class  BinaryLogicalExpression extends BinaryExpression
{


    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public BinaryLogicalExpression(Expression left, Expression right)
    {
        super(left, right);
        // TODO Auto-generated constructor stub
    }

}
