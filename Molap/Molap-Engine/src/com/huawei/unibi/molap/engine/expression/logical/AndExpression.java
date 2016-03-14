package com.huawei.unibi.molap.engine.expression.logical;

import com.huawei.unibi.molap.engine.expression.DataType;
import com.huawei.unibi.molap.engine.expression.Expression;
import com.huawei.unibi.molap.engine.expression.ExpressionResult;
import com.huawei.unibi.molap.engine.expression.exception.FilterUnsupportedException;
import com.huawei.unibi.molap.engine.molapfilterinterface.ExpressionType;
import com.huawei.unibi.molap.engine.molapfilterinterface.RowIntf;

public class AndExpression extends BinaryLogicalExpression
{

    private static final long serialVersionUID = 1L;
    public AndExpression(Expression left, Expression right)
    {
        super(left, right);
        // TODO Auto-generated constructor stub
    }

    @Override
    public ExpressionResult evaluate(RowIntf value) throws FilterUnsupportedException
    {
        ExpressionResult resultLeft = left.evaluate(value);
        ExpressionResult resultRight = right.evaluate(value);
        switch(resultLeft.getDataType())
        {
        case BooleanType:
            resultLeft.set(DataType.BooleanType, (resultLeft.getBoolean() && resultRight.getBoolean()));
            break;
        default:
            throw new FilterUnsupportedException("Incompatible datatype for applying AND Expression Filter");
        }

        return resultLeft;
    }

    @Override
    public ExpressionType getFilterExpressionType()
    {
        // TODO Auto-generated method stub
        return ExpressionType.AND;
    }

    @Override
    public String getString()
    {
        // TODO Auto-generated method stub
        return "And("+left.getString()+','+right.getString()+')';
    }




}
