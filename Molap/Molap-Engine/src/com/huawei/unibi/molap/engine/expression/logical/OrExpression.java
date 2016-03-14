package com.huawei.unibi.molap.engine.expression.logical;

import com.huawei.unibi.molap.engine.expression.DataType;
import com.huawei.unibi.molap.engine.expression.Expression;
import com.huawei.unibi.molap.engine.expression.ExpressionResult;
import com.huawei.unibi.molap.engine.expression.exception.FilterUnsupportedException;
import com.huawei.unibi.molap.engine.molapfilterinterface.ExpressionType;
import com.huawei.unibi.molap.engine.molapfilterinterface.RowIntf;

public class OrExpression extends BinaryLogicalExpression
{

    private static final long serialVersionUID = 4220598043176438380L;

    public OrExpression(Expression left, Expression right)
    {
        super(left, right);
    }

    @Override
    public ExpressionResult evaluate(RowIntf value) throws FilterUnsupportedException
    {
        ExpressionResult resultLeft = left.evaluate(value);
        ExpressionResult resultRight = right.evaluate(value);
        switch(resultLeft.getDataType())
        {
        case BooleanType:
            resultLeft.set(DataType.BooleanType, (resultLeft.getBoolean() || resultRight.getBoolean()));
            break;
        default:
            throw new FilterUnsupportedException("Incompatible datatype for applying OR Expression Filter");
        }

        return resultLeft;
    }

    @Override
    public ExpressionType getFilterExpressionType()
    {
        return ExpressionType.OR;
    }

    @Override
    public String getString()
    {
        return "Or(" + left.getString() + ',' + right.getString() + ')';
    }

}
