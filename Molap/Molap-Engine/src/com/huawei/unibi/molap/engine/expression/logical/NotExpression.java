package com.huawei.unibi.molap.engine.expression.logical;

import com.huawei.unibi.molap.engine.expression.DataType;
import com.huawei.unibi.molap.engine.expression.Expression;
import com.huawei.unibi.molap.engine.expression.ExpressionResult;
import com.huawei.unibi.molap.engine.expression.UnaryExpression;
import com.huawei.unibi.molap.engine.expression.exception.FilterUnsupportedException;
import com.huawei.unibi.molap.engine.molapfilterinterface.ExpressionType;
import com.huawei.unibi.molap.engine.molapfilterinterface.RowIntf;

public class NotExpression extends UnaryExpression
{
    private static final long serialVersionUID = 1L;
    public NotExpression(Expression child)
    {
        super(child);
    }

    @Override
    public ExpressionResult evaluate(RowIntf value) throws FilterUnsupportedException
    {
        ExpressionResult expResult=child.evaluate(value);
        expResult.set(DataType.BooleanType, !(expResult.getBoolean()));
        switch(expResult.getDataType())
        {
        case BooleanType:
            expResult.set(DataType.BooleanType, !(expResult.getBoolean()));
            break;
        default:
            throw new FilterUnsupportedException("Incompatible datatype for applying NOT Expression Filter");
        }
        return expResult;
    }

    @Override
    public ExpressionType getFilterExpressionType()
    {
        return ExpressionType.NOT;
    }
    
    @Override
    public String getString()
    {
        return "Not(" + child.getString() + ')';
    }
}
