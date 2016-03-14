package com.huawei.unibi.molap.engine.expression.conditional;

import com.huawei.unibi.molap.engine.expression.DataType;
import com.huawei.unibi.molap.engine.expression.Expression;
import com.huawei.unibi.molap.engine.expression.ExpressionResult;
import com.huawei.unibi.molap.engine.expression.exception.FilterUnsupportedException;
import com.huawei.unibi.molap.engine.molapfilterinterface.ExpressionType;
import com.huawei.unibi.molap.engine.molapfilterinterface.RowIntf;

public class GreaterThanEqualToExpression extends BinaryConditionalExpression
{
    private static final long serialVersionUID = 4185317066280688984L;

    public GreaterThanEqualToExpression(Expression left, Expression right)
    {
        super(left, right);
    }

    public ExpressionResult evaluate(RowIntf value) throws FilterUnsupportedException
    {
        ExpressionResult elRes = left.evaluate(value);
        ExpressionResult erRes = right.evaluate(value);
        ExpressionResult exprResVal1 = elRes;   
        if(elRes.isNull() || erRes.isNull()){
            elRes.set(DataType.BooleanType, false);
            return elRes;
        }
        if(elRes.getDataType() != erRes.getDataType())
        {
            if(elRes.getDataType().getPresedenceOrder() < erRes.getDataType().getPresedenceOrder())
            {
                exprResVal1 = erRes;
            }
            
        }
        boolean result = false;
        switch(exprResVal1.getDataType())
        {
        case StringType:
            result = elRes.getString().compareTo(erRes.getString())>=0;
            break;
        case IntegerType:
            result = elRes.getInt()>=(erRes.getInt());
            break;
        case DoubleType:
            result = elRes.getDouble()>=(erRes.getDouble());
            break;
        case TimestampType:
            result = elRes.getTime()>=(erRes.getTime());
            break;
        default:
            break;
        }
        exprResVal1.set(DataType.BooleanType, result);
        return exprResVal1;
    }

    @Override
    public ExpressionType getFilterExpressionType()
    {
        return ExpressionType.GREATERYHAN_EQUALTO;
    }

    @Override
    public String getString()
    {
        return "GreaterThanEqualTo(" + left.getString() + ',' + right.getString() + ')';
    }
}
