package com.huawei.unibi.molap.engine.expression.conditional;

import com.huawei.unibi.molap.engine.expression.DataType;
import com.huawei.unibi.molap.engine.expression.Expression;
import com.huawei.unibi.molap.engine.expression.ExpressionResult;
import com.huawei.unibi.molap.engine.expression.exception.FilterUnsupportedException;
import com.huawei.unibi.molap.engine.molapfilterinterface.ExpressionType;
import com.huawei.unibi.molap.engine.molapfilterinterface.RowIntf;

public class LessThanEqualToExpression extends BinaryConditionalExpression
{
    private static final long serialVersionUID = 1L;
    public LessThanEqualToExpression(Expression left, Expression right)
    {
        super(left, right);
        // TODO Auto-generated constructor stub
    }

    public ExpressionResult evaluate(RowIntf value) throws FilterUnsupportedException
    {
        ExpressionResult elRes = left.evaluate(value);
        ExpressionResult erRes = right.evaluate(value);
        ExpressionResult exprResValue1 = elRes;
        if(elRes.isNull() || erRes.isNull()){
            elRes.set(DataType.BooleanType, false);
            return elRes;
        }
        if(elRes.getDataType() != erRes.getDataType())
        {
            if(elRes.getDataType().getPresedenceOrder() < erRes.getDataType().getPresedenceOrder())
            {
                exprResValue1 = erRes;
            }
            
        }
        boolean result = false;
        switch(exprResValue1.getDataType())
        {
        case StringType:
            result = elRes.getString().compareTo(erRes.getString())<=0;
            break;
        case IntegerType:
            result = elRes.getInt()<=(erRes.getInt());
            break;
        case DoubleType:
            result = elRes.getDouble()<=(erRes.getDouble());
            break;
        case TimestampType:
            result = elRes.getTime()<=(erRes.getTime());
            break;
        default:
            break;
        }
        exprResValue1.set(DataType.BooleanType, result);
        return exprResValue1;
    }

    @Override
    public ExpressionType getFilterExpressionType()
    {
        // TODO Auto-generated method stub
        return ExpressionType.LESSTHAN_EQUALTO;
    }

    @Override
    public String getString()
    {
        return "LessThanEqualTo(" + left.getString() + ',' + right.getString() + ')';
    }

}
