package com.huawei.unibi.molap.engine.expression.conditional;

import com.huawei.unibi.molap.engine.expression.DataType;
import com.huawei.unibi.molap.engine.expression.Expression;
import com.huawei.unibi.molap.engine.expression.ExpressionResult;
import com.huawei.unibi.molap.engine.expression.exception.FilterUnsupportedException;
import com.huawei.unibi.molap.engine.molapfilterinterface.ExpressionType;
import com.huawei.unibi.molap.engine.molapfilterinterface.RowIntf;

public class NotEqualsExpression extends BinaryConditionalExpression
{

    private static final long serialVersionUID = 8684006025540863973L;

    public NotEqualsExpression(Expression left, Expression right)
    {
        super(left, right);
    }

    @Override
    public ExpressionResult evaluate(RowIntf value) throws FilterUnsupportedException 
    {
        ExpressionResult elRes = left.evaluate(value);
        ExpressionResult erRes = right.evaluate(value);

        boolean result = false;
        ExpressionResult val1 = elRes;
        ExpressionResult val2 = erRes;
        
        if(elRes.isNull() || erRes.isNull()){
            result = elRes.isNull() != erRes.isNull();
            val1.set(DataType.BooleanType, result);
            return val1;
        }
        
        //default implementation if the data types are different for the resultsets
        if(elRes.getDataType() != erRes.getDataType()){
//            result = elRes.getString().equals(erRes.getString());
            if(elRes.getDataType().getPresedenceOrder() < erRes.getDataType().getPresedenceOrder())
            {
                val1 = erRes;
                val2 = elRes;
            }
        }
        switch(val1.getDataType())
        {
        case StringType:
            result = !val1.getString().equals(val2.getString());
            break;
        case IntegerType:
            result = val1.getInt().intValue() != val2.getInt().intValue();
            break;
        case DoubleType:
            result = val1.getDouble().doubleValue() != val2.getDouble().doubleValue();
            break;
        case TimestampType:
            result = val1.getTime().longValue() != val2.getTime().longValue();
            break;
        default:
            break;
        }
        val1.set(DataType.BooleanType, result);
        return val1;
    }

    @Override
    public ExpressionType getFilterExpressionType()
    {
        return ExpressionType.NOT_EQUALS;
    }

    @Override
    public String getString()
    {
        return "NotEquals(" + left.getString() + ',' + right.getString() + ')';
    }
}
