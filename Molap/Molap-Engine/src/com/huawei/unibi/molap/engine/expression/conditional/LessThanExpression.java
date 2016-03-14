package com.huawei.unibi.molap.engine.expression.conditional;

import com.huawei.unibi.molap.engine.expression.DataType;
import com.huawei.unibi.molap.engine.expression.Expression;
import com.huawei.unibi.molap.engine.expression.ExpressionResult;
import com.huawei.unibi.molap.engine.expression.exception.FilterUnsupportedException;
import com.huawei.unibi.molap.engine.molapfilterinterface.ExpressionType;
import com.huawei.unibi.molap.engine.molapfilterinterface.RowIntf;

public class LessThanExpression extends BinaryConditionalExpression
{

    private static final long serialVersionUID = 6343040416663699924L;

    public LessThanExpression(Expression left, Expression right)
    {
        super(left, right);
    }

    public ExpressionResult evaluate(RowIntf value) throws FilterUnsupportedException
    {
        ExpressionResult erRes = right.evaluate(value);
        ExpressionResult elRes = left.evaluate(value);

        ExpressionResult val1 = elRes;
        
        boolean result = false;
     
        if(elRes.isNull() || erRes.isNull()){
            elRes.set(DataType.BooleanType, false);
            return elRes;
        }
        if(elRes.getDataType() != erRes.getDataType())
        {
            if(elRes.getDataType().getPresedenceOrder() < erRes.getDataType().getPresedenceOrder())
            {
                val1 = erRes;
            }
            
        }
        switch(val1.getDataType())
        {
        case StringType:
            result = elRes.getString().compareTo(erRes.getString())<0;
            break;
        case IntegerType:
            result = elRes.getInt()<(erRes.getInt());
            break;
        case DoubleType:
            result = elRes.getDouble()<(erRes.getDouble());
            break;
        case TimestampType:
            result = elRes.getTime()<(erRes.getTime());
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
        return ExpressionType.LESSTHAN;
    }

    @Override
    public String getString()
    {
        return "LessThan(" + left.getString() + ',' + right.getString() + ')';
    }

}