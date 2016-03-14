package com.huawei.unibi.molap.engine.expression.conditional;

import com.huawei.unibi.molap.engine.expression.DataType;
import com.huawei.unibi.molap.engine.expression.Expression;
import com.huawei.unibi.molap.engine.expression.ExpressionResult;
import com.huawei.unibi.molap.engine.expression.exception.FilterUnsupportedException;
import com.huawei.unibi.molap.engine.molapfilterinterface.ExpressionType;
import com.huawei.unibi.molap.engine.molapfilterinterface.RowIntf;

public class EqualToExpression extends BinaryConditionalExpression
{

    private static final long serialVersionUID = 1L;
    public EqualToExpression(Expression left, Expression right)
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
            result = elRes.isNull() && erRes.isNull();
            val1.set(DataType.BooleanType, result);
            return val1;
        }
        //default implementation if the data types are different for the resultsets
        if(elRes.getDataType() != erRes.getDataType())
        {
            if(elRes.getDataType().getPresedenceOrder() < erRes.getDataType().getPresedenceOrder())
            {
                val2 = elRes;
                val1 = erRes;
            }
            
/*            result = elRes.getString().equals(erRes.getString());
            // if the string comparision doesnt match then convert data type.
            if(!result)
            {
                // check if the left side is double and right is integer or viceversa , in both cases convert int to double. 
                if(elRes.getDataType() == DataType.IntegerType &&  erRes.getDataType() == DataType.DoubleType 
                        ||elRes.getDataType() == DataType.DoubleType && erRes.getDataType() == DataType.IntegerType)
                {
                    elRes.set(DataType.BooleanType, elRes.getDouble() == erRes.getDouble());
                    return elRes;
                }
            }*/
        }
       
        // todo: move to util
        switch(val1.getDataType())
        {
        case StringType:
            result = val1.getString().equals(val2.getString());
            break;
        case IntegerType:
            result = val1.getInt().equals(val2.getInt());
            break;
        case DoubleType:
            result = val1.getDouble().equals(val2.getDouble());
            break;
        case TimestampType:
            result = val1.getTime().equals(val2.getTime());
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
        return ExpressionType.EQUALS;
    }

    @Override
    public String getString()
    {
        return "EqualTo(" + left.getString() + ',' + right.getString() + ')';
    }

}
