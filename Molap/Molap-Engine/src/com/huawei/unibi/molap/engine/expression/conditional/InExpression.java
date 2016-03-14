package com.huawei.unibi.molap.engine.expression.conditional;

import java.util.HashSet;
import java.util.Set;

import com.huawei.unibi.molap.engine.expression.DataType;
import com.huawei.unibi.molap.engine.expression.Expression;
import com.huawei.unibi.molap.engine.expression.ExpressionResult;
import com.huawei.unibi.molap.engine.expression.exception.FilterUnsupportedException;
import com.huawei.unibi.molap.engine.molapfilterinterface.ExpressionType;
import com.huawei.unibi.molap.engine.molapfilterinterface.RowIntf;

public class InExpression extends BinaryConditionalExpression
{
    private static final long serialVersionUID = -3149927446694175489L;
    
    protected transient Set<ExpressionResult> setOfExprResult;

    public InExpression(Expression left, Expression right)
    {
        super(left, right);
    }

    @Override
    public ExpressionResult evaluate(RowIntf value) throws FilterUnsupportedException
    {
        ExpressionResult leftRsult = left.evaluate(value);
        
        
        if(setOfExprResult == null)
        {
            ExpressionResult rightRsult = right.evaluate(value);
            ExpressionResult val =null;
            setOfExprResult = new HashSet<ExpressionResult>(10);
            for(ExpressionResult expressionResVal:rightRsult.getList()) 
            { 
                
                if(leftRsult.getDataType().name().equals(expressionResVal.getDataType().name()))
                {
                    if(expressionResVal.getDataType().getPresedenceOrder() < leftRsult.getDataType().getPresedenceOrder())
                    {
                        val=leftRsult;
                    }
                    else
                    {
                        val=expressionResVal; 
                    }
                     
                        switch(val.getDataType())
                        {
                        case StringType:
                            val=new ExpressionResult(val.getDataType(),expressionResVal.getString());
                            break;
                        case IntegerType:
                            val=new ExpressionResult(val.getDataType(),expressionResVal.getInt());
                            break;
                        case DoubleType:
                            val=new ExpressionResult(val.getDataType(),expressionResVal.getDouble());
                            break;
                        case TimestampType:
                            val=new ExpressionResult(val.getDataType(),expressionResVal.getTime());
                            break;
                        default:
                            break;
                        }
                    
        
                }
                setOfExprResult.add(val);
                
            }
        }
        leftRsult.set(DataType.BooleanType, setOfExprResult.contains(leftRsult));

        return leftRsult;
    }

    @Override
    public ExpressionType getFilterExpressionType()
    {
        return ExpressionType.IN;
    }

    @Override
    public String getString()
    {
       return "IN(" + left.getString() + ',' + right.getString() + ')';
    }

}
