/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.huawei.unibi.molap.engine.expression.conditional;

import java.util.HashSet;
import java.util.Set;

import com.huawei.unibi.molap.engine.expression.DataType;
import com.huawei.unibi.molap.engine.expression.Expression;
import com.huawei.unibi.molap.engine.expression.ExpressionResult;
import com.huawei.unibi.molap.engine.expression.exception.FilterUnsupportedException;
import com.huawei.unibi.molap.engine.molapfilterinterface.ExpressionType;
import com.huawei.unibi.molap.engine.molapfilterinterface.RowIntf;

public class NotInExpression extends BinaryConditionalExpression
{
    private static final long serialVersionUID = -6835841923752118034L;
    protected transient Set<ExpressionResult> setOfExprResult;

    public NotInExpression(Expression left, Expression right)
    {
        super(left, right);
    }

    @Override
    public ExpressionResult evaluate(RowIntf value) throws FilterUnsupportedException
    {
        ExpressionResult leftRsult = left.evaluate(value);
        
        
        if(setOfExprResult == null)
        {
            ExpressionResult val =null;

            ExpressionResult rightRsult = right.evaluate(value);
            setOfExprResult = new HashSet<ExpressionResult>(10);
            for(ExpressionResult exprResVal:rightRsult.getList()) 
            { 
                
                if(leftRsult.getDataType().name().equals(exprResVal.getDataType().name()))
                {
                    if(exprResVal.getDataType().getPresedenceOrder() < leftRsult.getDataType().getPresedenceOrder())
                    {
                        val=leftRsult;
                    }
                    else
                    {
                        val=exprResVal; 
                    }
                     
                        switch(val.getDataType())
                        {
                        case StringType:
                            val=new ExpressionResult(val.getDataType(),exprResVal.getString());
                            break;
                        case IntegerType:
                            val=new ExpressionResult(val.getDataType(),exprResVal.getInt());
                            break;
                        case DoubleType:
                            val=new ExpressionResult(val.getDataType(),exprResVal.getDouble());
                            break;
                        case TimestampType:
                            val=new ExpressionResult(val.getDataType(),exprResVal.getTime());
                            break;
                        default:
                            break;
                        }
                    
        
                }
                setOfExprResult.add(val);
                
            }
        }
        leftRsult.set(DataType.BooleanType, !setOfExprResult.contains(leftRsult));

        return leftRsult;
    }

    @Override
    public ExpressionType getFilterExpressionType()
    {
        return ExpressionType.NOT_IN;
    }

    @Override
    public String getString()
    {
       return "NOT IN(" + left.getString() + ',' + right.getString() + ')';
    }

}
