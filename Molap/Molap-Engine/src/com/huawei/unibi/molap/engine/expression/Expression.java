package com.huawei.unibi.molap.engine.expression;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.engine.expression.exception.FilterUnsupportedException;
import com.huawei.unibi.molap.engine.molapfilterinterface.ExpressionType;
import com.huawei.unibi.molap.engine.molapfilterinterface.RowIntf;

public abstract class Expression implements Serializable
{

    /**
     * 
     */
    private static final long serialVersionUID = -7568676723039530713L;
    protected List<Expression> children = new ArrayList<Expression>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);

    public abstract ExpressionResult evaluate(RowIntf value) throws FilterUnsupportedException;

    public abstract ExpressionType getFilterExpressionType();

    public List<Expression> getChildren()
    {
        return children;
    }
    

    public abstract String getString();
   

   // public abstract void  accept(ExpressionVisitor visitor);
}
