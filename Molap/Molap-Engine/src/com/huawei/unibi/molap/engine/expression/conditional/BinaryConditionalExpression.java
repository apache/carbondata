package com.huawei.unibi.molap.engine.expression.conditional;

import java.util.ArrayList;
import java.util.List;

import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.engine.expression.ColumnExpression;
import com.huawei.unibi.molap.engine.expression.Expression;
import com.huawei.unibi.molap.engine.expression.logical.BinaryLogicalExpression;

public abstract class BinaryConditionalExpression  extends BinaryLogicalExpression implements ConditionalExpression
{

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public BinaryConditionalExpression(Expression left, Expression right)
    {
        super(left, right);
        // TODO Auto-generated constructor stub
    }

    // Will get the column informations involved in the expressions by
    // traversing the tree
    public List<ColumnExpression> getColumnList()
    {
        // TODO
        List<ColumnExpression> listOfExp=new ArrayList<ColumnExpression>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        getColumnList(this,listOfExp);
        return listOfExp;
    }
    
    private void getColumnList(Expression expression, List<ColumnExpression> lst)
    {
        if(expression instanceof ColumnExpression)
        {
            ColumnExpression colExp=(ColumnExpression)expression;
            boolean found=false;
                
           for(ColumnExpression currentColExp:lst)
            {
                if(currentColExp.getColumnName().equals(colExp.getColumnName()))
                {
                    found=true;
                    colExp.setColIndex(currentColExp.getColIndex());
                    break;
                }
            }
            if(!found)
            {
                colExp.setColIndex(lst.size());
                lst.add(colExp);
            }  
        }
        for(Expression child: expression.getChildren()){
           getColumnList(child, lst);
        }
    }

    public boolean isSingleDimension()
    {
       List<ColumnExpression> listOfExp = new ArrayList<ColumnExpression>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        getColumnList(this, listOfExp);
        if(listOfExp.size() == 1 && listOfExp.get(0).isDimension())
        {
            return true;
        }
        return false;

    }

}
