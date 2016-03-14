package com.huawei.unibi.molap.engine.expression;

import com.huawei.unibi.molap.engine.molapfilterinterface.ExpressionType;
import com.huawei.unibi.molap.engine.molapfilterinterface.RowIntf;
import com.huawei.unibi.molap.metadata.MolapMetadata.Dimension;

public class ColumnExpression extends LeafExpression
{
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    private String columnName;

    private boolean isDimension;

    private int colIndex=-1;

    private DataType dataType;

    private Dimension dim;

    public ColumnExpression(String columnName, DataType dataType)
    {
        this.columnName = columnName;
        this.dataType = dataType;

    }

    public String getColumnName()
    {
        return columnName;
    }

    public void setColumnName(String columnName)
    {
        this.columnName = columnName;
    }

    public boolean isDimension()
    {
        return isDimension;
    }

    public void setDimension(boolean isDimension)
    {
        this.isDimension = isDimension;
    }

    public int getColIndex()
    {
        return colIndex;
    }

    public void setColIndex(int colIndex)
    {
        this.colIndex = colIndex;
    }

    public DataType getDataType()
    {
        return dataType;
    }

    public void setDataType(DataType dataType)
    {
        this.dataType = dataType;
    }

    @Override
    public ExpressionResult evaluate(RowIntf value)
    {
        ExpressionResult expressionResult = new ExpressionResult(dataType, value.getVal(colIndex));
        return expressionResult;
    }

    @Override
    public ExpressionType getFilterExpressionType()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getString()
    {
        // TODO Auto-generated method stub
        return "ColumnExpression(" + columnName + ')';
    }

    public Dimension getDim()
    {
        return dim;
    }

    public void setDim(Dimension dim)
    {
        this.dim = dim;
    }



}
