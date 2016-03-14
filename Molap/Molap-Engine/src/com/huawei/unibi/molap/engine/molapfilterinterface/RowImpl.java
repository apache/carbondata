package com.huawei.unibi.molap.engine.molapfilterinterface;


public class RowImpl implements RowIntf
{
    private Object[] row;
    
    public RowImpl()
    {
        row = new Object[0];
    }
    
    @Override
    public Object getVal(int index)
    {
        return row[index];
    }

    @Override
    public Object[] getValues()
    {
        return row;
    }

    @Override
    public int size()
    {
        return this.row.length;
    }

    @Override
    public void setValues(final Object[] row)
    {
        this.row=row;
    }
}
