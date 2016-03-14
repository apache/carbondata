package com.huawei.unibi.molap.datastorage.store.columnar;

import com.huawei.unibi.molap.util.ByteUtil.UnsafeComparer;

public class ColumnWithIntIndex implements Comparable<ColumnWithIntIndex>
{
    protected byte[] column;
    
    private int index;

    public ColumnWithIntIndex(byte[] column, int index)
    {
        this.column = column;
        this.index = index;
    }
    
    public ColumnWithIntIndex()
    {
    }    

    /**
     * @return the column
     */
    public byte[] getColumn()
    {
        return column;
    }

    /**
     * @return the index
     */
    public int getIndex()
    {
        return index;
    }
    
    //we are not comparing the values
    /*public boolean equals(ColumnWithIntIndex colIndex){
        return compareTo(colIndex)==0;
    }*/

    @Override
    public int compareTo(ColumnWithIntIndex o)
    {
        return UnsafeComparer.INSTANCE.compareTo(column, o.column);
    }

    /**
     * @param column the column to set
     */
    public void setColumn(byte[] column)
    {
        this.column = column;
    }

    /**
     * @param index the index to set
     */
    public void setIndex(int index)
    {
        this.index = index;
    }
    
}
