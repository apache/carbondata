/**
 * 
 */
package com.huawei.unibi.molap.datastorage.store.columnar;

import com.huawei.unibi.molap.util.ByteUtil.UnsafeComparer;

/**
 * @author R00900208
 *
 */
public class ColumnWithIndex implements Comparable<ColumnWithIndex>
{
    private byte[] column;
    
    private short index;

    public ColumnWithIndex(byte[] column, short index)
    {
        this.column = column;
        this.index = index;
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
    public short getIndex()
    {
        return index;
    }
    
    //we are not comparing any values
  /*  public boolean equals(ColumnWithIndex colIndex){
        return compareTo(colIndex)==0;
    }*/
    

    @Override
    public int compareTo(ColumnWithIndex o)
    {
        return UnsafeComparer.INSTANCE.compareTo(column, o.column);
    }
    
}
