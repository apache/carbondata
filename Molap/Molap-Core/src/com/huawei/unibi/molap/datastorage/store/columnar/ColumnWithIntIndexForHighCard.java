package com.huawei.unibi.molap.datastorage.store.columnar;

import com.huawei.unibi.molap.util.ByteUtil.UnsafeComparer;

public class ColumnWithIntIndexForHighCard extends ColumnWithIntIndex implements Comparable<ColumnWithIntIndex>
{

    public ColumnWithIntIndexForHighCard(byte[] column, int index)
    {
        super(column, index);
    }

    @Override
    public int compareTo(ColumnWithIntIndex o)
    {
        return UnsafeComparer.INSTANCE.compareTo(column, 2, column.length-2, o.column, 2, o.column.length-2);
    }

}
