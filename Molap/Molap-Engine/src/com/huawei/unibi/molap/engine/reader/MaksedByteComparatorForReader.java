package com.huawei.unibi.molap.engine.reader;

import java.util.Comparator;

public class MaksedByteComparatorForReader implements Comparator<ResultTempFileReader>
{

    /**
     * compareRange
     */
    private int[] index;
    
    /**
     * sortOrder
     */
    private byte sortOrder;
    
    /**
     * maskedKey
     */
    private byte[] maskedKey;
    /**
     * MaksedByteResultComparator Constructor
     */
    public MaksedByteComparatorForReader(final int[] compareRange, final byte sortOrder,final byte[] maskedKey)
    {
        this.index=compareRange;
        this.sortOrder=sortOrder;
        this.maskedKey=maskedKey;
    }

    /**
     * This method will be used to compare two byte array
     * @param o1
     * @param o2
     */
    @Override
    public int compare(ResultTempFileReader dataFileChunkHolder1, ResultTempFileReader dataFileChunkHolder2)
    {
        int cmp=0;
        byte[] o1 = dataFileChunkHolder1.getRow();
        byte[] o2 = dataFileChunkHolder2.getRow();
        for(int i = 0;i < index.length;i++)
        {
            int a = (o1[index[i]] & this.maskedKey[i]) & 0xff;
            int b = (o2[index[i]] & this.maskedKey[i]) & 0xff;
            cmp = a - b;
            if(cmp == 0)
            {
                continue;
            }
            cmp = cmp < 0 ? -1 : 1;
            break;
        }
        if(sortOrder==1)
        {
            return cmp*-1;
        }
        return cmp;
    }

}
