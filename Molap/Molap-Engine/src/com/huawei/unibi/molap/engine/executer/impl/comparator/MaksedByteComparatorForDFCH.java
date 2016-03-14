/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbweRARwUrjYxPx0CUk3mVB7mxOcZSaagKrMQNlhB
QO/t7AKcVpMM9znzw9ojWEN/ToRIKubyVgVJsozdun71L7dz4j8yqXyYXugKf3dXjcdRNofD
rt64BN/cmFfaIYM0Wn+Q+DcTWf0ecuUNGsD1KIZlC5c/G5izx/ikPqwqQvBiQQ==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2013
 * =====================================
*/

package com.huawei.unibi.molap.engine.executer.impl.comparator;

import java.util.Comparator;

import com.huawei.unibi.molap.engine.executer.pagination.impl.DataFileChunkHolder;


/**
 * Project Name NSE V3R7C00 
 * Module Name : Molap Engine
 * Author K00900841
 * Created Date :21-May-2013 6:42:29 PM 
 * FileName :MaksedByteComparator.java
 * Class Description : Comparator responsible for Comparing to DataFileChunkHolder based on key
 * Version 1.0
 */
public class MaksedByteComparatorForDFCH implements Comparator<DataFileChunkHolder>
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
    public MaksedByteComparatorForDFCH(int[] compareRange, byte sortOrder,byte[] maskedKey)
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
    public int compare(DataFileChunkHolder dataFileChunkHolder1, DataFileChunkHolder dataFileChunkHolder2)
    {
        int compare=0;
        byte[] o1 = dataFileChunkHolder1.getRow();
        byte[] o2 = dataFileChunkHolder2.getRow();
        for(int i = 0;i < index.length;i++)
        {
            int a = (o1[index[i]] & this.maskedKey[i]) & 0xff;
            int b = (o2[index[i]] & this.maskedKey[i]) & 0xff;
            compare = a - b;
            if(compare == 0)
            {
                continue;
            }
            compare = compare < 0 ? -1 : 1;
            break;
        }
        if(sortOrder==1)
        {
            return compare*-1;
        }
        return compare;
    }

}
