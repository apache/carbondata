/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbweRARwUrjYxPx0CUk3mVB7mxOcZSaagKrMQNlhB
QO/t7Ps8s0MMil9X2oV0qmM04LX8T2imX5jZoGnawGoZDzxL9uI1jIB4R34hdqq1sNnYiRB8
D4oODeyFLmqs54vqFMx90BN7neQMY5rxvyr16QvUK+EO5JcDurmGNrn5zx/Omg==*/
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

import com.huawei.unibi.molap.engine.executer.Tuple;


/**
 * Project Name NSE V3R7C00 
 * Module Name : Molap Engine
 * Author K00900841
 * Created Date :21-May-2013 6:42:29 PM 
 * FileName :MaksedByteComparator.java
 * Class Description : Comparator responsible for Comparing to tuple based on key
 * Version 1.0
 */
public class MaksedByteComparatorForTuple implements Comparator<Tuple>
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
    public MaksedByteComparatorForTuple(int[] compareRange, byte sortOrder,byte[] maskedKey)
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
    public int compare(Tuple tuple1, Tuple tuple2)
    {
        int cmp=0;
        byte[] key1 =tuple1.getKey();
        byte[] key2 = tuple2.getKey();
        for(int i = 0;i < index.length;i++)
        {
            int a = (key1[index[i]] & this.maskedKey[i]) & 0xff;
            int b = (key2[index[i]] & this.maskedKey[i]) & 0xff;
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
