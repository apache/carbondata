/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbweRARwUrjYxPx0CUk3mVB7mxOcZSaagKrMQNlhB
QO/t7Hc2jrD/GkuJmo2hwE7u2mtb+CvUpiV26syqITUbc1Fbh020/7vjGIfMVkfmoJAcEB02
W20tYoR6LAutUU7D+aAzbIfkaLUlyYFdUciKZ6SlowK7Xje8wpoLeloKH/xwvA==*/
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
import java.util.List;

import com.huawei.unibi.molap.engine.executer.pagination.impl.DataFileWriter.KeyValueHolder;
import com.huawei.unibi.molap.util.ByteUtil.UnsafeComparer;

/**
 * Project Name NSE V3R7C00 
 * Module Name : Molap Engine
 * Author K00900841
 * Created Date :21-May-2013 6:42:29 PM 
 * FileName :MaksedByteComparator.java
 * Class Description : Comparator responsible for Comparing to ByteArrayWrapper based on key
 * Version 1.0
 */
public class MaksedByteComparatorBAW implements Comparator<KeyValueHolder>
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
    public MaksedByteComparatorBAW(int[] compareRange, byte sortOrder,byte[] maskedKey)
    {
        this.index=compareRange;
        this.sortOrder=sortOrder;
        this.maskedKey=maskedKey;
    }

    public MaksedByteComparatorBAW(byte sortOrder)
    {
        this.sortOrder=sortOrder;
    }

    /**
     * This method will be used to compare two byte array
     * @param o1
     * @param o2
     */
    @Override
    public int compare(KeyValueHolder byteArrayWrapper1, KeyValueHolder byteArrayWrapper2)
    {
        int cmp = 0;
        byte[] o1 = byteArrayWrapper1.key.getMaskedKey();
        byte[] o2 = byteArrayWrapper2.key.getMaskedKey();
        if(null!=index)
        {
        for(int i = 0;i < index.length;i++)
        {
            int a = (o1[index[i]] & this.maskedKey[i]) & 0xff;
            int b = (o2[index[i]] & this.maskedKey[i]) & 0xff;
            cmp = a - b;
            if(cmp != 0)
            {

                if(sortOrder == 1)
                {
                   return  cmp=cmp * -1;
                }
            }
        }
       }
        List<byte[]> listOfDirectSurrogateVal1= byteArrayWrapper1.key.getDirectSurrogateKeyList();
        List<byte[]> listOfDirectSurrogateVal2= byteArrayWrapper2.key.getDirectSurrogateKeyList();
        if(cmp == 0)
        {
            if(null != listOfDirectSurrogateVal1 && null!=listOfDirectSurrogateVal2)
            {
                for(int i = 0;i < listOfDirectSurrogateVal1.size();i++)
                {
                    cmp = UnsafeComparer.INSTANCE.compareTo(listOfDirectSurrogateVal1.get(i),
                            listOfDirectSurrogateVal2.get(i));
                    if(cmp != 0)
                    {

                        if(sortOrder == 1)
                        {
                            cmp=cmp * -1;
                        }
                        return cmp;
                    }
                }
            }
        }
        return cmp;
    }
}
