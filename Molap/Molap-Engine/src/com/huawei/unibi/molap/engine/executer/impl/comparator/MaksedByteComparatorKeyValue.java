/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.huawei.unibi.molap.engine.executer.impl.comparator;

import java.util.Comparator;

import com.huawei.unibi.molap.engine.executer.pagination.impl.DataFileWriter.KeyValueHolder;

public class MaksedByteComparatorKeyValue implements Comparator<KeyValueHolder>
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
    public MaksedByteComparatorKeyValue(int[] compareRange, byte sortOrder,byte[] maskedKey)
    {
        this.sortOrder=sortOrder;
        this.index=compareRange;       
        this.maskedKey=maskedKey;
    }

    /**
     * This method will be used to compare two byte array
     * @param o1
     * @param o2
     */
    @Override
    public int compare(KeyValueHolder byteArrayWrapper1, KeyValueHolder byteArrayWrapper2)
    {
        byte[] o1 = byteArrayWrapper1.key.getMaskedKey();
        byte[] o2 = byteArrayWrapper2.key.getMaskedKey();
        int cmp=0;
        for(int i = 0;i < index.length;i++)
        {
            int a = (o1[index[i]] & this.maskedKey[i]) & 0xff;
            int b = (o2[index[i]] & this.maskedKey[i]) & 0xff;
            cmp = a - b;
            if(cmp != 0)
            {
                
                if(sortOrder==1)
                {
                    return cmp*-1;
                }
                return cmp;
            }
        }

        return cmp;
    }
}
