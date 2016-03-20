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

package com.huawei.unibi.molap.sortandgroupby.sortKey;

import java.util.Comparator;

/**
 * Project Name NSE V3R7C00 
 * Module Name : Molap Data Processor 
 * Author K00900841
 * Created Date :21-May-2013 6:42:29 PM 
 * FileName :HierarchyBTreeWriter.java
 * Class Description : This class is responsible for comparing two mdkey 
 * Version 1.0
 */
public class MolapRowComparator implements Comparator<Object[]>
{
    /**
     * mdkey index
     */
    private int mdKeyIndex;

    /**
     * MolapRowComparator Constructor
     * 
     * @param mdKeyIndex
     *          mdmeky index
     *
     */
    public MolapRowComparator(int mdKeyIndex)
    {
        this.mdKeyIndex = mdKeyIndex;
    }

    /**
     * Below method will be used to compare two mdkey 
     * 
     * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
     *
     */
    public int compare(Object[] o1, Object[] o2)
    {
        // get the mdkey
        byte[] b1 = (byte[])o1[this.mdKeyIndex];
        // get the mdkey
        byte[] b2 = (byte[])o2[this.mdKeyIndex];
        int cmp = 0;
        int length = b1.length < b2.length ? b1.length : b2.length;

        for(int i = 0;i < length;i++)
        {
            int a = b1[i] & 0xFF;
            int b = b2[i] & 0xFF;
            cmp = a - b;
            if(cmp == 0)
            {
                continue;
            }
            cmp = cmp < 0 ? -1 : 1;
            break;
        }

        if((b1.length != b2.length) && (cmp == 0))
        {
            cmp = b1.length - b2.length;
        }
        return cmp;
    }
}
