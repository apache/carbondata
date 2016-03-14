/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwcfJtSMNYgnOYiEQwbS13nxM8hk/dmbY4B4u+tG
aRAl/hzMC92bX59BPG9xRoYctuFK7SJVjx2kQiL0pHBef+8GY3DBHHvElckcwmDwaOwdn7si
JvGjW6RJenISrhVbcWX+ZxoH6evjQ7S56SFx8bCiRO7qya8q7qhvLKuq6AUh9A==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 *
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2013
 * =====================================
 *
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
