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
package com.huawei.unibi.molap.sortandgroupby.sortData;

import java.util.Comparator;

import com.huawei.unibi.molap.util.RemoveDictionaryUtil;

/**
 * Project Name 	: Carbon 
 * Module Name 		: MOLAP Data Processor
 * Author 			: Suprith T 72079 
 * Created Date 	: 19-Aug-2015
 * FileName 		: RowComparator.java
 * Description 		: This class is responsible for comparing two rows
 * Class Version 	: 1.0
 */
public class RowComparator implements Comparator<Object[]>
{
    /**
     * dimension count
     */
    private int dimensionCount;

    /**
     * MolapRowComparator Constructor
     * 
     * @param dimensionCount
     *
     */
    public RowComparator(int dimensionCount)
    {
        this.dimensionCount = dimensionCount;
    }

    /**
     * Below method will be used to compare two mdkey 
     * 
     * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
     *
     */
    public int compare(Object[] rowA, Object[] rowB)
    {
    	int diff = 0;

        for(int i = 0; i < dimensionCount; i++)
        {
            
            int dimFieldA = RemoveDictionaryUtil.getDimension(i, rowA);
            int dimFieldB = RemoveDictionaryUtil.getDimension(i, rowB);
            
            diff = dimFieldA - dimFieldB;
            if(diff != 0)
            {
                return diff;
            }
        }
        return diff;
    }
}
