/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwedLwWEET5JCCp2J65j3EiB2PJ4ohyqaGEDuXyJ
TTt3d7RXgq62sVvnXIp1Blu2u/F8oTodIH8N2LUdbYT5c3O9BmTczv85MGnRCcX/cbITigJh
hpTSyRd2fJHgX5scVoHgpzwKTSU0cybKgR+GdEGZ+DxJt6TLb97dqlhIYvlrHA==*/
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

package com.huawei.unibi.molap.engine.filters.metadata;

import java.io.Serializable;

/**
 * @author R00900208
 * 
 */
public class FilterModel implements Serializable
{

    /**
	 * 
	 */
    private static final long serialVersionUID = 2779048394090554034L;

    /**
     * Dimensions,filter index, the filter values
     */
    private byte[][][] filter;

    /**
     * 
     */
    private byte[][] maxKey;

    /**
     * 
     */
    private int maxSize;

    public FilterModel(byte[][][] filter, byte[][] maxKey, int maxSize)
    {
        this.filter = filter;
        this.maxKey = maxKey;
        this.maxSize = maxSize;
    }

    /**
     * @return the filter
     */
    public byte[][][] getFilter()
    {
        return filter;
    }

    /**
     * @param filter
     *            the filter to set
     */
    public void setFilter(byte[][][] filter)
    {
        this.filter = filter;
    }

    /**
     * @return the maxKey
     */
    public byte[][] getMaxKey()
    {
        return maxKey;
    }

    /**
     * @param maxKey
     *            the maxKey to set
     */
    public void setMaxKey(byte[][] maxKey)
    {
        this.maxKey = maxKey;
    }

    /**
     * @return the maxSize
     */
    public int getMaxSize()
    {
        return maxSize;
    }

    /**
     * @param maxSize
     *            the maxSize to set
     */
    public void setMaxSize(int maxSize)
    {
        this.maxSize = maxSize;
    }

}
