/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwcfJtSMNYgnOYiEQwbS13nxM8hk/dmbY4B4u+tG
aRAl/nNACT22lIE6Mq/L7ZxlAU/wZ1/5hyWH4XalR77cp0hMjSDj39xVVqDDMbZ2UG7zv3bK
btBXhGSxVC9CTze2vHZk+m4mqXAc4m6GLAXKc52mnlkYVf4ho7XiAwxSTJyt8w==*/
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
package com.huawei.unibi.molap.surrogatekeysgenerator.dbbased;

import java.io.Serializable;
import java.util.Arrays;

/**
* Project Name NSE V3R7C00 
* Module Name : Molap
* Author K00900841
* Created Date :13-May-2013 3:35:33 PM
* FileName : ArrayWrapper.java
* Class Description :This class will be used as a key fi
* Version 1.0
*/
public class IntArrayWrapper implements Serializable, Comparable<IntArrayWrapper>
{

    /**
     * 
     * serialVersionUID
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * 
     */
    private int[] data;
    
    private int primaryKey;

    public IntArrayWrapper(int[] data,int primaryKey)
    {
        initialize(data);
        this.primaryKey = primaryKey;
    }

    /**
     * This method is used to initialize data array
     * 
     * @param data
     *
     */
    public void initialize(int[] data)
    {
        if(data == null)
        {
            throw new IllegalArgumentException(" Data Array is NUll");
        }
        this.data = data;
    }

    /**
     * This method will be used check to ArrayWrapper object is equal or not 
     * 
     * @param object
     *          ArrayWrapper object 
     * @return boolean 
     *          equal or not
     *
     */
    @Override
    public boolean equals(Object other)
    {
        if(other instanceof IntArrayWrapper)
        {
            return Arrays.equals(data, ((IntArrayWrapper)other).data);
        }
        return false;
    }

    /**
     * This method will be used to get the hascode, this will be used to the
     * index for inserting ArrayWrapper object as a key in Map
     * 
     * @return hascode
     * 
     */
    @Override
    public int hashCode()
    {
        return Arrays.hashCode(data);
    }

    /**
     * 
     * This method will be used to get the long array surrogate keys
     * 
     * @return data
     * 
     */
    public int[] getData()
    {
        return data;
    }

    /**
     * Compare method for ArrayWrapper class this will used to compare Two
     * ArrayWrapper data object, basically it will compare two surrogate keys
     * array to check which one is greater
     * 
     * @param other
     *            ArrayWrapper Object
     * 
     */
    @Override
    public int compareTo(IntArrayWrapper other)
    {
        for(int i = 0;i < data.length;i++)
        {
            if(data[i] > other.data[i])
            {
                return 1;
            }
            else if(data[i] < other.data[i])
            {
                return -1;
            }
        }
        return 0;
    }

    public int getPrimaryKey()
    {
        return primaryKey;
    }

    public void setPrimaryKey(int primaryKey)
    {
        this.primaryKey = primaryKey;
    }
}
