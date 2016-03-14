/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwcfJtSMNYgnOYiEQwbS13nxM8hk/dmbY4B4u+tG
aRAl/mMl/PCNVWxiGY31O1typv/3xRcUFz8xX0WWj5M6Px0mZ0ihH8KhDR5uhYDNmQBz3LbI
TnTaLtcPhyLHiSmR3+fSNXOA6RFHMKum7GgIq3VE8wSbiKvGdFupg82njmFR4g==*/
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
package com.huawei.unibi.molap.schema.metadata;

import java.io.Serializable;
import java.util.Arrays;

/**
 * @author R00900208
 */

public class ArrayWrapper implements Serializable
{
	
    /**
     * 
     * Comment for <code>serialVersionUID</code>
     * 
     */
    private static final long serialVersionUID = -2016551342632572869L;
    
    /**
     * data
     */
    private int[] data;

    /**
     * @param data
     */
    public ArrayWrapper(int[] data)
    {
        if (data == null)
        {
            throw new IllegalArgumentException();
        }
        this.data = data;
    }
    
    /**
     * 
     * @param data
     */
    public void setData(int[] data)
    {
    	this.data = data;
    }

    /**
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object other)
    {
        if(other instanceof ArrayWrapper)
        {
            return Arrays.equals(data, ((ArrayWrapper)other).data);
        }
        else
        {
            return false;
        }

    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode()
    {
    	return Arrays.hashCode(data);
    }
    
    /**
     * @return
     */
    public int[] getData()
    {
    	return data;
    }
}
