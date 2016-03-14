/**
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2013
 * =====================================
*/

package com.huawei.unibi.molap.writer;

import java.util.Arrays;

import com.huawei.unibi.molap.util.ByteUtil;

/**
 * Project Name NSE V3R7C00 
 * Module Name : 
 * Author V00900840
 * Created Date :15-Jul-2013 12:56:42 PM
 * FileName : ByteArrayHolder.java
 * Class Description :
 * Version 1.0
 */
public class ByteArrayHolder implements Comparable<ByteArrayHolder>
{
    
    /**
     * mdkey
     */
    private byte[] mdKey;
    
    /**
     * primary key
     */
    private int primaryKey;

    /**
     * 
     * @param mdKey
     * @param primaryKey
     * 
     */
    public ByteArrayHolder(byte[] mdKey, int primaryKey)
    {
        this.mdKey = mdKey;
        this.primaryKey = primaryKey;
    }
    
    

    /**
     * 
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     * 
     */
    @Override
    public int compareTo(ByteArrayHolder o)
    {
        return ByteUtil.compare(mdKey, o.mdKey);
    }


    @Override
    public boolean equals(Object obj) {
    	// TODO Auto-generated method stub
    	if(obj instanceof ByteArrayHolder)
    	{
    		if(0 == ByteUtil.compare(mdKey, ((ByteArrayHolder) obj).mdKey))
    		{
    			return true;
    		}
    	}
    	return false;
    }
    
    @Override
    public int hashCode() {
    	int prime = 31;
    	int result = prime * Arrays.hashCode(mdKey);
    	result =  result + prime*primaryKey;
    	return result;
    }

    /**
     * 
     * @return Returns the mdKey.
     * 
     */
    public byte[] getMdKey()
    {
        return mdKey;
    }



    /**
     * 
     * @return Returns the primaryKey.
     * 
     */
    public int getPrimaryKey()
    {
        return primaryKey;
    }

}

