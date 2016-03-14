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
package com.huawei.unibi.molap.query.metadata;

import java.io.Serializable;
import java.util.Arrays;


/**
 * MolapTuple class , it contains the each row or column information of query result.
 * @author R00900208
 *
 */
public class MolapTuple implements Serializable
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 6432454407461679716L;
	
	/**
	 * tuple
	 */
	private MolapMember[] tuple;
	
	/**
	 * Constructor
	 * @param tuple
	 */
	public MolapTuple(MolapMember[] tuple)
	{
		this.tuple = tuple;
	}
	
	/**
	 * Size of tuple.
	 * @return
	 */
	public int size()
	{
		return tuple.length;
	}
	
	/**
	 * Get all members inside tuple.
	 * @return
	 */
	public MolapMember[] getTuple()
	{
		return tuple;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() 
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(tuple);
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) 
	{
	    if (obj instanceof MolapTuple)
        {
	        if (this == obj)
	        {
	            return true;
	        }
	                
	        MolapTuple other = (MolapTuple) obj;
	        if (!Arrays.equals(tuple, other.tuple))
	        {
	            return false;
	        }
	        return true;
            
        }
	    
	    return false;
		
	}
}
