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
import java.util.ArrayList;
import java.util.List;

import com.huawei.unibi.molap.query.MolapQuery.SortType;

/**
 * It is Axis class, it can be row,column or slice axis.It contains all information of query depends on levels and measures added in query.
 * 
 * @author R00900208
 *
 */
public class Axis implements Serializable 
{

	/**
	 * 
	 */
	private static final long serialVersionUID = -574689684553603640L;
	
	/**
	 * dims
	 */
	private List<MolapLevelHolder> dims = new ArrayList<MolapLevelHolder>(10);

	/**
	 * Add query details to this axis.
	 * @param level
	 * @param sortType
	 * @param msrFilters
	 * @param dimLevelFilter
	 */
	public void add(MolapLevel level,SortType sortType,List<MolapMeasureFilter> msrFilters,MolapDimensionLevelFilter dimLevelFilter)
	{
		MolapLevelHolder holder = new MolapLevelHolder(level, sortType);
		holder.setMsrFilters(msrFilters);
		holder.setDimLevelFilter(dimLevelFilter);
		dims.add(holder);
	}

	/**
	 * Get dims
	 * @return the dims
	 */
	public List<MolapLevelHolder> getDims() 
	{
		return dims;
	}


}
