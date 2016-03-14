/**
 * 
 */
package com.huawei.datasight.molap.query.metadata;

import java.io.Serializable;

/**
 * TopOrBottomFilter
 * @author R00900208
 *
 */
public class TopOrBottomFilter implements Serializable
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 7524328420102662266L;

	/**
	 * Type of filter, like TOP or BOTTOM
	 */
	private TopOrBottomType topOrBottomType;
	
	/**
	 * What is the count user wants
	 */
	/*private int count;
	
	*//**
	 * On what dimension user wants Top or Bottom count
	 *//*
	private MolapDimension dimension;*/
	
	/**
	 * This constructor would be used if user apply topN/bottomN on all. 
	 * @param topOrBottomType
	 * @param count
	 */
	public TopOrBottomFilter(TopOrBottomType topOrBottomType,int count)
	{
		this.topOrBottomType = topOrBottomType;
//		this.count = count;
	}
	
	/**
	 * This constructor would be used if user apply topN/bottomN on particular dimension. 
	 * @param topOrBottomType
	 * @param count
	 * @param dimension
	 */
	public TopOrBottomFilter(TopOrBottomType topOrBottomType,int count,MolapDimension dimension)
	{
		this.topOrBottomType = topOrBottomType;
//		this.count = count;
//		this.dimension = dimension;
	}
	
	/**
	 * @return the topOrBottomType
	 */
	public TopOrBottomType getTopOrBottomType() 
	{
		return topOrBottomType;
	}

	/**
	 * @return the count
	 */
	/*public int getCount() 
	{
		return count;
	}*/

	/**
	 * @return the dimension
	 */
	/*public MolapDimension getDimension() 
	{
		return dimension;
	}*/

	/**
	 * TopOrBottomType
	 * @author R00900208
	 *
	 */
	public enum TopOrBottomType
	{
		TOP,
		BOTTOM
	}

}
