/**
 * 
 */
package com.huawei.datasight.molap.query.metadata;

/**
 * Type of sort order like asc,dsc or none 
 * @author R00900208
 *
 */
public enum SortOrderType 
{
	/**
	 * Ascending order
	 */
	ASC(0),
	/**
	 * Descending order.
	 */
	DSC(1),
	/**
	 * No order mentioned
	 */
	NONE(-1);
	
	/**
	 * Order type in numeric
	 */
	private int orderType;
	
	
	SortOrderType(int orderType)
	{
		this.orderType = orderType;
	}
	
	/**
	 * Order type in number
	 * @return orderType int
	 */
	public int getOrderType()
	{
		return orderType;
	}
}
