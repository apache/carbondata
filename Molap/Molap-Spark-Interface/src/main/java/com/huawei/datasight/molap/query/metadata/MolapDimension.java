/**
 * 
 */
package com.huawei.datasight.molap.query.metadata;

import java.io.Serializable;

/**
 * Dimension class
 * @author R00900208
 *
 */
public class MolapDimension implements MolapColumn, Serializable
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 8447604537458509117L;

	/**
	 * Dimension unique name
	 */
	private String dimensionUniqueName;
	
	/**
	 * queryOrder
	 */
	private int queryOrder;
	
	private boolean isDistinctCountQuery;

	/**
	 * sort order type. default is no order.
	 */
	private SortOrderType sortOrderType = SortOrderType.NONE;
	
	/**
	 * Constructor to create dimension.User needs to pass dimension unique name.
	 * @param dimensionUniqueName
	 */
	public MolapDimension(String dimensionUniqueName)
	{
		this.dimensionUniqueName = dimensionUniqueName;
	}

	/**
	 * @return the sortOrderType
	 */
	public SortOrderType getSortOrderType() 
	{
		return sortOrderType;
	}

	/**
	 * @param sortOrderType the sortOrderType to set
	 */
	public void setSortOrderType(SortOrderType sortOrderType) 
	{
		this.sortOrderType = sortOrderType;
	}

	/**
	 * @return the dimensionUniqueName
	 */
	public String getDimensionUniqueName() 
	{
		return dimensionUniqueName;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime
				* result
				+ ((dimensionUniqueName == null) ? 0 : dimensionUniqueName
						.hashCode());
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
		{
			return true;
		}
		if (obj == null)
		{
			return false;
		}
		if (getClass() != obj.getClass())
		{
			return false;
		}
		MolapDimension other = (MolapDimension) obj;
		if (dimensionUniqueName == null) {
			if (other.dimensionUniqueName != null)
			{
				return false;
			}
		} else if (!dimensionUniqueName.equals(other.dimensionUniqueName))
		{
			return false;
		}
		return true;
	}

	public int getQueryOrder() {
		return queryOrder;
	}

	public void setQueryOrder(int queryOrder) {
		this.queryOrder = queryOrder;
	}

	public boolean isDistinctCountQuery() {
		return isDistinctCountQuery;
	}

	public void setDistinctCountQuery(boolean isDistinctCountQuery) {
		this.isDistinctCountQuery = isDistinctCountQuery;
	}
}
