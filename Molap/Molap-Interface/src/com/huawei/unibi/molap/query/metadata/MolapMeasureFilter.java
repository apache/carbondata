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


/**
 * Measure filter
 * 
 * @author R00900208
 *
 */
public class MolapMeasureFilter implements Serializable
{
		
	/**
	 * 
	 */
	private static final long serialVersionUID = -4253090536204072658L;

	/**
	 * filterValue
	 */
	private double filterValue;
	
	/**
	 * filterType
	 */
	private FilterType filterType;
	
	/**
	 * afterTopN
	 */
	private boolean afterTopN;
	
	/**
	 * Constructor that takes filter information for measure filter.
	 * @param filterValue
	 * @param filterType
	 */
	public MolapMeasureFilter(double filterValue, FilterType filterType) 
	{
		this.filterValue = filterValue;
		this.filterType = filterType;
	}
	
	/**
	 * Constructor that takes filter information for measure filter.
	 * @param filterType
	 */
	public MolapMeasureFilter(FilterType filterType) 
	{
		this.filterType = filterType;
	}
	
	/**'
	 * Enum for measure filter types.
	 * @author R00900208
	 *
	 */
	public enum FilterType
	{
		/**
		 * EQUAL_TO
		 */
		EQUAL_TO(" = "),
		/**
		 * NOT_EQUAL_TO
		 */
		NOT_EQUAL_TO(" != "),
		/**
		 * GREATER_THAN
		 */
		GREATER_THAN(" > "),
		/**
		 * LESS_THAN
		 */
		LESS_THAN(" < "), 
		/**
		 * LESS_THAN_EQUAL
		 */
		LESS_THAN_EQUAL(" <= "), 
		/**
		 * GREATER_THAN_EQUAL
		 */
		GREATER_THAN_EQUAL(" >= "),
		
		/**
		 * NOT_EMPTY
		 */
		NOT_EMPTY(" IS NOT NULL ");
		
		String symbol;
        
		FilterType(String symbol) 
        {
            this.symbol = symbol;
        }
	}


	/**
	 * get FilterValue
	 * @return the filterValue
	 */
	public double getFilterValue() 
	{
		return filterValue;
	}


	/**
	 * FilterType
	 * @return the filterType
	 */
	public FilterType getFilterType() 
	{
		return filterType;
	}

	/**
	 * @return the afterTopN
	 */
	public boolean isAfterTopN() {
		return afterTopN;
	}

	/**
	 * @param afterTopN the afterTopN to set
	 */
	public void setAfterTopN(boolean afterTopN) {
		this.afterTopN = afterTopN;
	}
	
	public String toSQLConstruct(String levelName)
	{
	    return levelName + filterType.symbol + filterValue;
	}

}
