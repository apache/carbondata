package com.huawei.datasight.molap.query.metadata;

import com.huawei.unibi.molap.engine.filters.likefilters.FilterLikeExpressionIntf;

public class MolapLikeFilter extends MolapDimensionFilter {

	
	


	/**
	 * 
	 */
	private static final long serialVersionUID = 6243463940253685256L;
	/**
	 * Include like filters.
	 * Ex: select employee_name,department_name,sum(salary) from employee where employee_name like ("a","b");
	 * then "a" and "b" will be the include like filters.
	 */
	private FilterLikeExpressionIntf likeFilterExpressions;
	


	/**
	 * @param includeLikeFilter the includeLikeFilter to set
	 */
	public void setLikeFilterExpression(FilterLikeExpressionIntf likeFilterExpre) 
	{
		this.likeFilterExpressions=likeFilterExpre;
	}

	/**
	 * @return the excludeLikeFilter
	 */
	public FilterLikeExpressionIntf getLikeFilterExpression() 
	{
		return likeFilterExpressions;
	}
}
