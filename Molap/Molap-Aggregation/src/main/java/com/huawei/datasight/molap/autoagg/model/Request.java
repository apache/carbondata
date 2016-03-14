package com.huawei.datasight.molap.autoagg.model;

/**
 * DDL identifier that whether request is for data stats or query stats
 * @author A00902717
 *
 */
public enum Request
{

	DATA_STATS("DATA_STATS"),QUERY_STATS("QUERY_STATS");
	
	//aggregate suggestion type
	private String aggSuggType;
	
	Request(String aggSuggType)
	{
		this.aggSuggType=aggSuggType;
	}
	public static Request getRequest(String requestType)
	{
		if("DATA_STATS".equalsIgnoreCase(requestType))
		{
			return DATA_STATS;
		}
		else
		{
			return QUERY_STATS;
		}
	}
	
	public String getAggSuggestionType()
	{
		return this.aggSuggType;
	}
	
}
