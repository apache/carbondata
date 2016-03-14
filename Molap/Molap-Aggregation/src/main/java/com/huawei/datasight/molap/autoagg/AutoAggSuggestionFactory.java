package com.huawei.datasight.molap.autoagg;

import com.huawei.datasight.molap.autoagg.model.Request;
import com.huawei.datasight.molap.datastats.DSAutoAggSuggestionService;
import com.huawei.datasight.molap.querystats.QSAutoAggSuggestionService;

/**
 * This class will given instance of AggregateService based on request type
 * request type can be based on DATA or QUERY
 * @author A00902717
 *
 */
public final class AutoAggSuggestionFactory
{
	private AutoAggSuggestionFactory()
	{
		
	}
	public static AutoAggSuggestionService getAggregateService(Request requestType)
	{
		switch (requestType)
		{
		case DATA_STATS:
			return new DSAutoAggSuggestionService();
		case QUERY_STATS:
			return new QSAutoAggSuggestionService();
		default:			
			return new DSAutoAggSuggestionService();
		}
	}

}
