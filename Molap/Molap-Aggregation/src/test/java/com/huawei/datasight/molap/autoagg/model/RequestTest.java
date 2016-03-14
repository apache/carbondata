package com.huawei.datasight.molap.autoagg.model;

import junit.framework.Assert;

import org.junit.Test;

public class RequestTest
{
	@Test
	public void testGetRequest(){
		Assert.assertEquals(Request.DATA_STATS, Request.getRequest("DATA_STATS"));
		Assert.assertEquals(Request.QUERY_STATS, Request.getRequest("QUERY_STATS"));
		Assert.assertEquals("DATA_STATS", Request.DATA_STATS.getAggSuggestionType());
	}

}
