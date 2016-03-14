package com.huawei.unibi.molap.filter;

import com.huawei.unibi.molap.filter.MolapFilterInfo;

import junit.framework.TestCase;

public class MolapFilterInfoTest extends TestCase
{
	public void test()
	{
		MolapFilterInfo fil= new MolapFilterInfo();
		fil.getEffectiveExcludedMembers();
	}
}
