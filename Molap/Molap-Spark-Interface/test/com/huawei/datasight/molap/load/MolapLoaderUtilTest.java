package com.huawei.datasight.molap.load;

import java.util.ArrayList;

import junit.framework.TestCase;

import com.huawei.unibi.molap.olap.MolapDef.Schema;

public class MolapLoaderUtilTest extends TestCase
{
	public void testgetDimensionSplit()
	{
		try {
			MolapLoaderUtil.getDimensionSplit(new Schema(), "abc", 3);
		} catch (Exception e) {
			
		}		
		catch (Throwable e)
		{
		           
		}
	}

}
