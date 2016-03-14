package com.huawei.datasight.molap.autoagg;

import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;

import com.huawei.datasight.molap.datastats.MergeDistinctData;
import com.huawei.datasight.molap.datastats.model.Level;

public class MergeDistinctDataTest
{

	@Test
	public void testMergeData()
	{
		MergeDistinctData merge=new MergeDistinctData();
		merge.mergeDistinctData(createLevel());
		Level[] merged=merge.mergeDistinctData(createLevel());
		Assert.assertNotNull(merged);
	}
	
	public static Level[] createLevel()
	{
		Level[] levels=new Level[3];
		Level level0=new Level(0,10);
		level0.setName("0");
		Map<Integer,Integer> distinct0=new HashMap<Integer,Integer>();	
		distinct0.put(1, 3);
		distinct0.put(2, 3);
		level0.setOtherDimesnionDistinctData(distinct0);
		levels[0]=level0;
		
		Level level1=new Level(1,9);
		level1.setName("1");
		Map<Integer,Integer> distinct1=new HashMap<Integer,Integer>();	
		distinct1.put(0, 3);
		distinct1.put(2, 3);
		level1.setOtherDimesnionDistinctData(distinct1);
		levels[1]=level1;
		
		Level level2=new Level(2,5);
		level2.setName("2");
		Map<Integer,Integer> distinct2=new HashMap<Integer,Integer>();	
		distinct2.put(0, 3);
		distinct2.put(1, 3);
		level2.setOtherDimesnionDistinctData(distinct2);
		levels[2]=level2;
		return levels;
	}

}
