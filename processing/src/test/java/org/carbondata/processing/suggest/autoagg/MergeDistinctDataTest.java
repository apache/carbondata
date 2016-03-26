/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.carbondata.processing.suggest.autoagg;

import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.carbondata.processing.suggest.datastats.MergeDistinctData;
import org.carbondata.processing.suggest.datastats.model.Level;
import org.junit.Test;


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
