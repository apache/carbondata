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

package com.huawei.datasight.molap.datastats;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.huawei.datasight.molap.datastats.model.Level;

/**
 * This class will merge disinct data found across Restructure folder
 * @author A00902717
 *
 */
public class MergeDistinctData 
{
	/**
	 * final merged data
	 */
	private Level[] mergedData;
	
	public Level[] mergeDistinctData(Level[] levels)
	{
		if(null==mergedData)
		{
			this.mergedData=levels;
			return levels;
		}
		for(int i=0;i<levels.length;i++)
		{
			Level merge=searchInMergeData(levels[i]);
			if(null!=merge)
			{
				mergeLevels(levels[i],merge);	
			}
			
		}
		return mergedData;
	}

	/**
	 * 
	 * @param level : level to merge
	 * @param merge : merge to destination level
	 */
	private void mergeLevels(Level level, Level merge) 
	{
	
		Map<Integer,Integer> mergedOtherDimDistinctData=merge.getOtherDimesnionDistinctData();
		Map<Integer,Integer> levelOtherDimDistinctData=level.getOtherDimesnionDistinctData();
		Set<Entry<Integer,Integer>> entries=levelOtherDimDistinctData.entrySet();
		Iterator<Entry<Integer,Integer>> itr=entries.iterator();
		while(itr.hasNext())
		{
			Entry<Integer,Integer> entry=itr.next();
			int key=entry.getKey();
			Integer mergedDataValue=mergedOtherDimDistinctData.get(key);
			if(null!=mergedDataValue)
			{
				int mergedValue=Math.round((entry.getValue()+mergedDataValue)/2);
				mergedOtherDimDistinctData.put(key, mergedValue);
			}
			else
			{
				//if it is added dimension than add it in mergeddata
				mergedOtherDimDistinctData.put(key, entry.getValue());
				
			}
		}
	}

	/**
	 * find level to merge in final mergeDistinctdata
	 * @param level
	 * @return
	 */
	private Level searchInMergeData(Level level) 
	{
		for(int i=0;i<mergedData.length;i++)
		{
			if(mergedData[i].getOrdinal()==level.getOrdinal())
			{
				return mergedData[i];
			}
		}
		return null;
	}
	/**
	 * get merged data
	 * @return
	 */
	public Level[] getMergedData()
	{
		return mergedData;
	}

}