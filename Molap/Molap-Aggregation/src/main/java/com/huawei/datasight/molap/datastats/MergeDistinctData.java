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