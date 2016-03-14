package com.huawei.datasight.molap.datastats.model;

import java.io.Serializable;
import java.util.List;

public class DriverDistinctData implements Serializable
{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private List<String> loads;
	private Level[] levels;
	
	public DriverDistinctData(List<String> loads,Level[] levels)
	{
		this.loads=loads;
		this.levels=levels;
	}

	public List<String> getLoads()
	{
		return loads;
	}

	public Level[] getLevels()
	{
		return levels;
	}
	

}
