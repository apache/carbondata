/**
 *
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2013
 * =====================================
 *
 */
package com.huawei.unibi.molap.query.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.huawei.unibi.molap.query.MolapQuery;
import com.huawei.unibi.molap.query.metadata.Axis;
import com.huawei.unibi.molap.query.metadata.MolapDimensionLevel;
import com.huawei.unibi.molap.query.metadata.MolapDimensionLevelFilter;
import com.huawei.unibi.molap.query.metadata.MolapMeasure;
import com.huawei.unibi.molap.query.metadata.MolapMeasureFilter;
import com.huawei.unibi.molap.query.metadata.TopCount;
import com.huawei.unibi.molap.query.metadata.TopCount.TopNType;

/**
 * It is the implementation class for MolapQuery interface.
 * @author R00900208
 *
 */
public class MolapQueryImpl implements MolapQuery 
{

	/**
	 * 
	 */
	private static final long serialVersionUID = -1565369538375956018L;
	
	/**
	 * Slice number
	 */
	private static final int SLICE = 2;
	
	/**
	 * axises
	 */
	private Axis[] axises; 
	
	/**
	 * Top count list
	 */
	private List<TopCount> topCounts = new ArrayList<TopCount>(10);
	
	/**
	 * propertiesRequired
	 */
	private boolean propertiesRequired;
	
	/**
	 * When it set as true then user needs to provide the filters exactly with there parent members.
	 */
	private boolean exactLevelsMatch;
	
	/**
	 * breakHierarchyTopN
	 */
	private boolean breakHierarchyTopN;
	
	/**
	 * Time zone to covert the data. 
	 */
	private String timeZone;


	private Map<String, Object> extraProperties = new HashMap<String, Object>(16);
	
	/**
	 * Default constructor
	 */
	public MolapQueryImpl()
	{
		axises = new Axis[3];
		for (int i = 0; i < axises.length; i++) 
		{
			axises[i] = new Axis();
		}
	}
	
	/**
	 * see interface comments.
	 */
	@Override
	public void addDimensionLevel(MolapDimensionLevel dimensionLevel,
			MolapDimensionLevelFilter filter, SortType sortType, AxisType axis) 
	{
		sortType= sortType== null?SortType.NONE:sortType;
		axises[axis.getIndex()].add(dimensionLevel, sortType, null, filter);
	}

	/**
	 * see interface comments.
	 */
	@Override
	public void addMeasure(MolapMeasure measure, List<MolapMeasureFilter> filters,
			SortType sortType) 
	{
		sortType= sortType== null?SortType.NONE:sortType;
		axises[AxisType.COLUMN.getIndex()].add(measure, sortType, filters, null);
		
	}

	/**
	 * see interface comments.
	 */
	@Override
	public void addSlice(MolapDimensionLevel dimensionLevel,
			MolapDimensionLevelFilter filter) 
	{
		axises[SLICE].add(dimensionLevel, null, null, filter);
	}

	/**
	 * see interface comments.
	 */
	@Override
	public void addSlice(MolapMeasure measure, List<MolapMeasureFilter> filters) 
	{
		axises[SLICE].add(measure, null, filters, null);		
	}

	/**
	 * see interface comments.
	 */
	@Override
	public void addTopCount(MolapDimensionLevel dimensionLevel,
			MolapMeasure measure, int count) 
	{
		topCounts.add(new TopCount(dimensionLevel, measure, count, TopNType.TOP));
	}

	/**
	 * see interface comments.
	 */
	@Override
	public void addBottomCount(MolapDimensionLevel dimensionLevel,
			MolapMeasure measure, int count) 
	{
		topCounts.add(new TopCount(dimensionLevel, measure, count, TopNType.BOTTOM));
	}
	

	/**
	 * @return the axises
	 */
	public Axis[] getAxises() 
	{
		return axises;
	}


	/**
	 * @return the topCounts
	 */
	public List<TopCount> getTopCounts() 
	{
		return topCounts;
	}


	/**
	 * See interface comments
	 */
	@Override
	public void showLevelProperties(boolean showProerties) 
	{
		propertiesRequired = showProerties;
	}
	
	/**
	 * Whether can show properties or not.
	 * @return
	 */
	public boolean isShowLevelProperties() 
	{
		return propertiesRequired;
	}

	/**
	 * See interface comments
	 */
	@Override
	public void setExactHirarchyLevelsMatch(boolean exactLevelsMatch) 
	{
		this.exactLevelsMatch=exactLevelsMatch;
	}

	/**
	 * @return the exactLevelsMatch
	 */
	public boolean isExactLevelsMatch() {
		return exactLevelsMatch;
	}

	@Override
	public void setExtraProperties(Map<String, Object> extraProperties) 
	{
		this.extraProperties = extraProperties;
	}

	/**
	 * @return the extraProperties
	 */
	public Map<String, Object> getExtraProperties() 
	{
		return extraProperties;
	}
	
	
	/**
	 * @return the breakHierarchyTopN
	 */
	public boolean isBreakHierarchyTopN() 
	{
		return breakHierarchyTopN;
	}

	/**
	 * @param breakHierarchyTopN the breakHierarchyTopN to set
	 */
	public void setBreakHierarchyTopN(boolean breakHierarchyTopN) 
	{
		this.breakHierarchyTopN = breakHierarchyTopN;
	}

	/**
	 * setTimeZone
	 */
	@Override
	public void setTimeZone(String timeZone) 
	{
		this.timeZone = timeZone;
	}
	
	/**
	 * getTimeZone
	 * @return
	 */
	public String getTimeZone() 
	{
		return this.timeZone;
	}
	
}
