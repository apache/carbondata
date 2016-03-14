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
package com.huawei.unibi.molap.query.metadata;



/**
 * Molap dimension level;
 * @author R00900208
 *
 */
public class MolapDimensionLevel extends AbstractMolapLevel
{

	/**
	 * 
	 */
	private static final long serialVersionUID = 4012085091766940643L;	

	/**
	 * Dimension name
	 */
	private String dimensionName;
	
	/**
	 * Hierarchy name
	 */
	private String hierarchyName;
	
	
	/**
	 * level name
	 */
	private String levelName;
	
	
	/**
	 * Constructor
	 * @param dimensionName
	 * @param hierarchyName
	 * @param levelName
	 */
	public MolapDimensionLevel(String dimensionName, String hierarchyName,
			String levelName) 
	{
		this.dimensionName = dimensionName;
		this.hierarchyName = hierarchyName;
		this.levelName = levelName;
	}

	/**
	 * @return the dimensionName
	 */
	public String getDimensionName() 
	{
		return dimensionName;
	}

	/**
	 * @return the hierarchyName
	 */
	public String getHierarchyName() 
	{
		return hierarchyName;
	}

	/**
	 * @return the levelName
	 */
	public String getName() 
	{
		return levelName;
	}

	@Override
	public MolapLevelType getType() 
	{
		
		return MolapLevelType.DIMENSION;
	}


}
