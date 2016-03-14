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
 * Molap Measure class
 * @author R00900208
 *
 */
public class MolapMeasure extends AbstractMolapLevel
{

	/**
	 * 
	 */
	private static final long serialVersionUID = 4257185028603048687L;
	
	/**
	 * Measure name
	 */
	private String measureName;
	
	private MolapDimensionLevel dimensionLevel;
	/**
	 * 
	 */
//	private String aggregateName;
	
	/**
	 * Constructor 
	 * @param measureName
	 */
	public MolapMeasure(String measureName) 
	{
		this.measureName = measureName;
	}
	
	/**
	 * Constructor 
	 * 
	 * @param measureName
	 * @param aggregateName
	 */
	public MolapMeasure(String measureName, String aggregateName) 
	{
	    this.measureName = measureName;
	  //  this.aggregateName = aggregateName;
	}

	/**
	 * @return the measureName
	 */
	public String getName() 
	{
		return measureName;
	}

	/**
	 * See interface comments
	 */
	@Override
	public MolapLevelType getType() 
	{
		return MolapLevelType.MEASURE;
	}

	/**
	 * @return the dimensionLevel
	 */
	public MolapDimensionLevel getDimensionLevel() {
		return dimensionLevel;
	}

	/**
	 * @param dimensionLevel the dimensionLevel to set
	 */
	public void setDimensionLevel(MolapDimensionLevel dimensionLevel) {
		this.dimensionLevel = dimensionLevel;
	}

}
