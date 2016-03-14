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

import java.io.Serializable;

/**
 * It is level interface for MOLAP dimension level and measure.
 * 
 * @author R00900208
 *
 */
public interface MolapLevel extends Serializable 
{
	
	/**
	 * Type of level, either dimension level or measure.
	 * @return MolapLevelType
	 */
	MolapLevelType getType();
	
	/**
	 * Dimension name of the level it belonged to.
	 * @return the dimensionName
	 */
	String getDimensionName(); 


	/**
	 * Hierarchy name of the level it belonged to.
	 * @return the hierarchyName
	 */
	String getHierarchyName();


	/**
	 * Name of dimension level or measure
	 * @return the levelName
	 */
	String getName(); 

	/**
	 * Type of dimension level, either level or measure
	 * @author R00900208
	 *
	 */
	public enum MolapLevelType
	{
		/**
		 * DIMENSION
		 */
		DIMENSION,
		
		/**
		 * MEASURE
		 */
		MEASURE,
		
		/**
		 * MEASURE
		 */
		CALCULATED_MEASURE,
		
		/**
		 * DYNAMIC LEVEL
		 */
		DYNAMIC_DIMENSION;
	}
}
