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

package com.huawei.unibi.molap.query.metadata;

import java.io.Serializable;

/**
 * It is level interface for MOLAP dimension level and measure.
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
