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

/**
 * Molap Measure class
 */
public class MolapMeasure extends AbstractMolapLevel
{
	private static final long serialVersionUID = 4257185028603048687L;
	
	/**
	 * Measure name
	 */
	private String measureName;
	
	private MolapDimensionLevel dimensionLevel;

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
