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
 * @author R00900208
 *
 */
/**
 * It is top count meta class
 * @author R00900208
 *
 */
public class TopCount implements Serializable
{
	/**
	 * 
	 */
	private static final long serialVersionUID = -8571684898961076954L;

	/**
	 * MolapDimensionLevel
	 */
	private MolapDimensionLevel level;
	
	/**
	 * Measure 
	 */
	private MolapMeasure msr;
	
	/**
	 * TopN count
	 */
	private int count;
	
	/**
	 * TopN type
	 */
	private TopNType type;
	
	
	
	public TopCount(MolapDimensionLevel level, MolapMeasure msr, int count,
			TopNType type) 
	{
		this.level = level;
		this.msr = msr;
		this.count = count;
		this.type = type;
	}


	/**
	 * Enum for TopN types
	 * @author R00900208
	 *
	 */
	public enum TopNType
	{
		/**
		 * Top
		 */
		TOP,
		/**
		 * Bottom
		 */
		BOTTOM;
	}



	/**
	 * Get level
	 * @return the level
	 */
	public MolapDimensionLevel getLevel() 
	{
		return level;
	}



	/**
	 * get measure
	 * @return the msr
	 */
	public MolapMeasure getMsr() 
	{
		return msr;
	}



	/**
	 * Get top count
	 * @return the count
	 */
	public int getCount() 
	{
		return count;
	}



	/**
	 * Get the topn type
	 * @return the type
	 */
	public TopNType getType() 
	{
		return type;
	}
}

