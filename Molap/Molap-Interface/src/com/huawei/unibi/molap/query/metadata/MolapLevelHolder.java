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
import java.util.List;

import com.huawei.unibi.molap.query.MolapQuery.SortType;

/**
 * It is holder class for a level
 * @author R00900208
 *
 */
public class MolapLevelHolder implements Serializable
{

	/**
	 * 
	 */
	private static final long serialVersionUID = -6328136034161360231L;
	
	/**
	 * Level
	 */
	private MolapLevel level;
	
	/**
	 * sortType
	 */
	private SortType sortType;
	
	/**
	 * msrFilters
	 */
	private List<MolapMeasureFilter> msrFilters;
	
	/**
	 * dimLevelFilter
	 */
	private MolapDimensionLevelFilter dimLevelFilter;
	
	
	/**
	 * Constructor
	 * @param level
	 * @param sortType
	 */
	public MolapLevelHolder(MolapLevel level, SortType sortType) 
	{
		super();
		this.level = level;
		this.sortType = sortType;
	}

	/**
	 * @return the level
	 */
	public MolapLevel getLevel() 
	{
		return level;
	}

	/**
	 * @return the sortType
	 */
	public SortType getSortType() 
	{
		return sortType;
	}

	/**
	 * @return the msrFilter
	 */
	public List<MolapMeasureFilter> getMsrFilters() 
	{
		return msrFilters;
	}

	/**
	 * @param msrFilter the msrFilter to set
	 */
	public void setMsrFilters(List<MolapMeasureFilter> msrFilters) 
	{
		this.msrFilters = msrFilters;
	}

	/**
	 * @param sortType the sortType to set
	 */
	public void setSortType(SortType sortType) 
	{
		this.sortType = sortType;
	}

	/**
	 * @return the dimLevelFilter
	 */
	public MolapDimensionLevelFilter getDimLevelFilter() 
	{
		return dimLevelFilter;
	}

	/**
	 * @param dimLevelFilter the dimLevelFilter to set
	 */
	public void setDimLevelFilter(MolapDimensionLevelFilter dimLevelFilter) 
	{
		this.dimLevelFilter = dimLevelFilter;
	}

	
}