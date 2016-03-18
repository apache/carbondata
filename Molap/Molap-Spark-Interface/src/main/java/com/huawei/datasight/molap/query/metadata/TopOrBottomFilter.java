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
 */
package com.huawei.datasight.molap.query.metadata;

import java.io.Serializable;

/**
 * TopOrBottomFilter
 * @author R00900208
 *
 */
public class TopOrBottomFilter implements Serializable
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 7524328420102662266L;

	/**
	 * Type of filter, like TOP or BOTTOM
	 */
	private TopOrBottomType topOrBottomType;
	
	/**
	 * What is the count user wants
	 */
	/*private int count;
	
	*//**
	 * On what dimension user wants Top or Bottom count
	 *//*
	private MolapDimension dimension;*/
	
	/**
	 * This constructor would be used if user apply topN/bottomN on all. 
	 * @param topOrBottomType
	 * @param count
	 */
	public TopOrBottomFilter(TopOrBottomType topOrBottomType,int count)
	{
		this.topOrBottomType = topOrBottomType;
//		this.count = count;
	}
	
	/**
	 * This constructor would be used if user apply topN/bottomN on particular dimension. 
	 * @param topOrBottomType
	 * @param count
	 * @param dimension
	 */
	public TopOrBottomFilter(TopOrBottomType topOrBottomType,int count,MolapDimension dimension)
	{
		this.topOrBottomType = topOrBottomType;
//		this.count = count;
//		this.dimension = dimension;
	}
	
	/**
	 * @return the topOrBottomType
	 */
	public TopOrBottomType getTopOrBottomType() 
	{
		return topOrBottomType;
	}

	/**
	 * @return the count
	 */
	/*public int getCount() 
	{
		return count;
	}*/

	/**
	 * @return the dimension
	 */
	/*public MolapDimension getDimension() 
	{
		return dimension;
	}*/

	/**
	 * TopOrBottomType
	 * @author R00900208
	 *
	 */
	public enum TopOrBottomType
	{
		TOP,
		BOTTOM
	}

}
