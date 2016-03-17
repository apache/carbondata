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
 * This class represents Molap measure filter.
 * @author R00900208
 *
 */
public class MolapMeasureFilter implements Serializable
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 3034718663469418703L;

	/**
	 * first operand
	 */
	/*private double operandOne;
	
	*//**
	 * second operand
	 *//*
	private double operandTwo;
	
	*//**
	 * On which dimension this measure filter is applied.
	 *//*
	private MolapDimension dimension;*/
	
	/**
	 * Filter type.
	 */
	private MolapMeasureFilterType filterType = MolapMeasureFilterType.EQUAL_TO;
	
	/**
	 * This constructor would be used only when user uses NOT_EMPTY filter type.
	 * Ex: select employee_name sum(salary) from employee where salary is not empty.so it will filter out all salary rows which 
	 * has null in the DB.
	 * @param filterType
	 */
	public MolapMeasureFilter(MolapMeasureFilterType filterType)
	{
		this.filterType = filterType;
	}
	
	/**
	 * This constructor would be used for single operand filters like EQUAL_TO,NOT_EQUAL_TO,GREATER_THAN,GREATER_THAN_EQUAL_TO,
	 * LESS_THAN,LESS_THAN_EQUAL_TO.
	 * Ex: select employee_name sum(salary) from employee where salary> 10000.So here 10000 is opearndOne and GREATER_THAN is the filterType
	 * @param operandOne
	 * @param filterType
	 */
	public MolapMeasureFilter(double operandOne, MolapMeasureFilterType filterType)
	{
//		this.operandOne = operandOne;
		this.filterType = filterType;
	}
	
	/**
	 * This constructor would be used for double operand filters like BETWEEN.
	 * Ex: select employee_name sum(salary) from employee where salary between 5000 and 10000.So here 5000 is opearndOne,10000 is operandTwo and BETWEEN is the filterType
	 * @param operandOne
	 * @param operandTwo
	 * @param filterType
	 */
	public MolapMeasureFilter(double operandOne,double operandTwo,MolapMeasureFilterType filterType)
	{
//		this.operandOne = operandOne;
//		this.operandTwo = operandTwo;
		this.filterType = filterType;
	}
	
	/**
	 * On which dimension this measure filter is applied.
	 * If user does not set the dimension it takes the last query dimension as default.
	 * @param dimension
	 */
	/*public void setDimension(MolapDimension dimension)
	{
		this.dimension = dimension;
	}*/
	
	
	/**
	 * @return the operandOne
	 *//*
	public double getOperandOne() 
	{
		return operandOne;
	}*/

	/**
	 * @return the operandTwo
	 *//*
	public double getOperandTwo() 
	{
		return operandTwo;
	}*/

	/**
	 * @return the filterType
	 */
	public MolapMeasureFilterType getFilterType() 
	{
		return filterType;
	}


	/**
	 * Measure filter types.
	 * @author R00900208
	 *
	 */
	public enum MolapMeasureFilterType
	{
        /**
         * filterType
         */
        EQUAL_TO,
        /**
         * NOT_EQUAL_TO
         */
        NOT_EQUAL_TO,
        /**
         * GREATER_THAN
         */
        GREATER_THAN,
        /**
         * LESS_THAN
         */
        LESS_THAN,
        /**
         * LESS_THAN_EQUAL
         */
        LESS_THAN_EQUAL, 
        /**
         * GREATER_THAN_EQUAL
         */
        GREATER_THAN_EQUAL,
        
		BETWEEN;
	}


	/**
	 * @return the dimension
	 */
	/*public MolapDimension getDimension() {
		return dimension;
	}*/

}
