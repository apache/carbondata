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
import java.util.ArrayList;
import java.util.List;

/**
 * Level filter
 * @author R00900208
 *
 */
public class MolapDimensionLevelFilter implements Serializable
{

	/**
	 * 
	 */
	private static final long serialVersionUID = 5028332445998450964L;

	/**
	 * Include filter
	 */
	private List<Object> includeFilter = new ArrayList<Object>(10);
	
	/**
	 * Exclude filter
	 */
	private List<Object> excludeFilter = new ArrayList<Object>(10);
	
	
	/**
	 * Contains filter
	 */
	private List<String> containsFilter = new ArrayList<String>(10);
	
	/**
	 * Does not contain filter
	 */
	private List<String> doesNotContainsFilter = new ArrayList<String>(10);
	
	
	/**
	 * afterTopN
	 */
	private boolean afterTopN;
	

	/**
	 * @return the includeFilter
	 */
	public List<Object> getIncludeFilter() 
	{
		return includeFilter;
	}

	/**
	 * Add filters append  with braces []. Like [2000]
	 * @param includeFilter the includeFilter to set
	 */
	public void setIncludeFilter(List<Object> includeFilter) 
	{
		this.includeFilter = includeFilter;
	}

	/**
	 * @return the excludeFilter
	 */
	public List<Object> getExcludeFilter() 
	{
		return excludeFilter;
	}

	/**
	 * Add filters append  with braces []. Like [2000]
	 * @param excludeFilter the excludeFilter to set
	 */
	public void setExcludeFilter(List<Object> excludeFilter) 
	{
		this.excludeFilter = excludeFilter;
	}

	/**
	 * @return the containsFilter
	 */
	public List<String> getContainsFilter() 
	{
		return containsFilter;
	}

	/**
	 * This filter does not work along with MolapQuery.setExactHirarchyLevelsMatch set as true.
	 * @param containsFilter the containsFilter to set
	 */
	public void setContainsFilter(List<String> containsFilter) 
	{
		this.containsFilter = containsFilter;
	}

	/**
	 * @return the doesNotContainsFilter
	 */
	public List<String> getDoesNotContainsFilter() 
	{
		return doesNotContainsFilter;
	}

	/**
	 * @param doesNotContainsFilter the doesNotContainsFilter to set
	 */
	public void setDoesNotContainsFilter(List<String> doesNotContainsFilter) 
	{
		this.doesNotContainsFilter = doesNotContainsFilter;
	}

	/**
	 * @return the afterTopN
	 */
	public boolean isAfterTopN() {
		return afterTopN;
	}

	/**
	 * @param afterTopN the afterTopN to set
	 */
	public void setAfterTopN(boolean afterTopN) {
		this.afterTopN = afterTopN;
	}
	
	public String toSQLConstruct(String levelName)
    {
        StringBuffer buffer = new StringBuffer();

        boolean appendAndRequired = false;

        // Include filters list
        if(includeFilter.size() > 1)
        {
            buffer.append(levelName);
            buffer.append(" IN ( ");
            for(int i = 0;i < includeFilter.size();i++)
            {
                buffer.append("'" + includeFilter.get(i) + "'");
                if(i != includeFilter.size() - 1)
                {
                    buffer.append(" , ");
                }
            }
            buffer.append(" ) ");

            appendAndRequired = true;
        }
        else if(includeFilter.size() > 0)
        {
            buffer.append(levelName);
            buffer.append(" = '" + includeFilter.get(0)+"'");

            appendAndRequired = true;
        }

        // Exclude filters list
        if(excludeFilter.size() > 1)
        {
            if(appendAndRequired)
            {
                buffer.append(" AND ");
            }
            buffer.append(levelName);
            buffer.append(" NOT IN (");
            for(int i = 0;i < excludeFilter.size();i++)
            {
                buffer.append("'" + excludeFilter.get(i) + "'");
                if(i != excludeFilter.size() - 1)
                {
                    buffer.append(" , ");
                }
            }
            buffer.append(" ) ");

            appendAndRequired = true;
        }
        else if(excludeFilter.size() > 0)
        {
            if(appendAndRequired)
            {
                buffer.append(" AND ");
            }
            buffer.append(levelName);
            buffer.append(" != '" + excludeFilter.get(0)+"'");

            appendAndRequired = true;
        }

        // Contains filters list
        if(containsFilter.size() > 0)
        {

            for(String containsString : containsFilter)
            {
                if(appendAndRequired)
                {
                    buffer.append(" AND ");
                }
                
                buffer.append(levelName);
                buffer.append(" LIKE ");
                buffer.append("'" + containsString + "'");
                
                appendAndRequired = true;
            }

        }

        //Doesn't contain filter
        if(doesNotContainsFilter.size() > 0)
        {

            for(String containsString : doesNotContainsFilter)
            {
                if(appendAndRequired)
                {
                    buffer.append(" AND ");
                }
                buffer.append(levelName);
                buffer.append(" NOT LIKE ");
                buffer.append("'" + containsString + "'");
                
                appendAndRequired = true;
            }

        }

        return buffer.toString();
    }
}
