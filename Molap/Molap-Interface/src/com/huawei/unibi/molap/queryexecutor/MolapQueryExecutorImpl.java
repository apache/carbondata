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

package com.huawei.unibi.molap.queryexecutor;

import com.huawei.unibi.molap.query.MolapQuery;
import com.huawei.unibi.molap.query.result.MolapResultStream;

/**
 * Implementation class for MolapQueryExecutor class
 */
public class MolapQueryExecutorImpl implements MolapQueryExecutor 
{
	
	/**
	 * internalExecutor
	 */
	private MolapQueryInternalExecutor internalExecutor;
	
	/**
	 * schemaName
	 */
	private String schemaName;
	
	/**
	 * cubeName
	 */
	private String cubeName;

	/**
	 * Execute the query
	 */
	@Override
	public MolapResultStream execute(MolapQuery molapQuery) 
	{
		return internalExecutor.execute(molapQuery,schemaName,cubeName).getResultStream();
	}

	/**
	 * @return the internalExecutor
	 */
	public MolapQueryInternalExecutor getInternalExecutor() 
	{
		return internalExecutor;
	}

	/**
	 * @param internalExecutor the internalExecutor to set
	 */
	public void setInternalExecutor(MolapQueryInternalExecutor internalExecutor) 
	{
		this.internalExecutor = internalExecutor;
	}

	/**
	 * @return the schemaName
	 */
	public String getSchemaName() 
	{
		return schemaName;
	}

	/**
	 * @param schemaName the schemaName to set
	 */
	public void setSchemaName(String schemaName) 
	{
		this.schemaName = schemaName;
	}

	/**
	 * @return the cubeName
	 */
	public String getCubeName() 
	{
		return cubeName;
	}

	/**
	 * @param cubeName the cubeName to set
	 */
	public void setCubeName(String cubeName) 
	{
		this.cubeName = cubeName;
	}

}
