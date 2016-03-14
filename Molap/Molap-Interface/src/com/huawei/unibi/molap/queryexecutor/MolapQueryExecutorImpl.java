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
package com.huawei.unibi.molap.queryexecutor;

import com.huawei.unibi.molap.query.MolapQuery;
import com.huawei.unibi.molap.query.result.MolapResultStream;

/**
 * Implementation class for  MolapQueryExecutor class
 * @author R00900208
 *
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
