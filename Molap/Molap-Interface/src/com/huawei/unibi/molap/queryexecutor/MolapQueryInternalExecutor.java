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

import java.util.UUID;

import com.huawei.unibi.molap.query.MolapQuery;
import com.huawei.unibi.molap.query.result.MolapResultStreamHolder;

/**
 * It is the internal service for Spring to execute the query and fetch the data.
 * @author R00900208
 *
 */
public interface MolapQueryInternalExecutor 
{
	
	/**
	 * Execute the query 
	 * @param molapQuery
	 * @param schemaName
	 * @param cubeName
	 * @return
	 */
    MolapResultStreamHolder execute(MolapQuery molapQuery,String schemaName,String cubeName);

    /**
     * Get the next chunk with uuid
     * @param uuid
     * @return
     */
    MolapResultStreamHolder getNext(UUID uuid);
    
}
