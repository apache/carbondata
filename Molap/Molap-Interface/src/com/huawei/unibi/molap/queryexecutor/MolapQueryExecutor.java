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
 * Molap query executor
 * @author R00900208
 *
 */
public interface MolapQueryExecutor 
{
	/**
	 * Execute the Molap query.
	 * @param molapQuery
	 * @return
	 */
	MolapResultStream execute(MolapQuery molapQuery);

}
