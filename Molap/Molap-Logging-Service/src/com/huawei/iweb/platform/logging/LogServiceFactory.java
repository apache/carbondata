/**
 *
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2011
 * =====================================
 *
 */

package com.huawei.iweb.platform.logging;

import com.huawei.iweb.platform.logging.impl.StandardLogService;

/**
 * 
 * Log Service factory.
 * 
 * @author R72411
 * @version 1.0
 * @created 08-Oct-2008 10:37:40
 */

public final class LogServiceFactory
{
	private LogServiceFactory()
	{
		
	}
	
    /**
     * return Logger Service.
     * 
     * @param clazzName
     *            provides class name
     * @return LogService
     * 
     */
    public static LogService getLogService(final String className)
    {
    	return new StandardLogService(className);
    }
    
    public static LogService getLogService()
    {
    	return new StandardLogService();
    }

}