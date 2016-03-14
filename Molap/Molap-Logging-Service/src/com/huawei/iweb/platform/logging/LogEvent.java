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

/**
 * Interface for Log Event
 * 
 * @author R72411
 * @version 1.0
 * @created 08-Oct-2008 10:37:40
 */
public interface LogEvent
{

    /**
     * The Event code
     * 
     * @return The event's code
     */
    String getEventCode();

    /**
     * The module/bundle name from which the logging is done
     * 
     * @return String
     */
    String getModuleName();

}