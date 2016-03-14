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
package com.huawei.unibi.molap.util;

import com.huawei.iweb.platform.logging.LogEvent;

/**
 * MolapLogEvent
 * 
 * @author R00903154
 * 
 */
public enum MolapCoreLogEvent implements LogEvent 
{
    /**
     * MOLAPCORE_MSG
     */
    UNIBI_MOLAPCORE_MSG("molap.core");

    /**
     * eventCode.
     */
    private String eventCode;

    private MolapCoreLogEvent(final String eventCode)
    {
        this.eventCode = eventCode;
    }

    /**
     * 
     * 
     * @see com.huawei.iweb.platform.logging.LogEvent#getEventCode()
     * 
     */

    public String getEventCode()
    {
        return eventCode;
    }

    /**
     * 
     * 
     * @see com.huawei.iweb.platform.logging.LogEvent#getModuleName()
     * 
     */

    public String getModuleName()
    {
        return "MOLAP_CORE";
    }

}
