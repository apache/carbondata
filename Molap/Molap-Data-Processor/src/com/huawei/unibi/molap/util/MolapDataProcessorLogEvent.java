/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwdXNiZ+oxCgSX2SR8ePIzMmJfU7u5wJZ2zRTi4X
XHfqbaFkr4As4NKCh4fqAvYgdmQs0mgCSQwvz+LZlIGXCO0iDbgT63DnNIhccSpId4F0SYD6
PC/5IHkl+EwyLOGZaiMDZX6gH228RbMX0/9llMmJjsBFLZVlO/s8L8f2bvT1+A==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
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
public enum MolapDataProcessorLogEvent implements LogEvent 
{
    /**
     * MOLAPENGINE_MSG
     */
    UNIBI_MOLAPDATAPROCESSOR_MSG("molap.dataprocessor");

    /**
     * 
     */
    private String eventCode;

    private MolapDataProcessorLogEvent(final String eventCode)
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
        return "MOLAP_DATAPROCESSOR";
    }

}
