/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwfQVwqh74rUY6n+OZ2pUrkn1TkkvO60rFu08DZa
JnQq9B5Wiqv70ud3QZn6B2Lvqm9GnZHUsLSqCCvEhP53SFx8FLVV4aCVz+jeeeHIODgkh33q
UGeoRpURk/+laybnaEx/f5T4iRG+TJ1zFZLVQAw/85YQsOWyh/aCti/1P1548Q==*/
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
package com.huawei.unibi.molap.engine.util;

import com.huawei.iweb.platform.logging.LogEvent;

/**
 * MolapLogEvent
 * 
 * @author R00903154
 * 
 */
public enum MolapEngineLogEvent implements LogEvent 
{
    /**
     * MOLAPENGINE_MSG
     */
    UNIBI_MOLAPENGINE_MSG("molap.engine");

    /**
     * 
     */
    private String eventCode;

    private MolapEngineLogEvent(final String eventCode)
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
        return "MOLAP_ENGINE";
    }

}
