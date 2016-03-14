/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwcfJtSMNYgnOYiEQwbS13nxM8hk/dmbY4B4u+tG
aRAl/s5eiYskzQsgjdcUfEewvVuh066qd8hn8UtEx43qQO3ia2pTtuQHCFQ0Y8NNO7nYrZ5x
Q/KMjjBJ8vdUsEOIFOo2q7PpkeG+4qxV+COQkO6hw229TqTDaG4b/bSyTRrOXg==*/
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
package com.huawei.unibi.molap.sortandgroupby.exception;

import java.util.Locale;

/**
 * Project Name NSE V3R7C00 
 * Module Name : MOLAP
 * Author :C00900810
 * Created Date :24-Jun-2013
 * FileName : MolapSortKeyAndGroupByException.java
 * Class Description : MolapSortKeyAndGroupByException
 * Version 1.0
 */
public class MolapSortKeyAndGroupByException extends Exception
{

    /**
     * default serial version ID.
     */
    private static final long serialVersionUID = 1L;

    /**
     * The Error message.
     */
    private String msg = "";


    /**
     * Constructor
     * 
     * @param errorCode
     *            The error code for this exception.
     * @param msg
     *            The error message for this exception.
     * 
     */
    public MolapSortKeyAndGroupByException(String msg)
    {
        super(msg);
        this.msg = msg;
    }
    
    /**
     * Constructor
     * 
     * @param errorCode
     *            The error code for this exception.
     * @param msg
     *            The error message for this exception.
     * 
     */
    public MolapSortKeyAndGroupByException(String msg, Throwable t)
    {
        super(msg,t);
        this.msg = msg;
    }
    
   /**
    * Constructor
    * @param t
    */
    public MolapSortKeyAndGroupByException(Throwable t)
    {
        super(t);
    }

    /**
     * This method is used to get the localized message.
     * 
     * @param locale
     *            - A Locale object represents a specific geographical,
     *            political, or cultural region.
     * @return - Localized error message.
     */
    public String getLocalizedMessage(Locale locale)
    {
        return "";
    }

    /**
     * getLocalizedMessage
     */
    @Override
    public String getLocalizedMessage()
    {
        return super.getLocalizedMessage();
    }

    /**
     * getMessage
     */
    public String getMessage()
    {
        return this.msg;
    }
}
