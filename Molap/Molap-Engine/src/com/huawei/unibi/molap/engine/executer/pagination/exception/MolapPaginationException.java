/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwedLwWEET5JCCp2J65j3EiB2PJ4ohyqaGEDuXyJ
TTt3d9pkTPChhGGh3+OVlm96pe0Tyibd50vdkZc/BOykBRsRWVWLDvzyOAPA5UACzuYO0NoI
nym6ryvv2M8dq/xHwVrAMmGYWRw+hSgQZ9YYBT0T30oMd90X2o0E/B/Bh1X9Yw==*/
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
package com.huawei.unibi.molap.engine.executer.pagination.exception;

import java.util.Locale;

/**
 * 
 * Project Name NSE V3R7C00 
 * Module Name : Molap Data Processor
 * Author K00900841
 * Created Date :21-May-2013 6:42:29 PM
 * FileName : MolapPaginationException.java
 * Class Description :  Molap Pagination Exception class 
 * Version 1.0
 */
public class MolapPaginationException extends Exception
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
    public MolapPaginationException(String msg)
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
    public MolapPaginationException(String msg, Throwable t)
    {
        super(msg,t);
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
    public MolapPaginationException(Throwable t)
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
