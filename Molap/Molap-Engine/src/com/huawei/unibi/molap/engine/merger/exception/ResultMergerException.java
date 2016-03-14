package com.huawei.unibi.molap.engine.merger.exception;

import java.util.Locale;

public class ResultMergerException extends Exception
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
    public ResultMergerException(String msg)
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
    public ResultMergerException(String msg, Throwable t)
    {
        super(msg,t);
        this.msg = msg;
    }
    
   /**
    * Constructor
    * @param t
    */
    public ResultMergerException(Throwable t)
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
