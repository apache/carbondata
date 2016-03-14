package com.huawei.unibi.exception;

/**
 * 
 * @author B71024
 *
 */
public class CipherException extends Exception 
{

    private static final long serialVersionUID = 1L;
    
    /**
     * The constructor for Exception Class
     * 
     * @param message
     *            the message to be provided for exception
     * 
     */
    public CipherException(String message)
    {
        super(message);
    }
}
