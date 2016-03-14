/**
 * 
 */
package com.huawei.unibi.molap.keygenerator;

/**
 * It can be thrown while generating the key.
 * 
 * @author R00900208
 * 
 */
public class KeyGenException extends Exception
{

    /**
	 * 
	 */
    private static final long serialVersionUID = 3105132151795358241L;

    public KeyGenException()
    {
        super();
    }

    public KeyGenException(Exception e)
    {
        super(e);
    }

    public KeyGenException(Exception e, String msg)
    {
        super(msg, e);
    }

    public KeyGenException(String msg)
    {
        super(msg);
    }

}
