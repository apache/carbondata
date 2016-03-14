/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwddts1/q4bCGDA4M3dH8C2PEEMnfDqqdF4ZhcSc
1BeEnGMLrB5I65/XL6xjm/txYAjm19L2BFwZT8E7IToTDEY/moDXEsf+AwU+oqCMFTzW5jGf
nwsU+rEF3frL4FiJaKdGpqZCEwHOKUOaS+QIjHLy+CQ5P6a++AjBo7uFKlG+QA==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
package com.huawei.unibi.molap.etl;

/**
 * 
 * Project Name NSE V3R7C00 
 * Module Name : MOLAP
 * Author :C00900810
 * Created Date :24-Jun-2013
 * FileName : DataLoadingException.java
 * Class Description : 
 * Version 1.0
 */
public class DataLoadingException extends Exception
{
    /**
   * 
   */
    private static final long serialVersionUID = 1L;

    /**
     * 
     */
    private long errorCode = -1;

    public DataLoadingException()
    {
        super();
    }

    public DataLoadingException(long errorCode,String message)
    {
        super(message);
        this.errorCode = errorCode;
    }

    public DataLoadingException(String message)
    {
        super(message);
    }

    public DataLoadingException(Throwable cause)
    {
        super(cause);
    }

    public DataLoadingException(String message, Throwable cause)
    {
        super(message, cause);
    }

    /**
     * @return
     */
    public long getErrorCode()
    {
        return errorCode;
    }

}