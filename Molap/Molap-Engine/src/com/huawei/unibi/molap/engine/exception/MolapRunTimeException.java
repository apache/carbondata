/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwcAIRTtLWBkMMN+iqJ62JNQb/MYFaBoemC1VlrU
n+vkOWyFHtWKTxjrlY1IKvKfL05uqSU0hcqbnk1O1mv+y/A7QLB5cQbiTQ9JTdtB1qpkJMzI
DW0OJ+kkXxiqmTi7H4ecVM4YGSBx/M363gqrSwMh59u5Z3BdY0/2gHuZSNWM0g==*/
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
package com.huawei.unibi.molap.engine.exception;

/**
 * Molap exception
 * @author R00900208
 *
 */
public class MolapRunTimeException extends Exception
{

    /**
     * 
     */
    private static final long serialVersionUID = -6558635976353616798L;
    
    public MolapRunTimeException(Throwable exception)
    {
        super(exception);
    }
    
    public MolapRunTimeException(Throwable exception,String msg)
    {
        super(msg,exception);
    }
    
    public MolapRunTimeException(String msg)
    {
        super(msg);
    }

}
