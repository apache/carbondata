/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbweRARwUrjYxPx0CUk3mVB7mxOcZSaagKrMQNlhB
QO/t7Ih1bQ/c5eio3/tm3futG0mnuNtI1G2bkOIRbl0Nr/zf4ilGI9DSPb7dy/aKMgpejwXY
84rYzF1nh52YpbBWkYQbhGj+KZ1UZweCyiV798GAztDMJqJGQsDX0a+8upFGrA==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * 
 */
package com.huawei.unibi.molap.engine.executer.impl;

/**
 * Classes which wants to listens the row count exceed even can implement this interface
 * 
 * @author R00900208
 *
 */
public interface RowCounterListner
{
    /**
     * This method is called when row limit exceeds.
     */
    void rowLimitExceeded() throws Exception;
}
