/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwfnVI3c/udSMK6An9Lipq6FjccIMKj41/T4EBXl
K2tBN7ovKNbAOcEDl1zA9SLse1Va7O8cElyNt5pz0KRMcHUQwk1ygbuGdJnsD7xh6cRXtgCB
y04Ngl/4JKPiXGzkdFGwQy8WnRZF5P/mhod9uc+dZnLqSMsFm1vfY+9SPtYH7A==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
package com.huawei.unibi.molap.dataprocessor.queue;

/**
 * Project Name NSE V3R7C30 
 * Module Name : Molap
 * Author V00900840
 * Created Date :24-May-2013 12:15:55 PM
 * FileName : Queue.java
 * Class Description : This is queue interface for 
 * handling the dataload checkpoint request in queue.
 * Version 1.0
 */
public interface Queue<E>
{
    /**
     * This method insert the element in the queue, and return false if it fails
     * to isert in the queue.
     * 
     * @param obj
     * @return true if insererted properly false otherwise.
     */
    boolean offer(E obj);
    
    
    /**
     * This method get the element from the head of the queue and 
     * remove the element from the queue.
     * 
     * @return
     */
    E poll();
    
    /**
     * This method just return the element in the head of the queue.
     * @return
     */
    E peek();
   
}
