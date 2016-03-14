/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwdXNiZ+oxCgSX2SR8ePIzMmJfU7u5wJZ2zRTi4X
XHfqbZ8EoDhPx294GKZAm/6F4vGzkDwg9FC6WrH7FByElkrYfDT3zGUAK/PyRv1isK1AphO1
A6fSVyX1WlJDWQLZRZAqz/MhxDucYrPYxncCRI3qo4X0p04iZFvii+PaHsl6VQ==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2014
 * =====================================
*/
package com.huawei.unibi.molap.threadbasedmerger.iterator;

import com.huawei.unibi.molap.threadbasedmerger.container.Container;

/**
 * Project Name NSE V3R7C00 
 * Module Name : Molap Data Processor
 * Author K00900841
 * Created Date :10-June-2014 6:42:29 PM
 * FileName : RecordIterator.java
 * Class Description : Record Iterator class 
 * Version 1.0
 */
public class RecordIterator
{
    /**
     * producer container 
     */
    private final Container producerContainer;
    
    /**
     * record holder array 
     */
    private Object[][] sortHolderArray;
    
    /**
     * record counter
     */
    private int counter;
    
    /**
     * holder size
     */
    private int size;
    
    /**
     * ProducerIterator constructor
     * 
     * @param producerContainer
     */
    public RecordIterator(Container producerContainer)
    {
        this.producerContainer = producerContainer;
    }
    
    /**
     * below method will be used to increment the counter
     */
    public void next()
    {
        counter++;
    }
    
    /**
     * below method will be used to get the row from holder 
     * @return
     */
    public Object[] getRow()
    {
        return sortHolderArray[counter];
    }
    
    /**
     * has next method will be used to check any more row is present or not
     * @return is row present 
     */
    public boolean hasNext()
    {
        if(counter >= size)
        {
            if(!producerContainer.isDone())
            {
                sortHolderArray = producerContainer.getContainerData();
                if(null==sortHolderArray)
                {
                    return false;
                }
                counter = 0;
                size = sortHolderArray.length;
                synchronized(producerContainer)
                {
                    this.producerContainer.setFilled(false);
                    producerContainer.notifyAll();
                }
            }
            else
            {
                return false;
            }
        }
        return true;
    }
}
