/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwdXNiZ+oxCgSX2SR8ePIzMmJfU7u5wJZ2zRTi4X
XHfqbQR8tCKUflDPtJh60YpTlRW6rBgzzC3zzZL6iBVHl9Vsznh5pPScAkG6PdfW2CJ/CuTs
bZSrwQb4/4lFup+XITHHPVZonIQdDmP7Pcw+LSYHwrW9lBkL1fHpvcwjaS3aQQ==*/
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

package com.huawei.unibi.molap.threadbasedmerger.container;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.util.MolapDataProcessorLogEvent;

/**
 * Project Name NSE V3R7C00 
 * Module Name : Molap Data Processor
 * Author K00900841
 * Created Date :10-June-2014 6:42:29 PM
 * FileName : Container.java
 * Class Description : Container class 
 * Version 1.0
 */
public class Container
{
    /**
     * LOGGER
     */
    private static final LogService LOGGER = LogServiceFactory.getLogService(Container.class.getName());
    /**
     * record array
     */
    private Object[][] sortHolderArray;

    /**
     * is array filled 
     */
    private boolean isFilled;

    /**
     * is done 
     */
    private boolean isDone;
    
    /**
     * container counter
     */
    private int containerCounter;

    /**
     * Below method will be used to fill the container 
     * @param sortHolder
     */
    public void fillContainer(Object[][] sortHolder)
    {
        sortHolderArray = sortHolder;
    }

    /**
     * below method will be used to get the container data 
     * @return
     */
    public Object[][] getContainerData()
    {
    	//CHECKSTYLE:OFF    Approval No:Approval-252
        while(!isFilled && !isDone)
        {
            try//CHECKSTYLE:ON
            {
                Thread.sleep(10);
            }
            catch(InterruptedException e)
            {
                LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, e);
            }
        }
      
        Object[][] temp=sortHolderArray;
        sortHolderArray=null;
        return temp;
    }

    /**
     * @return the isFilled
     */
    public boolean isFilled()
    {
        return isFilled;
    }

    /**
     * @param isFilled
     *            the isFilled to set
     */
    public void setFilled(boolean isFilled)
    {
        this.isFilled = isFilled;
    }

    /**
     * @return the isDone
     */
    public boolean isDone()
    {
        return isDone;
    }

    /**
     * @param isDone the isDone to set
     */
    public void setDone(boolean isDone)
    {
        this.isDone = isDone;
    }

    /**
     * @return the containerCounter
     */
    public int getContainerCounter()
    {
        return containerCounter;
    }

    /**
     * @param containerCounter the containerCounter to set
     */
    public void setContainerCounter(int containerCounter)
    {
        this.containerCounter = containerCounter;
    }
}
