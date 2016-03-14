/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwfnVI3c/udSMK6An9Lipq6FjccIMKj41/T4EBXl
K2tBNxzuvXU/ULQ6C0/KH54Z7Dk92lUqmYBDRvyjyQapMz5kepuoYiZTRsWTiXNkckd362Qs
6KwQTFGpXBo70L680CbXOqrGJmPIZgqeIN2Lzn+2Dq0PgBq/wXc89owXetXmGQ==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
package com.huawei.unibi.molap.dataprocessor.queue.impl;

import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.huawei.unibi.molap.dataprocessor.queue.Queue;
import com.huawei.unibi.molap.dataprocessor.record.holder.DataProcessorRecordHolder;


/**
 * Project Name NSE V3R7C30 
 * Module Name : Molap
 * Author V00900840
 * Created Date :26-Feb-2014 12:12:25 PM
 * FileName : DataProcessorQueue.java
 * Class Description :
 * Version 1.0
 */
public class DataProcessorQueue implements Queue<DataProcessorRecordHolder>
{
    /**
     * Size of the queue
     */
    private int qSize;
    
    /**
     * Counter to maintain state of the queue.
     */
    private AtomicInteger counter;

    /**
     * Queue that holds the data.
     */
    private PriorityBlockingQueue<DataProcessorRecordHolder> priorityQueue;

    
    public DataProcessorQueue(int size)
    {
        this.counter = new AtomicInteger();
        this.qSize = size;
        this.priorityQueue = new PriorityBlockingQueue<DataProcessorRecordHolder>(size, new RecordComparator());
        
        
    }
    
    @Override
    public boolean offer(DataProcessorRecordHolder obj)
    {
        if(counter.get() == qSize)
        {
            return false;
        }
        else
        {
            
            priorityQueue.offer(obj);
            counter.getAndIncrement();
            return true;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public DataProcessorRecordHolder poll()
    {
        if(priorityQueue.isEmpty())
        {
            return null;
        }
        else
        {
            counter.getAndDecrement();
            return (DataProcessorRecordHolder)priorityQueue.poll();
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public DataProcessorRecordHolder peek()
    {
        if(priorityQueue.isEmpty())
        {
            return null;
        }
        else
        {
              return priorityQueue.peek();
        }
    }
    
    /**
     * Is Queue is Full.
     * 
     * @return
     *
     */
    public boolean isFull()
    {
        return counter.get() == qSize;
    }
    
    /**
     * Is queue is Empty 
     * 
     * @return
     *
     */
    public boolean isEmpty()
    {
        return priorityQueue.isEmpty();
    }
    
    /**
     * return the size (i.e. Elements present in the Queue)
     */
    public int size()
    {
        return counter.get();
    }
}
