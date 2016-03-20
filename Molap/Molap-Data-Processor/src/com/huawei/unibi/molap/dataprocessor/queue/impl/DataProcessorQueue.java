/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


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
