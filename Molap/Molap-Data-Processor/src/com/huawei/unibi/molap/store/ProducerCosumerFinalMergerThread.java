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

/**
 *
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2014
 * =====================================
 *
 */
package com.huawei.unibi.molap.store;

import java.util.AbstractQueue;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.sortandgroupby.sortKey.MolapSortKeyException;
import com.huawei.unibi.molap.store.writer.exception.MolapDataWriterException;
import com.huawei.unibi.molap.threadbasedmerger.container.Container;
import com.huawei.unibi.molap.threadbasedmerger.iterator.RecordIterator;
import com.huawei.unibi.molap.util.ByteUtil;
import com.huawei.unibi.molap.util.MolapDataProcessorLogEvent;

/**
 * Project Name NSE V3R7C00 
 * Module Name : Molap Data Processor 
 * Author K00900841
 * Created Date :21-May-2013 6:42:29 PM 
 * FileName : MolapDataWriterStep.java
 * Class Description : below class is responsible for sorting the data, this class uses producer consumer based algorithm to sort the data  
 * 
 * Version 1.0
 */
public class ProducerCosumerFinalMergerThread implements Callable<Void>
{
    /**
     * LOGGER
     */
    private static final LogService LOGGER = LogServiceFactory
            .getLogService(ProducerCosumerFinalMergerThread.class.getName());
    
    /**
     * dataHandler
     */
    private MolapFactHandler dataHandler;
    
    /**
     * measureCount
     */
    private int measureCount;
    
    /**
     * mdKeyIndex
     */
    private int mdKeyIndex;
    
    /**
     * container
     */
    private List<Container> producerContainer;

    /**
     * container
     */
    private int producerCounter;

    /**
     * recordHolderHeap
     */
    private AbstractQueue<RecordIterator> recordHolderHeap;
    
    /**
     * ProducerCosumerFinalMergerThread
     * 
     * @param dataHanlder
     * @param measureCount
     * @param mdKeyIndex
     */
    public ProducerCosumerFinalMergerThread(MolapFactHandler dataHanlder,
            int measureCount, int mdKeyIndex, List<Container> producerContainer)
    {
        this.dataHandler = dataHanlder;
        this.measureCount = measureCount;
        this.mdKeyIndex = mdKeyIndex;
        this.producerContainer = producerContainer;
    }

    @Override
    public Void call() throws Exception
    {
        RecordIterator[] iterators = new RecordIterator[this.producerContainer
                .size()];
        RecordIterator iterator= null;
      //CHECKSTYLE:OFF    Approval No:Approval-V3R8C00_015
        for(Container container : producerContainer)
        {
            iterator = new RecordIterator(container);
            iterators[producerCounter] = iterator;
            producerCounter++;
        }
      //CHECKSTYLE:ON
        int counter = 0;
      //CHECKSTYLE:OFF    Approval No:Approval-V3R8C00_020
        try
        {
            if(producerCounter == 2)
            {
                Object[] row1= null;
                Object[] row2 = null;
                while(iterators[0].hasNext() && iterators[1].hasNext())
                {
                    row1 = iterators[0].getRow();
                    row2 = iterators[1].getRow();
                    if(ByteUtil.compare((byte[])row1[measureCount],
                            (byte[])row2[measureCount]) > 0)
                    {
                        dataHandler.addDataToStore(row2);
                        iterators[1].next();
                        counter++;
                    }
                    else
                    {
                        dataHandler.addDataToStore(row1);
                        iterators[0].next();
                        counter++;
                    }
                }
                while(iterators[0].hasNext())
                {
                    dataHandler.addDataToStore(iterators[0].getRow());
                    iterators[0].next();
                    counter++;
                }
                while(iterators[1].hasNext())
                {
                    dataHandler.addDataToStore(iterators[1].getRow());
                    iterators[1].next();
                    counter++;
                }
                LOGGER.info(
                        MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                        "************************************************ Total number of records processed"
                                + counter);
            }

            else if(producerCounter == 1)
            {
                while(iterators[0].hasNext())
                {
                    dataHandler.addDataToStore(iterators[0].getRow());
                    iterators[0].next();
                    counter++;
                }
                LOGGER.info(
                        MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                        "************************************************ Total number of records processed"
                                + counter);
            }
            else
            {
                createRecordHolderQueue(iterators);
                initialiseHeap(iterators);
                fillBuffer();
            }
            //CHECKSTYLE:ON
        }
        catch(Exception e)
        {
            LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, e);
            throw new MolapDataWriterException(e.getMessage(),e);
        }
        
        return null;
    }

    /**
     * below method will be used to initialise the heap
     * @param iterators
     * @throws MolapSortKeyException
     */
    private void initialiseHeap(RecordIterator[] iterators)
            throws MolapSortKeyException
    {
      //CHECKSTYLE:OFF    Approval No:Approval-V3R8C00_015
        for(RecordIterator iterator : iterators)
        {
            if(iterator.hasNext())
            {
                this.recordHolderHeap.add(iterator);
            }
        }
      //CHECKSTYLE:ON
    }
  //CHECKSTYLE:OFF    Approval No:Approval-V3R8C00_021
    /**
     * This method will be used to get the sorted record from file
     * 
     * @return sorted record sorted record
     * @throws MolapDataWriterException
     * 
     */
    private void fillBuffer() throws MolapDataWriterException
    {
        int counter=0;
        while(producerCounter > 0)
        {
            Object[] row = null;
            // poll the top object from heap
            // heap maintains binary tree which is based on heap condition
            // that
            // will
            // be based on comparator we are passing the heap
            // when will call poll it will always delete root of the tree
            // and
            // then
            // it does trickel down operation complexity is log(n)
            RecordIterator poll = this.recordHolderHeap.poll();
            row = poll.getRow();
            poll.next();
            // get the row from chunk
            // check if there no entry present
            if(!poll.hasNext())
            {
                // this.dataWriterStep.processRow(row);
                // putRow(data.getOutputRowMeta(), row);
                dataHandler.addDataToStore(row);
                counter++;
                producerCounter--;
                continue;
            }
            // putRow(data.getOutputRowMeta(), row);
            dataHandler.addDataToStore(row);
            counter++;
            this.recordHolderHeap.add(poll);
        }
      
        LOGGER.info(
                MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                "************************************************ Total number of records processed"
                        + counter);
        // return row
    }

    //TODO SIMIAN
    /**
     * This method will be used to create the heap which will be used to
     * hold the chunk of data
     * 
     * @param listFiles
     *            list of temp files
     * 
     */
    private void createRecordHolderQueue(RecordIterator[] iteratorsLocal)
    {
        // creating record holder heap
        this.recordHolderHeap = new PriorityQueue<RecordIterator>(
                iteratorsLocal.length, new Comparator<RecordIterator>()
                {
                    public int compare(RecordIterator recordItr1, 
                            RecordIterator recordItr2) 
                    {
                        byte[] b1 = (byte[])recordItr1.getRow()[mdKeyIndex];
                        byte[] b2 = (byte[])recordItr2.getRow()[mdKeyIndex];
                        int cmpVal = 0;
                        int a = 0;
                        int b = 0;
                        for(int i = 0;i < b2.length;i++)
                        {
                            a = b1[i] & 0xFF;
                            b = b2[i] & 0xFF;
                            cmpVal = a - b; 
                            if(cmpVal != 0)
                            {
                                return cmpVal;
                            }
                        }
                        return cmpVal;
                    }
                });
    }

}
