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

package org.carbondata.processing.store;

import java.util.AbstractQueue;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.util.ByteUtil;
import org.carbondata.processing.sortandgroupby.sortKey.CarbonSortKeyException;
import org.carbondata.processing.store.writer.exception.CarbonDataWriterException;
import org.carbondata.processing.threadbasedmerger.container.Container;
import org.carbondata.processing.threadbasedmerger.iterator.RecordIterator;
import org.carbondata.processing.util.CarbonDataProcessorLogEvent;

public class ProducerCosumerFinalMergerThread implements Callable<Void> {
    /**
     * LOGGER
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(ProducerCosumerFinalMergerThread.class.getName());

    /**
     * dataHandler
     */
    private CarbonFactHandler dataHandler;

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
    public ProducerCosumerFinalMergerThread(CarbonFactHandler dataHanlder, int measureCount,
            int mdKeyIndex, List<Container> producerContainer) {
        this.dataHandler = dataHanlder;
        this.measureCount = measureCount;
        this.mdKeyIndex = mdKeyIndex;
        this.producerContainer = producerContainer;
    }

    @Override
    public Void call() throws Exception {
        RecordIterator[] iterators = new RecordIterator[this.producerContainer.size()];
        RecordIterator iterator = null;
        //CHECKSTYLE:OFF    Approval No:Approval-V3R8C00_015
        for (Container container : producerContainer) {
            iterator = new RecordIterator(container);
            iterators[producerCounter] = iterator;
            producerCounter++;
        }
        int counter = 0;
        try {
            if (producerCounter == 2) {
                Object[] row1 = null;
                Object[] row2 = null;
                while (iterators[0].hasNext() && iterators[1].hasNext()) {
                    row1 = iterators[0].getRow();
                    row2 = iterators[1].getRow();
                    if (ByteUtil.compare((byte[]) row1[measureCount], (byte[]) row2[measureCount])
                            > 0) {
                        dataHandler.addDataToStore(row2);
                        iterators[1].next();
                        counter++;
                    } else {
                        dataHandler.addDataToStore(row1);
                        iterators[0].next();
                        counter++;
                    }
                }
                while (iterators[0].hasNext()) {
                    dataHandler.addDataToStore(iterators[0].getRow());
                    iterators[0].next();
                    counter++;
                }
                while (iterators[1].hasNext()) {
                    dataHandler.addDataToStore(iterators[1].getRow());
                    iterators[1].next();
                    counter++;
                }
                LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                        "************************************************ Total number of records processed"
                                + counter);
            } else if (producerCounter == 1) {
                while (iterators[0].hasNext()) {
                    dataHandler.addDataToStore(iterators[0].getRow());
                    iterators[0].next();
                    counter++;
                }
                LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                        "************************************************ Total number of records processed"
                                + counter);
            } else {
                createRecordHolderQueue(iterators);
                initialiseHeap(iterators);
                fillBuffer();
            }
        } catch (Exception e) {
            LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG, e);
            throw new CarbonDataWriterException(e.getMessage(), e);
        }

        return null;
    }

    /**
     * below method will be used to initialise the heap
     *
     * @param iterators
     * @throws CarbonSortKeyException
     */
    private void initialiseHeap(RecordIterator[] iterators) throws CarbonSortKeyException {
        for (RecordIterator iterator : iterators) {
            if (iterator.hasNext()) {
                this.recordHolderHeap.add(iterator);
            }
        }
    }

    /**
     * This method will be used to get the sorted record from file
     *
     * @return sorted record sorted record
     * @throws CarbonDataWriterException
     */
    private void fillBuffer() throws CarbonDataWriterException {
        int counter = 0;
        while (producerCounter > 0) {
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
            if (!poll.hasNext()) {
                dataHandler.addDataToStore(row);
                counter++;
                producerCounter--;
                continue;
            }
            dataHandler.addDataToStore(row);
            counter++;
            this.recordHolderHeap.add(poll);
        }

        LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                "************************************************ Total number of records processed"
                        + counter);
    }

    /**
     * This method will be used to create the heap which will be used to
     * hold the chunk of data
     */
    private void createRecordHolderQueue(RecordIterator[] iteratorsLocal) {
        // creating record holder heap
        this.recordHolderHeap = new PriorityQueue<RecordIterator>(iteratorsLocal.length,
                new Comparator<RecordIterator>() {
                    public int compare(RecordIterator recordItr1, RecordIterator recordItr2) {
                        byte[] b1 = (byte[]) recordItr1.getRow()[mdKeyIndex];
                        byte[] b2 = (byte[]) recordItr2.getRow()[mdKeyIndex];
                        int cmpVal = 0;
                        int a = 0;
                        int b = 0;
                        for (int i = 0; i < b2.length; i++) {
                            a = b1[i] & 0xFF;
                            b = b2[i] & 0xFF;
                            cmpVal = a - b;
                            if (cmpVal != 0) {
                                return cmpVal;
                            }
                        }
                        return cmpVal;
                    }
                });
    }

}
