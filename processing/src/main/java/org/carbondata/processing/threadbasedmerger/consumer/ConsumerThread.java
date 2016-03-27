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

package org.carbondata.processing.threadbasedmerger.consumer;

import java.util.*;
import java.util.concurrent.Callable;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.processing.sortandgroupby.sortKey.CarbonSortKeyException;
import org.carbondata.processing.threadbasedmerger.container.Container;
import org.carbondata.processing.threadbasedmerger.iterator.RecordIterator;
import org.carbondata.processing.util.CarbonDataProcessorLogEvent;

public class ConsumerThread implements Callable<Void> {
    /**
     * LOGGER
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(ConsumerThread.class.getName());
    /**
     * carbonSortHolderContainer
     */
    private final Container container;
    /**
     * list of producer container
     */
    private List<Container> producerContainer = new ArrayList<Container>(0);
    /**
     * recordHolderHeap
     */
    private AbstractQueue<RecordIterator> recordHolderHeap;
    /**
     * current buffer
     */
    private Object[][] currentBuffer;
    /**
     * backup buffer
     */
    private Object[][] backupBuffer;
    /**
     * read buffer size
     */
    private int readBufferSize;
    /**
     * producer counter
     */
    private int producerCounter;
    /**
     * is current filled to check current buffer is filled or not
     */
    private boolean isCurrentFilled;

    /**
     * consumer number
     */
    private int counter;

    /**
     * mdKeyIndex
     */
    private int mdKeyIndex;

    /**
     * ConsumerThread
     *
     * @param producerContainer
     * @param readBufferSize
     * @param container
     * @param counter
     */
    public ConsumerThread(List<Container> producerContainer, int readBufferSize,
            Container container, int counter, int mdKeyIndex) {
        this.producerContainer = producerContainer;
        this.readBufferSize = readBufferSize;
        this.container = container;
        this.counter = counter;
        this.mdKeyIndex = mdKeyIndex;
    }

    @Override
    public Void call() throws Exception {
        RecordIterator[] iterators = new RecordIterator[producerContainer.size()];
        int i = 0;
        //CHECKSTYLE:OFF
        for (Container container : producerContainer) {
            iterators[i++] = new RecordIterator(container);
        }

        //CHECKSTYLE:ON
        createRecordHolderQueue(iterators);
        try {
            initialiseHeap(iterators);
        } catch (CarbonSortKeyException e) {
            LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG, e,
                    "Problem while initialising the heap");
        }
        try {
            fillBuffer(false);
            isCurrentFilled = true;
        } catch (CarbonSortKeyException e1) {
            LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG, e1,
                    "Problem while Filling the buffer");
        }
        try {
            while (producerCounter > 0 || isCurrentFilled) {
                this.container.fillContainer(currentBuffer);
                isCurrentFilled = false;
                synchronized (this.container) {
                    this.container.setFilled(true);
                    fillBuffer(true);
                    this.container.wait();
                }
                currentBuffer = backupBuffer;
                if (currentBuffer != null) {
                    isCurrentFilled = true;
                } else {
                    isCurrentFilled = false;
                }
            }
        } catch (InterruptedException e) {
            LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG, e);
        } catch (Exception e) {
            LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG, e);
        }
        LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                "Consumer Thread: " + this.counter + ": Done");
        this.container.setDone(true);
        return null;
    }

    private void initialiseHeap(RecordIterator[] iterators) throws CarbonSortKeyException {
        //CHECKSTYLE:OFF
        for (RecordIterator iterator : iterators) {
            if (iterator.hasNext()) {
                this.recordHolderHeap.add(iterator);
                producerCounter++;
            }
        }
        //CHECKSTYLE:ON
    }

    /**
     * This method will be used to get the sorted record from file
     *
     * @return sorted record sorted record
     * @throws CarbonSortKeyException
     */
    private void fillBuffer(boolean isBackupFilling) throws CarbonSortKeyException {
        if (producerCounter < 1) {
            if (isBackupFilling) {
                backupBuffer = null;
            } else {
                currentBuffer = null;
            }
            return;
        }
        Object[][] sortRecordHolders = new Object[readBufferSize][];
        int counter = 0;
        Object[] row = null;
        RecordIterator poll = null;
        while (counter < readBufferSize && producerCounter > 0) {
            // poll the top object from heap
            // heap maintains binary tree which is based on heap condition that
            // will
            // be based on comparator we are passing the heap
            // when will call poll it will always delete root of the tree and
            // then
            // it does trickel down operation complexity is log(n)
            poll = this.recordHolderHeap.poll();
            row = poll.getRow();
            poll.next();
            // get the row from chunk
            // check if there no entry present
            if (!poll.hasNext()) {
                sortRecordHolders[counter++] = row;
                --producerCounter;
                continue;
            }
            sortRecordHolders[counter++] = row;
            this.recordHolderHeap.add(poll);
        }
        // return row
        if (counter < readBufferSize) {
            Object[][] temp = new Object[counter][];
            System.arraycopy(sortRecordHolders, 0, temp, 0, temp.length);
            sortRecordHolders = temp;
        }
        if (isBackupFilling) {
            backupBuffer = sortRecordHolders;
        } else {
            currentBuffer = sortRecordHolders;
        }
    }

    /**
     * This method will be used to create the heap which will be used to hold
     * the chunk of data
     */
    private void createRecordHolderQueue(RecordIterator[] iterators) {
        // creating record holder heap
        this.recordHolderHeap = new PriorityQueue<RecordIterator>(iterators.length,
                new Comparator<RecordIterator>() {
                    public int compare(RecordIterator r1, RecordIterator r2) {
                        byte[] b1 = (byte[]) r1.getRow()[mdKeyIndex];
                        byte[] b2 = (byte[]) r2.getRow()[mdKeyIndex];
                        int cmp = 0;
                        int a = 0;
                        int b = 0;
                        for (int i = 0; i < b2.length; i++) {
                            a = b1[i] & 0xFF;
                            b = b2[i] & 0xFF;
                            cmp = a - b;
                            if (cmp != 0) {
                                return cmp;
                            }
                        }
                        return cmp;
                    }
                });
    }

}
