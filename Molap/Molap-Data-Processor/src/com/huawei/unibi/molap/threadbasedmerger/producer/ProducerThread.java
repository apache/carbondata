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

package com.huawei.unibi.molap.threadbasedmerger.producer;

import java.io.File;
import java.util.AbstractQueue;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.sortandgroupby.exception.MolapSortKeyAndGroupByException;
import com.huawei.unibi.molap.sortandgroupby.sortKey.MolapSortTempFileChunkHolder;
import com.huawei.unibi.molap.threadbasedmerger.container.Container;
import com.huawei.unibi.molap.util.MolapDataProcessorLogEvent;

public class ProducerThread implements Callable<Void> {
    /**
     * LOGGER
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(ProducerThread.class.getName());
    /**
     * molapSortHolderContainer
     */
    private final Container container;
    /**
     * sortTempFiles
     */
    private File[] sortTempFiles;
    /**
     * fileBufferSize
     */
    private int fileBufferSize;
    /**
     * readBufferSize
     */
    private int readBufferSize;
    /**
     * measureCount
     */
    private int measureCount;
    /**
     * mdkeyLenght
     */
    private int mdKeyLength;
    /**
     * fileCounter
     */
    private int fileCounter;
    /**
     * recordHolderHeap
     */
    private AbstractQueue<MolapSortTempFileChunkHolder> recordHolderHeap;

    /**
     * currentBuffer
     */
    private Object[][] currentBuffer;

    /**
     * backUpBuffer
     */
    private Object[][] backUpBuffer;

    /**
     * isCurrentFilled
     */
    private boolean isCurrentFilled;

    /**
     * producer counter
     */
    private int counter;

    /**
     * isFactMdkeyInInputRow
     */
    private boolean isFactMdkeyInInputRow;

    /**
     * factMdkeyLength
     */
    private int factMdkeyLength;

    private char[] type;

    /**
     * Producer Thread constructor
     *
     * @param sortTempFiles
     * @param fileBufferSize
     * @param readBufferSize
     * @param measureCount
     * @param mdKeyLength
     * @param container
     * @param counter
     */
    public ProducerThread(File[] sortTempFiles, int fileBufferSize, int readBufferSize,
            int measureCount, int mdKeyLength, Container container, int counter,
            boolean isFactMdkeyInInputRow, int factMdkeyLength, char[] type) {
        this.sortTempFiles = sortTempFiles;
        this.fileBufferSize = fileBufferSize;
        this.readBufferSize = readBufferSize;
        this.measureCount = measureCount;
        this.mdKeyLength = mdKeyLength;
        this.container = container;
        this.isFactMdkeyInInputRow = isFactMdkeyInInputRow;
        this.factMdkeyLength = factMdkeyLength;
        createRecordHolderQueue(sortTempFiles);
        this.counter = counter;
        this.type = type;
        initialise();
    }

    /**
     * Below method will be used to initialise the produce thread
     */
    private void initialise() {
        try {
            initialiseHeap();
            fillBuffer(false);
            isCurrentFilled = true;

        } catch (MolapSortKeyAndGroupByException e) {
            LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, e,
                    "Proble while creating the heap");
        }
    }

    /**
     * Run method
     */
    @Override public Void call() throws Exception {
        try {
            while (fileCounter > 0 || isCurrentFilled) {
                this.container.fillContainer(currentBuffer);
                isCurrentFilled = false;
                synchronized (container) {
                    this.container.setFilled(true);
                    fillBuffer(true);
                    this.container.wait();
                }
                currentBuffer = backUpBuffer;
                if (currentBuffer != null) {
                    isCurrentFilled = true;
                } else {
                    isCurrentFilled = false;
                }
            }

        } catch (InterruptedException ex) {
            LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, ex);
        } catch (Exception e) {
            LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, e);
            throw e;
        }
        LOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                "Producer Thread: " + this.counter + ": Done");
        this.container.setDone(true);
        return null;
    }

    /**
     * Below method will be for filling the buffer both backup and current
     *
     * @param isForBackupFilling
     */
    private void fillBuffer(boolean isForBackupFilling) {
        Object[][] buffer = null;
        try {
            buffer = getBuffer();
        } catch (MolapSortKeyAndGroupByException e) {
            LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, e);
        }
        if (isForBackupFilling) {
            backUpBuffer = buffer;
        } else {
            currentBuffer = buffer;
        }
    }

    /**
     * This method will be used to get the sorted record from file
     *
     * @return sorted record sorted record
     * @throws MolapSortKeyAndGroupByException
     */
    private Object[][] getBuffer() throws MolapSortKeyAndGroupByException {
        if (fileCounter < 1) {
            return null;
        }
        Object[][] sortRecordHolders = new Object[readBufferSize][];
        int counter = 0;
        Object[] row = null;
        MolapSortTempFileChunkHolder poll = null;
        while (counter < readBufferSize && fileCounter > 0) {

            // poll the top object from heap
            // heap maintains binary tree which is based on heap condition that
            // will
            // be based on comparator we are passing the heap
            // when will call poll it will always delete root of the tree and
            // then
            // it does trickel down operation complexity is log(n)
            poll = this.recordHolderHeap.poll();
            // get the row from chunk
            row = poll.getRow();
            // check if there no entry present
            if (!poll.hasNext()) {
                // if chunk is empty then close the stream
                poll.closeStream();
                // change the file counter
                --this.fileCounter;
                // reaturn row
                // return row;
                sortRecordHolders[counter++] = row;
                continue;
            }
            sortRecordHolders[counter++] = row;
            // read new row
            poll.readRow();
            this.recordHolderHeap.add(poll);
        }
        // return row
        if (counter < readBufferSize) {
            Object[][] temp = new Object[counter][];
            System.arraycopy(sortRecordHolders, 0, temp, 0, temp.length);
            sortRecordHolders = temp;
        }
        return sortRecordHolders;
    }

    /**
     * Below method will be used to initialise the priority heap
     *
     * @throws MolapSortKeyAndGroupByException
     */
    private void initialiseHeap() throws MolapSortKeyAndGroupByException {
        this.fileCounter = this.sortTempFiles.length;
        MolapSortTempFileChunkHolder molapSortTempFileChunkHolder = null;
        //CHECKSTYLE:OFF
        for (File tempFile : this.sortTempFiles) {
            // create chunk holder
            molapSortTempFileChunkHolder =
                    new MolapSortTempFileChunkHolder(tempFile, this.measureCount, this.mdKeyLength,
                            this.fileBufferSize, isFactMdkeyInInputRow, factMdkeyLength,
                            new String[0]);
            // initialize
            molapSortTempFileChunkHolder.initialize();
            molapSortTempFileChunkHolder.readRow();
            // add to heap
            this.recordHolderHeap.add(molapSortTempFileChunkHolder);
        }
        //CHECKSTYLE:ON
    }

    /**
     * This method will be used to create the heap which will be used to hold
     * the chunk of data
     *
     * @param listFiles list of temp files
     */
    private void createRecordHolderQueue(File[] listFiles) {
        // creating record holder heap
        this.recordHolderHeap = new PriorityQueue<MolapSortTempFileChunkHolder>(listFiles.length,
                new Comparator<MolapSortTempFileChunkHolder>() {
                    public int compare(MolapSortTempFileChunkHolder r1,
                            MolapSortTempFileChunkHolder r2) {
                        byte[] b1 = (byte[]) r1.getRow()[measureCount];
                        byte[] b2 = (byte[]) r2.getRow()[measureCount];
                        int cmp = 0;
                        int a = 0;
                        int b = 0;
                        for (int i = 0; i < mdKeyLength; i++) {
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
