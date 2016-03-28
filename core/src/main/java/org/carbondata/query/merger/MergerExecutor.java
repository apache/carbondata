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
 */
package org.carbondata.query.merger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.carbondata.query.processor.DataProcessor;
import org.carbondata.query.schema.metadata.DataProcessorInfo;
import org.carbondata.query.writer.exception.ResultWriterException;

/**
 * Project Name  : Carbon
 * Module Name   : CARBON Data Processor
 * Author    : R00903928,k00900841
 * Created Date  : 27-Aug-2015
 * FileName   : MergerExecutor.java
 * Description   : This is the executor class for merge. This is responsible for
 * creating executor service for merge and running the callable object.
 * Class Version  : 1.0
 */
public class MergerExecutor {
    /**
     * execService Service for maintaining thread pool.
     */
    private ExecutorService execService;

    public MergerExecutor() {
        execService = Executors.newFixedThreadPool(2);
    }

    public void mergeResult(DataProcessor processor, DataProcessorInfo info, CarbonFile[] files) {
        if (!info.isSortedData()) {
            execService.submit(new UnSortedResultMerger(processor, info, files));
        } else {
            execService.submit(new SortedResultFileMerger(processor, info, files));
        }
    }

    public void mergeFinalResult(DataProcessor processor, DataProcessorInfo info, CarbonFile[] files)
            throws Exception {
        if (!info.isSortedData()) {
            new UnSortedResultMerger(processor, info, files).call();
        } else {
            new SortedResultFileMerger(processor, info, files).call();
        }
    }

    public void closeMerger() throws ResultWriterException {
        execService.shutdown();
        try {
            execService.awaitTermination(2, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            throw new ResultWriterException(e);
        }
    }

}
