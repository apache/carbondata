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
package org.carbondata.query.writer;

import java.util.AbstractQueue;
import java.util.Comparator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.carbondata.query.executer.Tuple;
import org.carbondata.query.result.Result;
import org.carbondata.query.schema.metadata.DataProcessorInfo;
import org.carbondata.query.writer.exception.ResultWriterException;

/**
 * Project Name  : Carbon
 * Module Name   : CARBON Data Processor
 * Author    : R00903928,k00900841
 * Created Date  : 27-Aug-2015
 * FileName   : WriterExecutor.java
 * Description   : This is the executor class which is responsible for creating the
 * ExecutorService and submitting the Result Writer callable object.
 * Class Version  : 1.0
 */
public class WriterExecutor {
  private ExecutorService execService;

  /**
   * Constructor.
   */
  public WriterExecutor() {
    execService = Executors.newFixedThreadPool(2);
  }

  /**
   * This method is used to invoke the writer based on the scanned result.
   *
   * @param scannedResult
   * @param comparator
   * @param writerVo
   * @param outLocation
   */
  public void writeResult(Result scannedResult, Comparator comparator,
      DataProcessorInfo dataProcessorInfo, String outLocation) {
    execService.submit(
        new ScannedResultDataFileWriterThread(scannedResult, dataProcessorInfo, comparator,
            outLocation));
  }

  /**
   * This method is used to invoke the writer based on the data heap.
   *
   * @param dataHeap
   * @param writerVo
   * @param outLocation
   */
  public void writeResult(AbstractQueue<Tuple> dataHeap, DataProcessorInfo writerVo,
      String outLocation) {
    execService.submit(new HeapBasedDataFileWriterThread(dataHeap, writerVo, outLocation));
  }

  /**
   * This method will wait till the writer threads are finished.
   *
   * @throws ResultWriterException
   */
  public void closeWriter() throws ResultWriterException {
    execService.shutdown();
    try {
      execService.awaitTermination(2, TimeUnit.DAYS);
    } catch (InterruptedException e) {
      throw new ResultWriterException(e);
    }
  }

}
