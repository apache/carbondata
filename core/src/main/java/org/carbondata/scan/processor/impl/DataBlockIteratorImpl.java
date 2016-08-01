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
package org.carbondata.scan.processor.impl;

import java.util.List;

import org.carbondata.core.datastorage.store.FileHolder;
import org.carbondata.scan.executor.infos.BlockExecutionInfo;
import org.carbondata.scan.processor.AbstractDataBlockIterator;

/**
 * Below class will be used to process the block for detail query
 */
public class DataBlockIteratorImpl extends AbstractDataBlockIterator {

  /**
   * DataBlockIteratorImpl Constructor
   *
   * @param blockExecutionInfo execution information
   */
  public DataBlockIteratorImpl(BlockExecutionInfo blockExecutionInfo, FileHolder fileReader,
      int batchSize) {
    super(blockExecutionInfo, fileReader, batchSize);
  }

  /**
   * It scans the block and returns the result with @batchSize
   *
   * @return Result of @batchSize
   */
  public List<Object[]> next() {
    List<Object[]> collectedResult =
        this.scannerResultAggregator.collectData(scannedResult, batchSize);
    while (collectedResult.size() < batchSize && hasNext()) {
      List<Object[]> data = this.scannerResultAggregator
          .collectData(scannedResult, batchSize - collectedResult.size());
      collectedResult.addAll(data);
    }
    return collectedResult;
  }

}
