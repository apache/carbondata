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
package org.carbondata.query.carbon.scanner;

import org.carbondata.query.carbon.executor.exception.QueryExecutionException;
import org.carbondata.query.carbon.processor.BlocksChunkHolder;
import org.carbondata.query.carbon.result.AbstractScannedResult;

/**
 * Interface for processing the block
 * Processing can be filter based processing or non filter based processing
 */
public interface BlockletScanner {

  /**
   * Below method will used to process the block data and get the scanned result
   *
   * @param blocksChunkHolder block chunk which holds the block data
   * @return scannerResult
   * result after processing
   * @throws QueryExecutionException
   */
  AbstractScannedResult scanBlocklet(BlocksChunkHolder blocksChunkHolder)
      throws QueryExecutionException;
}
