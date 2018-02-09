/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.carbondata.core.scan.scanner;

import java.io.IOException;

import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.processor.BlocksChunkHolder;
import org.apache.carbondata.core.scan.result.AbstractScannedResult;

/**
 * Interface for processing the block
 * Processing can be filter based processing or non filter based processing
 */
public interface BlockletScanner {

  /**
   * Checks whether this blocklet required to scan or not based on min max of each blocklet.
   * @param blocksChunkHolder
   * @return
   * @throws IOException
   */
  boolean isScanRequired(BlocksChunkHolder blocksChunkHolder) throws IOException;

  /**
   * Below method will used to process the block data and get the scanned result
   *
   * @param blocksChunkHolder block chunk which holds the block data
   * @return scannerResult
   * result after processing
   */
  AbstractScannedResult scanBlocklet(BlocksChunkHolder blocksChunkHolder)
      throws IOException, MemoryException, FilterUnsupportedException;

  /**
   * Just reads the blocklet from file, does not uncompress it.
   * @param blocksChunkHolder
   */
  void readBlocklet(BlocksChunkHolder blocksChunkHolder) throws IOException;

  /**
   * In case if there is no filter satisfies.
   * @return AbstractScannedResult
   */
  AbstractScannedResult createEmptyResult();
}
