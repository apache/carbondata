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
package org.carbondata.query.carbon.processor;

import org.carbondata.query.carbon.executor.infos.BlockExecutionInfo;
import org.carbondata.query.carbon.result.AbstractScannedResult;
import org.carbondata.query.carbon.scanner.BlocksChunkHolder;

public abstract class AbstractDataBlocksProcessor implements BlockletScanner {

  protected AbstractScannedResult scannedResult;

  protected BlockExecutionInfo tableBlockExecutionInfos;

  public AbstractDataBlocksProcessor(BlockExecutionInfo tableBlockExecutionInfos) {
    this.tableBlockExecutionInfos = tableBlockExecutionInfos;
  }

  @Override public AbstractScannedResult processBlockData(BlocksChunkHolder blocksChunkHolder) {
    fillKeyValue(blocksChunkHolder);
    return scannedResult;
  }

  protected void fillKeyValue(BlocksChunkHolder blocksChunkHolder) {
    scannedResult.reset();
    scannedResult.setMeasureChunks(blocksChunkHolder.getDataBlock()
        .getMeasureChunks(blocksChunkHolder.getFileReader(),
            tableBlockExecutionInfos.getAllSelectedMeasureBlocksIndexes()));
    scannedResult.setNumberOfRows(blocksChunkHolder.getDataBlock().nodeSize());

    scannedResult.setDimensionChunks(blocksChunkHolder.getDataBlock()
        .getDimensionChunks(blocksChunkHolder.getFileReader(),
            tableBlockExecutionInfos.getAllSelectedDimensionBlocksIndexes()));
  }
}
