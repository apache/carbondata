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

package org.carbondata.query.result.iterator;

import java.io.IOException;
import java.util.List;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.iterator.CarbonIterator;
import org.carbondata.core.metadata.BlockletInfo;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.query.executer.pagination.impl.QueryResult;
import org.carbondata.query.reader.QueryDataFileReader;
import org.carbondata.query.reader.exception.ResultReaderException;
import org.carbondata.query.schema.metadata.DataProcessorInfo;

/**
 * provides the iterator over the blocklet and return the query result.
 */
public class FileBasedResultIteartor implements CarbonIterator<QueryResult> {
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(FileBasedResultIteartor.class.getName());
  /**
   * blockletInfos
   */
  private List<BlockletInfo> blockletInfos;
  private int counter;
  private QueryDataFileReader carbonQueryDataFileReader;
  private boolean hasNext;

  public FileBasedResultIteartor(String path, DataProcessorInfo info) {
    readBlockletInfo(path, info);
    carbonQueryDataFileReader = new QueryDataFileReader(path, info);
  }

  private void readBlockletInfo(String path, DataProcessorInfo info) {
    CarbonFile carbonFile = FileFactory.getCarbonFile(path, FileFactory.getFileType(path));
    try {
      if (FileFactory.isFileExist(path, FileFactory.getFileType(path))) {
        blockletInfos =
            CarbonUtil.getBlockletInfo(carbonFile, info.getAggType().length, info.getKeySize());
      } else {
        LOGGER.info("file doesnot exist " + path);
      }
      if (blockletInfos.size() > 0) {
        hasNext = true;
      }
    } catch (IOException e) {
      LOGGER.info(e.getMessage());
    }
  }

  @Override public boolean hasNext() {
    return hasNext;
  }

  @Override public QueryResult next() {
    QueryResult prepareResultFromFile = null;
    try {
      prepareResultFromFile =
          carbonQueryDataFileReader.prepareResultFromFile(blockletInfos.get(counter));
    } catch (ResultReaderException e) {
      carbonQueryDataFileReader.close();
    }
    counter++;
    if (counter >= blockletInfos.size()) {
      hasNext = false;
      carbonQueryDataFileReader.close();
    }
    return prepareResultFromFile;
  }

}
