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

package org.apache.carbondata.datamap.examples;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.dev.DataMap;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.fileoperations.AtomicFileOperations;
import org.apache.carbondata.core.fileoperations.AtomicFileOperationsImpl;
import org.apache.carbondata.core.indexstore.Blocklet;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.scan.filter.FilterUtil;
import org.apache.carbondata.core.scan.filter.executer.FilterExecuter;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.util.CarbonUtil;

import com.google.gson.Gson;

/**
 * Datamap implementation for min max blocklet.
 */
public class MinMaxDataMap implements DataMap {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(MinMaxDataMap.class.getName());

  public static final String NAME = "clustered.minmax.btree.blocklet";

  private String filePath;

  private MinMaxIndexBlockDetails[] readMinMaxDataMap;

  @Override public void init(String filePath) throws MemoryException, IOException {
    this.filePath = filePath;
    CarbonFile[] listFiles = getCarbonMinMaxIndexFiles(filePath, "0");
    for (int i = 0; i < listFiles.length; i++) {
      readMinMaxDataMap = readJson(listFiles[i].getPath());
    }
  }

  private CarbonFile[] getCarbonMinMaxIndexFiles(String filePath, String segmentId) {
    String path = filePath.substring(0, filePath.lastIndexOf("/") + 1);
    CarbonFile carbonFile = FileFactory.getCarbonFile(path);
    return carbonFile.listFiles(new CarbonFileFilter() {
      @Override public boolean accept(CarbonFile file) {
        return file.getName().endsWith(".minmaxindex");
      }
    });
  }

  public MinMaxIndexBlockDetails[] readJson(String filePath) throws IOException {
    Gson gsonObjectToRead = new Gson();
    DataInputStream dataInputStream = null;
    BufferedReader buffReader = null;
    InputStreamReader inStream = null;
    MinMaxIndexBlockDetails[] readMinMax = null;
    AtomicFileOperations fileOperation =
        new AtomicFileOperationsImpl(filePath, FileFactory.getFileType(filePath));

    try {
      if (!FileFactory.isFileExist(filePath, FileFactory.getFileType(filePath))) {
        return null;
      }
      dataInputStream = fileOperation.openForRead();
      inStream = new InputStreamReader(dataInputStream,
          CarbonCommonConstants.CARBON_DEFAULT_STREAM_ENCODEFORMAT);
      buffReader = new BufferedReader(inStream);
      readMinMax = gsonObjectToRead.fromJson(buffReader, MinMaxIndexBlockDetails[].class);
    } catch (IOException e) {
      return null;
    } finally {
      CarbonUtil.closeStreams(buffReader, inStream, dataInputStream);
    }
    return readMinMax;
  }

  /**
   * Block Prunning logic for Min Max DataMap.
   *
   * @param filterExp
   * @param segmentProperties
   * @return
   */
  @Override public List<Blocklet> prune(FilterResolverIntf filterExp,
      SegmentProperties segmentProperties) {
    List<Blocklet> blocklets = new ArrayList<>();

    if (filterExp == null) {
      for (int i = 0; i < readMinMaxDataMap.length; i++) {
        blocklets.add(new Blocklet(readMinMaxDataMap[i].getFilePath(),
            String.valueOf(readMinMaxDataMap[i].getBlockletId())));
      }
    } else {
      FilterExecuter filterExecuter =
          FilterUtil.getFilterExecuterTree(filterExp, segmentProperties, null);
      int startIndex = 0;
      while (startIndex < readMinMaxDataMap.length) {
        BitSet bitSet = filterExecuter.isScanRequired(readMinMaxDataMap[startIndex].getMaxValues(),
            readMinMaxDataMap[startIndex].getMinValues());
        if (!bitSet.isEmpty()) {
          blocklets.add(new Blocklet(readMinMaxDataMap[startIndex].getFilePath(),
              String.valueOf(readMinMaxDataMap[startIndex].getBlockletId())));
        }
        startIndex++;
      }
    }
    return blocklets;
  }

  @Override
  public void clear() {
    readMinMaxDataMap = null;
  }

}