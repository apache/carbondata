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
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datamap.dev.DataMapModel;
import org.apache.carbondata.core.datamap.dev.cgdatamap.CoarseGrainDataMap;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.fileoperations.AtomicFileOperations;
import org.apache.carbondata.core.fileoperations.AtomicFileOperationFactory;
import org.apache.carbondata.core.indexstore.Blocklet;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.scan.filter.FilterUtil;
import org.apache.carbondata.core.scan.filter.executer.FilterExecuter;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.util.CarbonUtil;

import com.google.gson.Gson;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * Datamap implementation for min max blocklet.
 */
public class MinMaxIndexDataMap extends CoarseGrainDataMap {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(MinMaxIndexDataMap.class.getName());

  private String[] indexFilePath;

  private MinMaxIndexBlockDetails[][] readMinMaxDataMap;

  @Override
  public void init(DataMapModel model) throws MemoryException, IOException {
    Path indexPath = FileFactory.getPath(model.getFilePath());

    FileSystem fs = FileFactory.getFileSystem(indexPath);
    if (!fs.exists(indexPath)) {
      throw new IOException(
          String.format("Path %s for MinMax index dataMap does not exist", indexPath));
    }
    if (!fs.isDirectory(indexPath)) {
      throw new IOException(
          String.format("Path %s for MinMax index dataMap must be a directory", indexPath));
    }

    FileStatus[] indexFileStatus = fs.listStatus(indexPath, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.getName().endsWith(".minmaxindex");
      }
    });

    this.indexFilePath = new String[indexFileStatus.length];
    this.readMinMaxDataMap = new MinMaxIndexBlockDetails[indexFileStatus.length][];
    for (int i = 0; i < indexFileStatus.length; i++) {
      this.indexFilePath[i] = indexFileStatus[i].getPath().toString();
      this.readMinMaxDataMap[i] = readJson(this.indexFilePath[i]);
    }
  }

  private MinMaxIndexBlockDetails[] readJson(String filePath) {
    Gson gsonObjectToRead = new Gson();
    DataInputStream dataInputStream = null;
    BufferedReader buffReader = null;
    InputStreamReader inStream = null;
    MinMaxIndexBlockDetails[] readMinMax = null;
    AtomicFileOperations fileOperation =
        AtomicFileOperationFactory.getAtomicFileOperations(filePath);

    try {
      if (!FileFactory.isFileExist(filePath)) {
        return null;
      }
      dataInputStream = fileOperation.openForRead();
      inStream = new InputStreamReader(dataInputStream, "UTF-8");
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
  @Override
  public List<Blocklet> prune(FilterResolverIntf filterExp,
      SegmentProperties segmentProperties, List<PartitionSpec> partitions) {
    List<Blocklet> blocklets = new ArrayList<>();

    if (filterExp == null) {
      for (int i = 0; i < readMinMaxDataMap.length; i++) {
        for (int j = 0; j < readMinMaxDataMap[i].length; j++) {
          blocklets.add(new Blocklet(indexFilePath[i],
              String.valueOf(readMinMaxDataMap[i][j].getBlockletId())));
        }
      }
    } else {
      FilterExecuter filterExecuter =
          FilterUtil.getFilterExecuterTree(filterExp, segmentProperties, null, false);
      for (int blkIdx = 0; blkIdx < readMinMaxDataMap.length; blkIdx++) {
        for (int blkltIdx = 0; blkltIdx < readMinMaxDataMap[blkIdx].length; blkltIdx++) {

          BitSet bitSet = filterExecuter.isScanRequired(
              readMinMaxDataMap[blkIdx][blkltIdx].getMaxValues(),
              readMinMaxDataMap[blkIdx][blkltIdx].getMinValues(), null);
          if (!bitSet.isEmpty()) {
            String blockFileName = indexFilePath[blkIdx].substring(
                indexFilePath[blkIdx].lastIndexOf(File.separatorChar) + 1,
                indexFilePath[blkIdx].indexOf(".minmaxindex"));
            Blocklet blocklet = new Blocklet(blockFileName,
                String.valueOf(readMinMaxDataMap[blkIdx][blkltIdx].getBlockletId()));
            LOGGER.info(String.format("MinMaxDataMap: Need to scan block#%s -> blocklet#%s, %s",
                blkIdx, blkltIdx, blocklet));
            blocklets.add(blocklet);
          } else {
            LOGGER.info(String.format("MinMaxDataMap: Skip scan block#%s -> blocklet#%s",
                blkIdx, blkltIdx));
          }
        }
      }
    }
    return blocklets;
  }

  @Override
  public boolean isScanRequired(FilterResolverIntf filterExp) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
    readMinMaxDataMap = null;
  }

  @Override
  public void finish() {

  }

  @Override
  public int getNumberOfEntries() {
    // keep default, one record in one datamap
    return 1;
  }
}
