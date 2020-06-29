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

package org.apache.carbondata.index.examples;

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
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.index.dev.IndexModel;
import org.apache.carbondata.core.index.dev.cgindex.CoarseGrainIndex;
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
 * Index implementation for min max blocklet.
 */
public class MinMaxIndex extends CoarseGrainIndex {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(MinMaxIndex.class.getName());
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765

  private String[] indexFilePath;

  private MinMaxIndexBlockDetails[][] readMinMaxIndex;

  @Override
  public void init(IndexModel model) throws MemoryException, IOException {
    Path indexPath = FileFactory.getPath(model.getFilePath());

    FileSystem fs = FileFactory.getFileSystem(indexPath);
    if (!fs.exists(indexPath)) {
      throw new IOException(
          String.format("Path %s for MinMax index does not exist", indexPath));
    }
    if (!fs.isDirectory(indexPath)) {
      throw new IOException(
          String.format("Path %s for MinMax index must be a directory", indexPath));
    }

    FileStatus[] indexFileStatus = fs.listStatus(indexPath, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.getName().endsWith(".minmaxindex");
      }
    });

    this.indexFilePath = new String[indexFileStatus.length];
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765
    this.readMinMaxIndex = new MinMaxIndexBlockDetails[indexFileStatus.length][];
    for (int i = 0; i < indexFileStatus.length; i++) {
      this.indexFilePath[i] = indexFileStatus[i].getPath().toString();
      this.readMinMaxIndex[i] = readJson(this.indexFilePath[i]);
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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1544
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
   * Block Prunning logic for Min Max Index.
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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765
      for (int i = 0; i < readMinMaxIndex.length; i++) {
        for (int j = 0; j < readMinMaxIndex[i].length; j++) {
          blocklets.add(new Blocklet(indexFilePath[i],
              String.valueOf(readMinMaxIndex[i][j].getBlockletId())));
        }
      }
    } else {
      FilterExecuter filterExecuter =
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3611
          FilterUtil.getFilterExecuterTree(filterExp, segmentProperties, null, false);
      for (int blkIdx = 0; blkIdx < readMinMaxIndex.length; blkIdx++) {
        for (int blkltIdx = 0; blkltIdx < readMinMaxIndex[blkIdx].length; blkltIdx++) {

          BitSet bitSet = filterExecuter.isScanRequired(
              readMinMaxIndex[blkIdx][blkltIdx].getMaxValues(),
              readMinMaxIndex[blkIdx][blkltIdx].getMinValues(), null);
          if (!bitSet.isEmpty()) {
            String blockFileName = indexFilePath[blkIdx].substring(
                indexFilePath[blkIdx].lastIndexOf(File.separatorChar) + 1,
                indexFilePath[blkIdx].indexOf(".minmaxindex"));
            Blocklet blocklet = new Blocklet(blockFileName,
                String.valueOf(readMinMaxIndex[blkIdx][blkltIdx].getBlockletId()));
            LOGGER.info(String.format("Need to scan block#%s -> blocklet#%s, %s",
                blkIdx, blkltIdx, blocklet));
            blocklets.add(blocklet);
          } else {
            LOGGER.info(String.format("Skip scan block#%s -> blocklet#%s",
                blkIdx, blkltIdx));
          }
        }
      }
    }
    return blocklets;
  }

  @Override
  public boolean isScanRequired(FilterResolverIntf filterExp) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1544
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765
    readMinMaxIndex = null;
  }

  @Override
  public void finish() {

  }

  @Override
  public int getNumberOfEntries() {
    // keep default, one record in one indexSchema
    return 1;
  }
}
