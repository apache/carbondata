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

package org.apache.carbondata.examples.rtree;


import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.cache.Cacheable;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.dev.DataMap;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.fileoperations.AtomicFileOperations;
import org.apache.carbondata.core.fileoperations.AtomicFileOperationsImpl;
import org.apache.carbondata.core.indexstore.Blocklet;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.util.CarbonUtil;

import com.google.gson.Gson;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.index.strtree.STRtree;

public class RTreeDataMap implements DataMap, Cacheable {
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(RTreeDataMap.class.getName());

  public static final String NAME = "unclustered.rtree.blocklet";

  private String filePath;

  private STRtree rtree;

  @Override
  public void init(String filePath) throws MemoryException, IOException {
    this.filePath = filePath;
    CarbonFile carbonFile = FileFactory.getCarbonFile(
        filePath.substring(0, filePath.lastIndexOf("/") + 1));
    CarbonFile[] listFiles = carbonFile.listFiles(new CarbonFileFilter() {
      @Override
      public boolean accept(CarbonFile file) {
        return file.getName().endsWith(".rtreeindex");
      }
    });
    this.rtree = new STRtree(25);
    for (int i = 0; i < listFiles.length; ++i) {
      String index_path = listFiles[i].getPath();
      Gson gsonObjectToRead = new Gson();
      DataInputStream dataInputStream = null;
      BufferedReader buffReader = null;
      InputStreamReader inStream = null;
      AtomicFileOperations fileOperation =
          new AtomicFileOperationsImpl(index_path, FileFactory.getFileType(index_path));
      try {
        if (!FileFactory.isFileExist(index_path, FileFactory.getFileType(index_path))) {
          throw new IOException("cannot find index file");
        }
        dataInputStream = fileOperation.openForRead();
        inStream = new InputStreamReader(dataInputStream,
            CarbonCommonConstants.CARBON_DEFAULT_STREAM_ENCODEFORMAT);
        buffReader = new BufferedReader(inStream);
        BlockletBoundingBox[] boxes =
            gsonObjectToRead.fromJson(buffReader, BlockletBoundingBox[].class);
        for (BlockletBoundingBox box : boxes) {
          BlockletBoundingBox now = boxes[i];
          rtree.insert(new Envelope(now.lowx, now.highx, now.lowy, now.highy), now);
        }
      } catch (IOException e) {
        LOGGER.info(e.getMessage());
      } finally {
        CarbonUtil.closeStreams(buffReader, inStream, dataInputStream);
      }
    }
  }

  @Override
  public List<Blocklet> prune(FilterResolverIntf filterExp) {
    return null;
  }

  @Override
  public void clear() {
    rtree = null;
  }

  @Override
  public long getFileTimeStamp() {
    return 0;
  }

  @Override
  public int getAccessCount() {
    return 0;
  }

  @Override
  public long getMemorySize() {
    return 0;
  }
}
