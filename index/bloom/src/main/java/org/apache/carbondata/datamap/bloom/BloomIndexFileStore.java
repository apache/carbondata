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

package org.apache.carbondata.datamap.bloom;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.util.CarbonUtil;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.bloom.CarbonBloomFilter;
import org.apache.log4j.Logger;

/**
 * This class works for merging and loading bloom index
 */
@InterfaceAudience.Internal
public class BloomIndexFileStore {

  private static final Logger LOGGER =
          LogServiceFactory.getLogService(BloomIndexFileStore.class.getName());

  // suffix of original generated file
  public static final String BLOOM_INDEX_SUFFIX = ".bloomindex";
  // suffix of merged bloom index file
  public static final String MERGE_BLOOM_INDEX_SUFFIX = ".bloomindexmerge";
  // directory to store merged bloom index files
  public static final String MERGE_BLOOM_INDEX_SHARD_NAME = "mergeShard";
  /**
   * flag file for merging
   * if flag file exists, query won't use mergeShard
   * if flag file not exists and mergeShard generated, query will use mergeShard
   */
  public static final String MERGE_INPROGRESS_FILE = "mergeShard.inprogress";

  public static void mergeBloomIndexFile(String dmSegmentPathString, List<String> indexCols) {

    // Step 1. check current folders

    // get all shard paths of old store
    CarbonFile segmentPath = FileFactory.getCarbonFile(dmSegmentPathString);
    CarbonFile[] shardPaths = segmentPath.listFiles(new CarbonFileFilter() {
      @Override
      public boolean accept(CarbonFile file) {
        return file.isDirectory() && !file.getName().equals(MERGE_BLOOM_INDEX_SHARD_NAME);
      }
    });

    String mergeShardPath = dmSegmentPathString + File.separator + MERGE_BLOOM_INDEX_SHARD_NAME;
    String mergeInprogressFile = dmSegmentPathString + File.separator + MERGE_INPROGRESS_FILE;

    // Step 2. prepare for fail-safe merging

    try {
      // delete mergeShard folder if exists
      if (FileFactory.isFileExist(mergeShardPath)) {
        FileFactory.deleteFile(mergeShardPath);
      }
      // create flag file before creating mergeShard folder
      if (!FileFactory.isFileExist(mergeInprogressFile)) {
        FileFactory.createNewFile(mergeInprogressFile);
      }
      // create mergeShard output folder
      if (!FileFactory.mkdirs(mergeShardPath)) {
        throw new RuntimeException("Failed to create directory " + mergeShardPath);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    // Step 3. merge index files
    // Query won't use mergeShard until MERGE_INPROGRESS_FILE is deleted

    // for each index column, merge the bloomindex files from all shards into one
    for (String indexCol: indexCols) {
      String mergeIndexFile = getMergeBloomIndexFile(mergeShardPath, indexCol);
      DataInputStream dataInputStream = null;
      DataOutputStream dataOutputStream = null;
      try {
        FileFactory.createNewFile(mergeIndexFile);
        dataOutputStream = FileFactory.getDataOutputStream(
            mergeIndexFile);
        for (CarbonFile shardPath : shardPaths) {
          String bloomIndexFile = getBloomIndexFile(shardPath.getCanonicalPath(), indexCol);
          dataInputStream = FileFactory.getDataInputStream(bloomIndexFile);
          byte[] fileData = new byte[(int) FileFactory.getCarbonFile(bloomIndexFile).getSize()];
          dataInputStream.readFully(fileData);
          byte[] shardName = shardPath.getName().getBytes(Charset.forName("UTF-8"));
          dataOutputStream.writeInt(shardName.length);
          dataOutputStream.write(shardName);
          dataOutputStream.writeInt(fileData.length);
          dataOutputStream.write(fileData);
          CarbonUtil.closeStream(dataInputStream);
        }
      } catch (IOException e) {
        LOGGER.error("Error occurs while merge bloom index file of column: " + indexCol, e);
        // if any column failed, delete merge shard for this segment and exit
        FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(mergeShardPath));
        FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(mergeInprogressFile));
        throw new RuntimeException(
            "Error occurs while merge bloom index file of column: " + indexCol, e);
      } finally {
        CarbonUtil.closeStreams(dataInputStream, dataOutputStream);
      }
    }

    // Step 4. delete flag file and mergeShard can be used
    try {
      FileFactory.deleteFile(mergeInprogressFile);
    } catch (IOException e) {
      LOGGER.error("Error occurs while deleting file " + mergeInprogressFile, e);
      throw new RuntimeException("Error occurs while deleting file " + mergeInprogressFile);
    }
    // remove old store
    for (CarbonFile shardpath: shardPaths) {
      FileFactory.deleteAllCarbonFilesOfDir(shardpath);
    }
  }

  /**
   * load bloom filter from bloom index file
   */
  public static List<CarbonBloomFilter> loadBloomFilterFromFile(
          String shardPath, String colName) {
    if (shardPath.endsWith(MERGE_BLOOM_INDEX_SHARD_NAME)) {
      return loadMergeBloomIndex(shardPath, colName);
    } else {
      return loadBloomIndex(shardPath, colName);
    }
  }

  /**
   * load bloom filter of {@code colName} from {@code shardPath}
   */
  public static List<CarbonBloomFilter> loadBloomIndex(
          String shardPath, String colName) {
    DataInputStream dataInStream = null;
    List<CarbonBloomFilter> bloomFilters = new ArrayList<>();
    try {
      String indexFile = getBloomIndexFile(shardPath, colName);
      dataInStream = FileFactory.getDataInputStream(indexFile);
      while (dataInStream.available() > 0) {
        CarbonBloomFilter bloomFilter = new CarbonBloomFilter();
        bloomFilter.readFields(dataInStream);
        bloomFilter.setShardName(new Path(shardPath).getName());
        bloomFilters.add(bloomFilter);
      }
      LOGGER.info(String.format("Read %d bloom indices from %s", bloomFilters.size(), indexFile));

      return bloomFilters;
    } catch (IOException e) {
      LOGGER.error("Error occurs while reading bloom index", e);
      throw new RuntimeException("Error occurs while reading bloom index", e);
    } finally {
      CarbonUtil.closeStreams(dataInStream);
    }
  }

  /**
   * load bloom filter of {@code colName} from {@code mergeShardPath}
   */
  public static List<CarbonBloomFilter> loadMergeBloomIndex(
          String mergeShardPath, String colName) {
    String mergeIndexFile = getMergeBloomIndexFile(mergeShardPath, colName);
    DataInputStream mergeIndexInStream = null;
    List<CarbonBloomFilter> bloomFilters = new ArrayList<>();
    try {
      mergeIndexInStream = FileFactory.getDataInputStream(mergeIndexFile);
      while (mergeIndexInStream.available() > 0) {
        // read shard name
        int shardNameByteLength = mergeIndexInStream.readInt();
        byte[] shardNameBytes = new byte[shardNameByteLength];
        mergeIndexInStream.readFully(shardNameBytes);
        String shardName = new String(shardNameBytes, Charset.forName("UTF-8"));
        // read bloom index file data
        int indexFileByteLength = mergeIndexInStream.readInt();
        byte[] indexFileBytes = new byte[indexFileByteLength];
        mergeIndexInStream.readFully(indexFileBytes);
        // warp byte array as input stream to get bloom filters
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(indexFileBytes);
        DataInputStream indexDataInStream = new DataInputStream(byteArrayInputStream);
        while (indexDataInStream.available() > 0) {
          CarbonBloomFilter bloomFilter = new CarbonBloomFilter();
          bloomFilter.readFields(indexDataInStream);
          bloomFilter.setShardName(shardName);
          bloomFilters.add(bloomFilter);
        }
      }
      LOGGER.info(
          String.format("Read %d bloom indices from %s", bloomFilters.size(), mergeIndexFile));
      return bloomFilters;
    } catch (IOException e) {
      LOGGER.error("Error occurs while reading merge bloom index", e);
      throw new RuntimeException("Error occurs while reading merge bloom index", e);
    } finally {
      CarbonUtil.closeStreams(mergeIndexInStream);
    }
  }

  /**
   * get bloom index file
   */
  public static String getBloomIndexFile(String shardPath, String colName) {
    return shardPath.concat(File.separator).concat(colName).concat(BLOOM_INDEX_SUFFIX);
  }

  /**
   * get merge bloom index file
   */
  public static String getMergeBloomIndexFile(String mergeShardPath, String colName) {
    return mergeShardPath.concat(File.separator).concat(colName).concat(MERGE_BLOOM_INDEX_SUFFIX);
  }
}
