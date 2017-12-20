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
package org.apache.carbondata.core.metadata;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.fileoperations.AtomicFileOperations;
import org.apache.carbondata.core.fileoperations.AtomicFileOperationsImpl;
import org.apache.carbondata.core.fileoperations.FileWriteOperation;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import com.google.gson.Gson;

/**
 * Provide read and write support for partition mapping file in each segment
 */
public class PartitionMapFileStore {

  private Map<String, List<String>> partitionMap = new HashMap<>();

  private boolean partionedSegment = false;
  /**
   * Write partitionmapp file to the segment folder with indexfilename and corresponding partitions.
   *
   * @param segmentPath
   * @param taskNo
   * @param partionNames
   * @throws IOException
   */
  public void writePartitionMapFile(String segmentPath, final String taskNo,
      List<String> partionNames) throws IOException {
    CarbonFile carbonFile = FileFactory.getCarbonFile(segmentPath);
    // write partition info to new file.
    if (carbonFile.exists() && partionNames.size() > 0) {
      CarbonFile[] carbonFiles = carbonFile.listFiles(new CarbonFileFilter() {
        @Override public boolean accept(CarbonFile file) {
          return file.getName().startsWith(taskNo) && file.getName()
              .endsWith(CarbonTablePath.INDEX_FILE_EXT);
        }
      });
      if (carbonFiles != null && carbonFiles.length > 0) {
        PartitionMapper partitionMapper = new PartitionMapper();
        Map<String, List<String>> partitionMap = new HashMap<>();
        partitionMap.put(carbonFiles[0].getName(), partionNames);
        partitionMapper.setPartitionMap(partitionMap);
        String path = segmentPath + "/" + taskNo + CarbonTablePath.PARTITION_MAP_EXT;
        writePartitionFile(partitionMapper, path);
      }
    }
  }

  private void writePartitionFile(PartitionMapper partitionMapper, String path) throws IOException {
    AtomicFileOperations fileWrite =
        new AtomicFileOperationsImpl(path, FileFactory.getFileType(path));
    BufferedWriter brWriter = null;
    DataOutputStream dataOutputStream = null;
    Gson gsonObjectToWrite = new Gson();
    try {
      dataOutputStream = fileWrite.openForWrite(FileWriteOperation.OVERWRITE);
      brWriter = new BufferedWriter(new OutputStreamWriter(dataOutputStream,
          Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET)));

      String metadataInstance = gsonObjectToWrite.toJson(partitionMapper);
      brWriter.write(metadataInstance);
    } finally {
      if (null != brWriter) {
        brWriter.flush();
      }
      CarbonUtil.closeStreams(brWriter);
      fileWrite.close();
    }
  }

  /**
   * Merge all partition files in a segment to single file.
   *
   * @param segmentPath
   * @throws IOException
   */
  public void mergePartitionMapFiles(String segmentPath, String mergeFileName) throws IOException {
    CarbonFile[] partitionFiles = getPartitionFiles(segmentPath);
    if (partitionFiles != null && partitionFiles.length > 0) {
      PartitionMapper partitionMapper = null;
      for (CarbonFile file : partitionFiles) {
        PartitionMapper localMapper = readPartitionMap(file.getAbsolutePath());
        if (partitionMapper == null && localMapper != null) {
          partitionMapper = localMapper;
        }
        if (localMapper != null) {
          partitionMapper = partitionMapper.merge(localMapper);
        }
      }
      if (partitionMapper != null) {
        String path = segmentPath + "/" + mergeFileName + CarbonTablePath.PARTITION_MAP_EXT;
        writePartitionFile(partitionMapper, path);
        for (CarbonFile file : partitionFiles) {
          if (!FileFactory.deleteAllCarbonFilesOfDir(file)) {
            throw new IOException("Old partition map files cannot be deleted");
          }
        }
      }
    }
  }

  private CarbonFile[] getPartitionFiles(String segmentPath) {
    CarbonFile carbonFile = FileFactory.getCarbonFile(segmentPath);
    if (carbonFile.exists()) {
      return carbonFile.listFiles(new CarbonFileFilter() {
        @Override public boolean accept(CarbonFile file) {
          return file.getName().endsWith(CarbonTablePath.PARTITION_MAP_EXT);
        }
      });
    }
    return null;
  }

  /**
   * This method reads the partition file
   *
   * @param partitionMapPath
   * @return
   */
  private PartitionMapper readPartitionMap(String partitionMapPath) {
    Gson gsonObjectToRead = new Gson();
    DataInputStream dataInputStream = null;
    BufferedReader buffReader = null;
    InputStreamReader inStream = null;
    PartitionMapper partitionMapper;
    AtomicFileOperations fileOperation =
        new AtomicFileOperationsImpl(partitionMapPath, FileFactory.getFileType(partitionMapPath));

    try {
      if (!FileFactory.isFileExist(partitionMapPath, FileFactory.getFileType(partitionMapPath))) {
        return null;
      }
      dataInputStream = fileOperation.openForRead();
      inStream = new InputStreamReader(dataInputStream,
          Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
      buffReader = new BufferedReader(inStream);
      partitionMapper = gsonObjectToRead.fromJson(buffReader, PartitionMapper.class);
    } catch (IOException e) {
      return null;
    } finally {
      CarbonUtil.closeStreams(buffReader, inStream, dataInputStream);
    }

    return partitionMapper;
  }

  public void readAllPartitionsOfSegment(String segmentPath) {
    CarbonFile[] partitionFiles = getPartitionFiles(segmentPath);
    if (partitionFiles != null && partitionFiles.length > 0) {
      partionedSegment = true;
      int i = 0;
      // Get the latest partition map file based on the timestamp of that file.
      long [] partitionTimestamps = new long[partitionFiles.length];
      for (CarbonFile file : partitionFiles) {
        partitionTimestamps[i++] =
            Long.parseLong(file.getName().substring(
                0, file.getName().length() - CarbonTablePath.PARTITION_MAP_EXT.length()));
      }
      Arrays.sort(partitionTimestamps);
      PartitionMapper partitionMapper = readPartitionMap(
          segmentPath + "/" + partitionTimestamps[partitionTimestamps.length - 1]
              + CarbonTablePath.PARTITION_MAP_EXT);
      partitionMap.putAll(partitionMapper.getPartitionMap());
    }
  }

  public boolean isPartionedSegment() {
    return partionedSegment;
  }

  /**
   * Drops the partitions from the partition mapper file of the segment and writes to a new file.
   * @param segmentPath
   * @param partitionsToDrop
   * @param uniqueId
   * @throws IOException
   */
  public void dropPartitions(String segmentPath, List<String> partitionsToDrop, String uniqueId)
      throws IOException {
    readAllPartitionsOfSegment(segmentPath);
    List<String> indexesToDrop = new ArrayList<>();
    for (Map.Entry<String, List<String>> entry: partitionMap.entrySet()) {
      for (String partition: partitionsToDrop) {
        if (entry.getValue().contains(partition)) {
          indexesToDrop.add(entry.getKey());
        }
      }
    }
    if (indexesToDrop.size() > 0) {
      // Remove the indexes from partition map
      for (String indexToDrop : indexesToDrop) {
        partitionMap.remove(indexToDrop);
      }
      PartitionMapper mapper = new PartitionMapper();
      mapper.setPartitionMap(partitionMap);
      String path = segmentPath + "/" + uniqueId + CarbonTablePath.PARTITION_MAP_EXT;
      writePartitionFile(mapper, path);
    }
  }

  /**
   * It deletes the old partition mapper files in case of success. And in case of failure it removes
   * the old new file.
   * @param segmentPath
   * @param uniqueId
   * @param success
   */
  public void commitPartitions(String segmentPath, final String uniqueId, boolean success) {
    CarbonFile carbonFile = FileFactory.getCarbonFile(segmentPath);
    // write partition info to new file.
    if (carbonFile.exists()) {
      CarbonFile[] carbonFiles = carbonFile.listFiles(new CarbonFileFilter() {
        @Override public boolean accept(CarbonFile file) {
          return file.getName().endsWith(CarbonTablePath.PARTITION_MAP_EXT);
        }
      });
      CarbonFile latestFile = null;
      for (CarbonFile mapFile: carbonFiles) {
        if (mapFile.getName().startsWith(uniqueId)) {
          latestFile = mapFile;
        }
      }
      if (latestFile != null) {
        for (CarbonFile mapFile : carbonFiles) {
          if (latestFile != mapFile) {
            // Remove old files in case of success scenario
            if (success) {
              mapFile.delete();
            }
          }
        }
      }
      // If it is failure scenario then remove the new file.
      if (!success && latestFile != null) {
        latestFile.delete();
      }
    }
  }

  public List<String> getPartitions(String indexFileName) {
    return partitionMap.get(indexFileName);
  }

  public Map<String, List<String>> getPartitionMap() {
    return partitionMap;
  }

  public static class PartitionMapper implements Serializable {

    private static final long serialVersionUID = 3582245668420401089L;

    private Map<String, List<String>> partitionMap;

    public PartitionMapper merge(PartitionMapper mapper) {
      if (this == mapper) {
        return this;
      }
      if (partitionMap != null && mapper.partitionMap != null) {
        partitionMap.putAll(mapper.partitionMap);
      }
      if (partitionMap == null) {
        partitionMap = mapper.partitionMap;
      }
      return this;
    }

    public Map<String, List<String>> getPartitionMap() {
      return partitionMap;
    }

    public void setPartitionMap(Map<String, List<String>> partitionMap) {
      this.partitionMap = partitionMap;
    }
  }

}
