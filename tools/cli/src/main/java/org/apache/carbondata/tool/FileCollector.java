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

package org.apache.carbondata.tool;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import org.apache.carbondata.common.Strings;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.format.BlockletInfo3;
import org.apache.carbondata.format.FileFooter3;

/**
 * A helper to collect all data files, schema file, table status file in a given folder
 */
class FileCollector {

  private long numBlock;
  private long numShard;
  private long numBlocklet;
  private long numPage;
  private long numRow;
  private long totalDataSize;

  // file path mapping to file object
  private LinkedHashMap<String, DataFile> dataFiles = new LinkedHashMap<>();
  private CarbonFile tableStatusFile;
  private CarbonFile schemaFile;

  private PrintStream out;

  FileCollector(PrintStream out) {
    this.out = out;
  }

  void collectFiles(String dataFolder) throws IOException {
    Set<String> shards = new HashSet<>();
    CarbonFile folder = FileFactory.getCarbonFile(dataFolder);
    List<CarbonFile> files = folder.listFiles(true);
    List<DataFile> unsortedFiles = new ArrayList<>();
    for (CarbonFile file : files) {
      if (isColumnarFile(file.getName())) {
        DataFile dataFile = new DataFile(file);
        dataFile.collectAllMeta();
        unsortedFiles.add(dataFile);
        collectNum(dataFile.getFooter());
        shards.add(dataFile.getShardName());
        totalDataSize += file.getSize();
      } else if (file.getName().endsWith(CarbonTablePath.TABLE_STATUS_FILE)) {
        tableStatusFile = file;
      } else if (file.getName().startsWith(CarbonTablePath.SCHEMA_FILE)) {
        schemaFile = file;
      } else if (isStreamFile(file.getName())) {
        out.println("WARN: input path contains streaming file, this tool does not support it yet, "
            + "skipping it...");
      }
    }
    unsortedFiles.sort((o1, o2) -> {
      if (o1.getShardName().equalsIgnoreCase(o2.getShardName())) {
        return Integer.parseInt(o1.getPartNo()) - Integer.parseInt(o2.getPartNo());
      } else {
        return o1.getShardName().compareTo(o2.getShardName());
      }
    });
    for (DataFile collectedFile : unsortedFiles) {
      this.dataFiles.put(collectedFile.getFilePath(), collectedFile);
    }
    numShard = shards.size();
  }

  private void collectNum(FileFooter3 footer) {
    numBlock++;
    numBlocklet += footer.blocklet_index_list.size();
    numRow += footer.num_rows;
    for (BlockletInfo3 blockletInfo3 : footer.blocklet_info_list3) {
      numPage += blockletInfo3.number_number_of_pages;
    }
  }

  private boolean isColumnarFile(String fileName) {
    // if the timestamp in file name is "0", it is a streaming file
    return fileName.endsWith(CarbonTablePath.CARBON_DATA_EXT) &&
        !CarbonTablePath.DataFileUtil.getTimeStampFromFileName(fileName).equals("0");
  }

  private boolean isStreamFile(String fileName) {
    // if the timestamp in file name is "0", it is a streaming file
    return fileName.endsWith(CarbonTablePath.CARBON_DATA_EXT) &&
        CarbonTablePath.DataFileUtil.getTimeStampFromFileName(fileName).equals("0");
  }

  // return file path mapping to file object
  LinkedHashMap<String, DataFile> getDataFiles() {
    return dataFiles;
  }

  CarbonFile getTableStatusFile() {
    return tableStatusFile;
  }

  CarbonFile getSchemaFile() {
    return schemaFile;
  }

  int getNumDataFiles() {
    return dataFiles.size();
  }

  void printBasicStats() {
    if (dataFiles.size() == 0) {
      System.out.println("no data file found");
      return;
    }
    out.println("## Summary");
    out.println(
        String.format("total: %,d blocks, %,d shards, %,d blocklets, %,d pages, %,d rows, %s",
            numBlock, numShard, numBlocklet, numPage, numRow, Strings.formatSize(totalDataSize)));
    out.println(
        String.format("avg: %s/block, %s/blocklet, %,d rows/block, %,d rows/blocklet",
            Strings.formatSize((float) totalDataSize / numBlock),
            Strings.formatSize((float) totalDataSize / numBlocklet),
            numRow / numBlock,
            numRow / numBlocklet));
  }
}
