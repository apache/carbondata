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
import java.util.*;

import org.apache.carbondata.common.Strings;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.indexstore.blockletindex.SegmentIndexFileStore;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.format.BlockletInfo3;
import org.apache.carbondata.format.ColumnSchema;
import org.apache.carbondata.format.FileFooter3;
import org.apache.carbondata.format.IndexHeader;

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
  private List<String> outPuts;

  // file path mapping to file object
  private LinkedHashMap<String, DataFile> dataFiles = new LinkedHashMap<>();
  private CarbonFile tableStatusFile;
  private CarbonFile schemaFile;


  FileCollector(List<String> outPuts) {
    this.outPuts = outPuts;
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
        outPuts.add(("WARN: input path contains streaming file, this tool does not support it yet, "
            + "skipping it..."));

      }
    }

    Collections.sort(unsortedFiles, new Comparator<DataFile>() {
      @Override public int compare(DataFile o1, DataFile o2) {
        if (o1.getShardName().equalsIgnoreCase(o2.getShardName())) {
          return Integer.parseInt(o1.getPartNo()) - Integer.parseInt(o2.getPartNo());
        } else {
          return o1.getShardName().compareTo(o2.getShardName());
        }
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
    outPuts.add("## Summary");
    String format = String
        .format("total: %,d blocks, %,d shards, %,d blocklets, %,d pages, %,d rows, %s", numBlock,
            numShard, numBlocklet, numPage, numRow, Strings.formatSize(totalDataSize));
    outPuts.add(format);

    String format1 = String.format("avg: %s/block, %s/blocklet, %,d rows/block, %,d rows/blocklet",
        Strings.formatSize((float) totalDataSize / numBlock),
        Strings.formatSize((float) totalDataSize / numBlocklet), numRow / numBlock,
        numRow / numBlocklet);
    outPuts.add(format1);
  }

  private String makeSortColumnsString(List<ColumnSchema> columnList) {
    StringBuilder builder = new StringBuilder();
    for (ColumnSchema column : columnList) {
      if (column.isDimension()) {
        Map<String, String> properties = column.getColumnProperties();
        if (properties != null) {
          if (properties.get(CarbonCommonConstants.SORT_COLUMNS) != null) {
            builder.append(column.column_name).append(",");
          }
        }
      }
    }
    if (builder.length() > 1) {
      return builder.substring(0, builder.length() - 1);
    } else {
      return "";
    }
  }

  public void collectSortColumns(String segmentFolder) throws IOException {
    CarbonFile[] files = SegmentIndexFileStore.getCarbonIndexFiles(
        segmentFolder, FileFactory.getConfiguration());
    if (files.length == 0) {
      throw new IllegalArgumentException("\"" + segmentFolder + "\" is not a valid Segment Folder");
    }
    Set<Boolean> isSortSet = new HashSet<>();
    Set<String> sortColumnsSet = new HashSet<>();
    for (CarbonFile file : files) {
      IndexHeader indexHeader = SegmentIndexFileStore.readIndexHeader(
          file.getCanonicalPath(), FileFactory.getConfiguration());
      if (indexHeader != null) {
        if (indexHeader.isSetIs_sort()) {
          isSortSet.add(indexHeader.is_sort);
          if (indexHeader.is_sort) {
            sortColumnsSet.add(makeSortColumnsString(indexHeader.getTable_columns()));
          }
        } else {
          // if is_sort is not set, it will be old store and consider as local_sort by default.
          sortColumnsSet.add(makeSortColumnsString(indexHeader.getTable_columns()));
        }
      }
      if (isSortSet.size() >= 2 || sortColumnsSet.size() >= 2) {
        break;
      }
    }
    // for all index files, sort_columns should be same
    if (isSortSet.size() <= 1 && sortColumnsSet.size() == 1) {
      outPuts.add("sorted by " + sortColumnsSet.iterator().next());
    } else {
      outPuts.add("unsorted");
    }
  }

  public void close() throws IOException {
    for (DataFile file : dataFiles.values()) {
      file.close();
    }
  }
}
