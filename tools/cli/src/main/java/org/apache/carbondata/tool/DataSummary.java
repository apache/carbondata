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
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.carbondata.common.Strings;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.reader.CarbonHeaderReader;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.format.BlockletInfo3;
import org.apache.carbondata.format.FileFooter3;
import org.apache.carbondata.format.FileHeader;
import org.apache.carbondata.format.TableInfo;

import static org.apache.carbondata.core.constants.CarbonCommonConstants.DEFAULT_CHARSET;

/**
 * Data Summary command implementation for {@link CarbonCli}
 */
class DataSummary {
  private String dataFolder;
  private PrintStream out;

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

  DataSummary(String dataFolder, PrintStream out) throws IOException {
    this.dataFolder = dataFolder;
    this.out = out;
    collectDataFiles();
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

  private void collectDataFiles() throws IOException {
    Set<String> shards = new HashSet<>();
    CarbonFile folder = FileFactory.getCarbonFile(dataFolder);
    List<CarbonFile> files = folder.listFiles(true);
    List<DataFile> unsortedFiles = new ArrayList<>();
    for (CarbonFile file : files) {
      if (isColumnarFile(file.getName())) {
        DataFile dataFile = new DataFile(file);
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

  void printBasic() {
    out.println("## Summary");
    out.println(
        String.format("total: %,d blocks, %,d shards, %,d blocklets, %,d pages, %,d rows, %s",
            numBlock, numShard, numBlocklet, numPage, numRow, Strings.formatSize(totalDataSize)));
    out.println(
        String.format("avg: %s/block, %s/blocklet, %,d rows/block, %,d rows/blocklet",
            Strings.formatSize(totalDataSize / numBlock),
            Strings.formatSize(totalDataSize / numBlocklet),
            numRow / numBlock,
            numRow / numBlocklet));
  }

  void printSchema() throws IOException {
    if (dataFiles.size() > 0) {
      String firstFile = dataFiles.keySet().iterator().next();
      CarbonFile file = FileFactory.getCarbonFile(firstFile);
      out.println();
      out.println("## Schema");
      out.println(String.format("schema in %s", file.getName()));
      CarbonHeaderReader reader = new CarbonHeaderReader(file.getPath());
      FileHeader header = reader.readHeader();
      out.println("version: V" + header.version);
      out.println("timestamp: " + new java.sql.Timestamp(header.time_stamp));
      List<ColumnSchema> columns = reader.readSchema();
      TablePrinter printer = new TablePrinter(
          new String[]{"Column Name", "Data Type", "Column Type",
              "SortColumn", "Encoding", "Ordinal", "Id"});
      for (ColumnSchema column : columns) {
        String shortColumnId = "NA";
        if (column.getColumnUniqueId() != null && column.getColumnUniqueId().length() > 4) {
          shortColumnId = "*" +
              column.getColumnUniqueId().substring(column.getColumnUniqueId().length() - 4);
        }
        printer.addRow(new String[]{
            column.getColumnName(),
            column.getDataType().getName(),
            column.isDimensionColumn() ? "dimension" : "measure",
            String.valueOf(column.isSortColumn()),
            column.getEncodingList().toString(),
            Integer.toString(column.getSchemaOrdinal()),
            shortColumnId
        });
      }
      printer.printFormatted(out);
    }
  }

  void printSegments() throws IOException {
    out.println();
    out.println("## Segment");
    if (tableStatusFile != null) {
      // first collect all information in memory then print a formatted table
      LoadMetadataDetails[] segments =
          SegmentStatusManager.readTableStatusFile(tableStatusFile.getPath());
      TablePrinter printer = new TablePrinter(
          new String[]{"SegmentID", "Status", "Load Start", "Load End",
              "Merged To", "Format", "Data Size", "Index Size"});
      for (LoadMetadataDetails segment : segments) {
        String dataSize, indexSize;
        if (segment.getDataSize() == null) {
          dataSize = "NA";
        } else {
          dataSize = Strings.formatSize(Long.parseLong(segment.getDataSize()));
        }
        if (segment.getIndexSize() == null) {
          indexSize = "NA";
        } else {
          indexSize = Strings.formatSize(Long.parseLong(segment.getIndexSize()));
        }
        printer.addRow(new String[]{
            segment.getLoadName(),
            segment.getSegmentStatus().toString(),
            new java.sql.Date(segment.getLoadStartTime()).toString(),
            new java.sql.Date(segment.getLoadEndTime()).toString(),
            segment.getMergedLoadName() == null ? "NA" : segment.getMergedLoadName(),
            segment.getFileFormat().toString(),
            dataSize,
            indexSize}
        );
      }
      printer.printFormatted(out);
    } else {
      out.println("table status file not found");
    }
  }

  void printTableProperties() throws IOException {
    out.println();
    out.println("## Table Properties");
    if (schemaFile != null) {
      TableInfo thriftTableInfo = CarbonUtil.readSchemaFile(schemaFile.getPath());
      Map<String, String> tblProperties = thriftTableInfo.fact_table.tableProperties;
      TablePrinter printer = new TablePrinter(
          new String[]{"Property Name", "Property Value"});
      for (Map.Entry<String, String> entry : tblProperties.entrySet()) {
        printer.addRow(new String[] {
            String.format("'%s'", entry.getKey()),
            String.format("'%s'", entry.getValue())
        });
      }
      printer.printFormatted(out);
    } else {
      out.println("schema file not found");
    }
  }

  void printBlockletDetail() {
    out.println();
    out.println("## Block Detail");

    ShardPrinter printer = new ShardPrinter(new String[]{
        "BLK", "BLKLT", "NumPages", "NumRows", "Size"
    });

    for (Map.Entry<String, DataFile> entry : dataFiles.entrySet()) {
      DataFile file = entry.getValue();
      FileFooter3 footer = file.getFooter();
      for (int blockletId = 0; blockletId < footer.blocklet_info_list3.size(); blockletId++) {
        BlockletInfo3 blocklet = footer.blocklet_info_list3.get(blockletId);
        printer.addRow(file.getShardName(), new String[]{
            file.getPartNo(),
            String.valueOf(blockletId),
            String.format("%,d", blocklet.number_number_of_pages),
            String.format("%,d", blocklet.num_rows),
            Strings.formatSize(file.getBlockletSizeInBytes(blockletId))
        });
      }
    }
    printer.printFormatted(out);
  }

  private int getColumnIndex(String columnName) {
    if (dataFiles.size() > 0) {
      List<ColumnSchema> columns = dataFiles.entrySet().iterator().next().getValue().getSchema();
      for (int i = 0; i < columns.size(); i++) {
        if (columns.get(i).getColumnName().equalsIgnoreCase(columnName)) {
          return i;
        }
      }
    }
    throw new RuntimeException("schema for column " + columnName + " not found");
  }

  void printColumnStats(String columnName) throws IOException, MemoryException {
    out.println();
    out.println("## Column Statistics for '" + columnName + "'");
    for (DataFile dataFile : dataFiles.values()) {
      dataFile.initAllBlockletStats(columnName);
    }
    collectAllBlockletStats(dataFiles.values());

    int columnIndex = getColumnIndex(columnName);
    String[] header = new String[]{"BLK", "BLKLT", "Meta Size", "Data Size",
        "LocalDict", "DictEntries", "DictSize", "AvgPageSize", "Min%", "Max%"};

    ShardPrinter printer = new ShardPrinter(header);
    for (Map.Entry<String, DataFile> entry : dataFiles.entrySet()) {
      DataFile file = entry.getValue();
      for (DataFile.Blocklet blocklet : file.getAllBlocklets()) {
        String min, max;
        if (blocklet.getColumnChunk().getDataType() == DataTypes.STRING) {
          min = new String(blocklet.getColumnChunk().min, Charset.forName(DEFAULT_CHARSET));
          max = new String(blocklet.getColumnChunk().max, Charset.forName(DEFAULT_CHARSET));
        } else {
          min = String.format("%.1f", blocklet.getColumnChunk().getMinPercentage() * 100);
          max = String.format("%.1f", blocklet.getColumnChunk().getMaxPercentage() * 100);
        }
        printer.addRow(
            blocklet.getShardName(),
            new String[]{
                file.getPartNo(),
                String.valueOf(blocklet.id),
                Strings.formatSize(file.getColumnMetaSizeInBytes(blocklet.id, columnIndex)),
                Strings.formatSize(file.getColumnDataSizeInBytes(blocklet.id, columnIndex)),
                String.valueOf(blocklet.getColumnChunk().localDict),
                String.valueOf(blocklet.getColumnChunk().blockletDictionaryEntries),
                Strings.formatSize(blocklet.getColumnChunk().blocketletDictionarySize),
                Strings.formatSize(blocklet.getColumnChunk().avgPageLengthInBytes),
                min,
                max}
        );
      }
    }
    printer.printFormatted(out);
  }

  private void collectAllBlockletStats(Collection<DataFile> dataFiles) {
    // shard name mapping to blocklets belong to same shard
    Map<String, List<DataFile.Blocklet>> shards = new HashMap<>();

    // collect blocklets based on shard name
    for (DataFile dataFile : dataFiles) {
      List<DataFile.Blocklet> blocklets = dataFile.getAllBlocklets();
      List<DataFile.Blocklet> existing = shards.get(dataFile.getShardName());
      if (existing == null) {
        existing = new LinkedList<>();
      }
      existing.addAll(blocklets);
      shards.put(dataFile.getShardName(), existing);
    }

    // calculate min/max for each shard
    Map<String, byte[]> shardMinMap = new HashMap<>();
    Map<String, byte[]> shardMaxMap = new HashMap<>();
    for (Map.Entry<String, List<DataFile.Blocklet>> shard : shards.entrySet()) {
      byte[] shardMin = null;
      byte[] shardMax = null;
      for (DataFile.Blocklet blocklet : shard.getValue()) {
        shardMin = blocklet.getColumnChunk().min(shardMin);
        shardMax = blocklet.getColumnChunk().max(shardMax);
      }
      shardMinMap.put(shard.getKey(), shardMin);
      shardMaxMap.put(shard.getKey(), shardMax);
    }

    // calculate min/max percentage for each blocklet
    for (Map.Entry<String, List<DataFile.Blocklet>> shard : shards.entrySet()) {
      byte[] shardMin = shardMinMap.get(shard.getKey());
      byte[] shardMax = shardMaxMap.get(shard.getKey());
      for (DataFile.Blocklet blocklet : shard.getValue()) {
        blocklet.computePercentage(shardMin, shardMax);
      }
    }
  }

  public boolean isEmpty() {
    return dataFiles.size() == 0;
  }
}
