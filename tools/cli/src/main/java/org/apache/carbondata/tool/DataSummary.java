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
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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
import org.apache.carbondata.format.BlockletInfo3;
import org.apache.carbondata.format.FileFooter3;
import org.apache.carbondata.format.FileHeader;
import org.apache.carbondata.format.TableInfo;

import static org.apache.carbondata.core.constants.CarbonCommonConstants.DEFAULT_CHARSET;

import org.apache.commons.cli.CommandLine;

/**
 * Data Summary command implementation for {@link CarbonCli}
 */
class DataSummary implements Command {
  private String dataFolder;
  private PrintStream out;

  // file path mapping to file object
  private LinkedHashMap<String, DataFile> dataFiles;

  DataSummary(String dataFolder, PrintStream out) {
    this.dataFolder = dataFolder;
    this.out = out;
  }

  @Override
  public void run(CommandLine line) throws IOException, MemoryException {
    FileCollector collector = new FileCollector(out);
    collector.collectFiles(dataFolder);
    collector.printBasicStats();
    if (collector.getNumDataFiles() == 0) {
      return;
    }
    dataFiles = collector.getDataFiles();
    boolean printAll = false;
    if (line.hasOption("a")) {
      printAll = true;
    }
    if (line.hasOption("s") || printAll) {
      if (dataFiles.size() > 0) {
        printSchema(dataFiles.entrySet().iterator().next().getValue());
      }
    }
    if (line.hasOption("m") || printAll) {
      printSegments(collector.getTableStatusFile());
    }
    if (line.hasOption("t") || printAll) {
      printTableProperties(collector.getSchemaFile());
    }
    if (line.hasOption("b") || printAll) {
      printBlockletDetail();
    }
    if (line.hasOption("c")) {
      String columName = line.getOptionValue("c");
      printColumnStats(columName);
    }
  }

  private void printSchema(DataFile dataFile) throws IOException {
    CarbonFile file = FileFactory.getCarbonFile(dataFile.getFilePath());
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

  private void printSegments(CarbonFile tableStatusFile) throws IOException {
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

  private void printTableProperties(CarbonFile schemaFile) throws IOException {
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

  private void printBlockletDetail() {
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
      return dataFiles.entrySet().iterator().next().getValue().getColumnIndex(columnName);
    }
    throw new RuntimeException("schema for column " + columnName + " not found");
  }

  private void printColumnStats(String columnName) throws IOException, MemoryException {
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

}
