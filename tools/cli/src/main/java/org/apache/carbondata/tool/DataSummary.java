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

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.Strings;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.reader.CarbonHeaderReader;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.format.BlockletInfo3;
import org.apache.carbondata.format.DataChunk2;
import org.apache.carbondata.format.DataChunk3;
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
  private List<String> outPuts;

  // file path mapping to file object
  private LinkedHashMap<String, DataFile> dataFiles;

  DataSummary(String dataFolder, List<String> outPuts) {
    this.dataFolder = dataFolder;
    this.outPuts = outPuts;
  }

  @Override
  public void run(CommandLine line) throws IOException {
    FileCollector collector = new FileCollector(outPuts);
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
        List<String> dataFilesSet = new ArrayList<>(dataFiles.keySet());
        Collections.reverse(dataFilesSet);
        collectSchemaDetails(dataFiles.get(dataFilesSet.get(0)));
      }
    }
    if (line.hasOption("m") || printAll) {
      collectSegmentsDetails(collector.getTableStatusFile());
    }
    if (line.hasOption("t") || printAll) {
      collectTableProperties(collector.getSchemaFile());
    }
    if (line.hasOption("b") || printAll) {
      String limitSize = line.getOptionValue("b");
      if (limitSize == null) {
        // by default we can limit the output to two shards and user can increase this limit
        limitSize = "2";
      }
      collectBlockletDetail(Integer.parseInt(limitSize));
    }
    if (line.hasOption("v") || printAll) {
      collectVersionDetails();
    }
    if (line.hasOption("B")) {
      String blockFileName = line.getOptionValue("B");
      collectBlockDetails(blockFileName);
    }
    if (line.hasOption("c")) {
      String columName = line.getOptionValue("c");
      printColumnStats(columName);
      if (line.hasOption("k")) {
        collectColumnChunkMeta(columName);
      }
    }
    if (line.hasOption("C")) {
      printAllColumnStats();
    }
    collector.close();
    for (DataFile file : dataFiles.values()) {
      file.close();
    }
  }

  private void collectSchemaDetails(DataFile dataFile) throws IOException {
    CarbonFile file = FileFactory.getCarbonFile(dataFile.getFilePath());
    outPuts.add("");
    outPuts.add("## Schema");
    outPuts.add(String.format("schema in %s", file.getName()));
    CarbonHeaderReader reader = new CarbonHeaderReader(file.getPath());
    FileHeader header = reader.readHeader();
    outPuts.add("version: V" + header.version);
    outPuts.add("timestamp: " + new java.sql.Timestamp(header.time_stamp));
    List<ColumnSchema> columns = reader.readSchema();
    TableFormatter tableFormatter = new TableFormatter(
        new String[]{"Column Name", "Data Type", "Column Type",
            "SortColumn", "Encoding", "Ordinal", "Id"}, outPuts);
    for (ColumnSchema column : columns) {
      String shortColumnId = "NA";
      if (column.getColumnUniqueId() != null && column.getColumnUniqueId().length() > 4) {
        shortColumnId = "*" +
            column.getColumnUniqueId().substring(column.getColumnUniqueId().length() - 4);
      }
      tableFormatter.addRow(new String[]{
          column.getColumnName(),
          column.getDataType().getName(),
          column.isDimensionColumn() ? "dimension" : "measure",
          String.valueOf(column.isSortColumn()),
          column.getEncodingList().toString(),
          Integer.toString(column.getSchemaOrdinal()),
          shortColumnId
      });
    }
    tableFormatter.printFormatted();
  }

  private void collectSegmentsDetails(CarbonFile tableStatusFile) throws IOException {
    outPuts.add("");
    outPuts.add("## Segment");
    if (tableStatusFile != null) {
      // first collect all information in memory then print a formatted table
      LoadMetadataDetails[] segments =
          SegmentStatusManager.readTableStatusFile(tableStatusFile.getPath());
      TableFormatter tableFormatter = new TableFormatter(
          new String[]{"SegmentID", "Status", "Load Start", "Load End",
              "Merged To", "Format", "Data Size", "Index Size"}, outPuts);
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
        tableFormatter.addRow(new String[]{
            segment.getLoadName(),
            segment.getSegmentStatus().toString(),
            new java.sql.Timestamp(segment.getLoadStartTime()).toString(),
            new java.sql.Timestamp(segment.getLoadEndTime()).toString(),
            segment.getMergedLoadName() == null ? "NA" : segment.getMergedLoadName(),
            segment.getFileFormat().toString(),
            dataSize,
            indexSize}
        );
      }
      tableFormatter.printFormatted();
    } else {
      outPuts.add("table status file not found");
    }
  }

  private void collectTableProperties(CarbonFile schemaFile) throws IOException {
    outPuts.add("");
    outPuts.add("## Table Properties");
    if (schemaFile != null) {
      TableInfo thriftTableInfo = CarbonUtil.readSchemaFile(schemaFile.getPath());
      Map<String, String> tblProperties = thriftTableInfo.fact_table.tableProperties;
      TableFormatter tableFormatter = new TableFormatter(
          new String[]{"Property Name", "Property Value"}, outPuts);
      for (Map.Entry<String, String> entry : tblProperties.entrySet()) {
        tableFormatter.addRow(new String[] {
            String.format("'%s'", entry.getKey()),
            String.format("'%s'", entry.getValue())
        });
      }
      tableFormatter.printFormatted();
    } else {
      outPuts.add("schema file not found");
    }
  }

  private void collectBlockletDetail(int limitSize) {
    outPuts.add("");
    outPuts.add("## Block Detail");

    ShardPrinter printer =
        new ShardPrinter(new String[] { "BLK", "BLKLT", "NumPages", "NumRows", "Size" }, outPuts);

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
      limitSize--;
      if (limitSize == 0) {
        break;
      }
    }
    printer.collectFormattedData();
  }

  private void collectBlockDetails(String blockFilePath) throws IOException {
    outPuts.add("");
    outPuts.add("## Filtered Block Details for: " + blockFilePath
        .substring(blockFilePath.lastIndexOf(File.separator) + 1, blockFilePath.length()));
    TableFormatter tableFormatter =
        new TableFormatter(new String[] { "BLKLT", "NumPages", "NumRows", "Size" }, outPuts);
    CarbonFile datafile = FileFactory.getCarbonFile(blockFilePath);
    DataFile dataFile = new DataFile(datafile);
    dataFile.collectAllMeta();
    FileFooter3 footer = dataFile.getFooter();
    for (int blockletId = 0; blockletId < footer.blocklet_info_list3.size(); blockletId++) {
      BlockletInfo3 blocklet = footer.blocklet_info_list3.get(blockletId);
      tableFormatter.addRow(new String[]{
          String.valueOf(blockletId),
          String.format("%,d", blocklet.number_number_of_pages),
          String.format("%,d", blocklet.num_rows),
          Strings.formatSize(dataFile.getBlockletSizeInBytes(blockletId))
      });
    }
    tableFormatter.printFormatted();
  }

  private void collectVersionDetails() {
    DataFile file = dataFiles.entrySet().iterator().next().getValue();
    FileFooter3 footer = file.getFooter();
    if (null != footer.getExtra_info()) {
      outPuts.add("");
      outPuts.add("## version Details");
      TableFormatter tableFormatter =
          new TableFormatter(new String[] { "written_by", "Version" }, outPuts);
      tableFormatter.addRow(new String[] { String.format("%s",
          footer.getExtra_info().get(CarbonCommonConstants.CARBON_WRITTEN_BY_FOOTER_INFO)),
          String.format("%s",
              footer.getExtra_info().get(CarbonCommonConstants.CARBON_WRITTEN_VERSION)) });
      tableFormatter.printFormatted();
    }
  }

  private int getColumnIndex(String columnName) {
    if (dataFiles.size() > 0) {
      return dataFiles.entrySet().iterator().next().getValue().getColumnIndex(columnName);
    }
    throw new RuntimeException("schema for column " + columnName + " not found");
  }

  // true if blockled stats are collected
  private boolean collected = false;

  private void printColumnStats(String columnName) throws IOException {
    outPuts.add("");
    outPuts.add("## Column Statistics for '" + columnName + "'");
    collectStats(columnName);

    int columnIndex = getColumnIndex(columnName);
    String[] header = new String[]{"BLK", "BLKLT", "Meta Size", "Data Size",
        "LocalDict", "DictEntries", "DictSize", "AvgPageSize", "Min%", "Max%", "Min", "Max"};

    ShardPrinter printer = new ShardPrinter(header, outPuts);
    for (Map.Entry<String, DataFile> entry : dataFiles.entrySet()) {
      DataFile file = entry.getValue();
      for (DataFile.Blocklet blocklet : file.getAllBlocklets()) {
        String min, max, minPercent, maxPercent;
        byte[] blockletMin = blocklet.getColumnChunk().min;
        byte[] blockletMax = blocklet.getColumnChunk().max;
        if (blocklet.getColumnChunk().getDataType() == DataTypes.STRING) {
          minPercent = "NA";
          maxPercent = "NA";
          // for complex types min max can be given as NA and for varchar where min max is not
          // written, can give NA
          if (blocklet.getColumnChunk().column.getDataType() == DataTypes.DATE ||
              blocklet.getColumnChunk().column.isComplexColumn() ||
              !blocklet.getColumnChunk().isMinMaxPresent) {
            min = "NA";
            max = "NA";
          } else {
            min = new String(blockletMin, Charset.forName(DEFAULT_CHARSET));
            max = new String(blockletMax, Charset.forName(DEFAULT_CHARSET));
          }
        } else {
          // for column has global dictionary and for complex columns,min and max percentage can be
          // NA
          if (blocklet.getColumnChunk().column.getDataType() == DataTypes.DATE ||
              blocklet.getColumnChunk().column.isComplexColumn() ||
              blocklet.getColumnChunk().column.getDataType().isComplexType()) {
            minPercent = "NA";
            maxPercent = "NA";
          } else {
            minPercent =
                String.format("%.1f", Math.abs(blocklet.getColumnChunk().getMinPercentage() * 100));
            maxPercent =
                String.format("%.1f", Math.abs(blocklet.getColumnChunk().getMaxPercentage() * 100));
          }
          DataFile.ColumnChunk columnChunk = blocklet.columnChunk;
          // need to consider dictionary and complex columns
          if (columnChunk.column.getDataType() == DataTypes.DATE ||
              blocklet.getColumnChunk().column.isComplexColumn() ||
              blocklet.getColumnChunk().column.getDataType().isComplexType()) {
            min = "NA";
            max = "NA";
          } else if (columnChunk.column.isDimensionColumn() && DataTypeUtil
              .isPrimitiveColumn(columnChunk.column.getDataType())) {
            min = DataTypeUtil.getDataBasedOnDataTypeForNoDictionaryColumn(blockletMin,
                columnChunk.column.getDataType()).toString();
            max = DataTypeUtil.getDataBasedOnDataTypeForNoDictionaryColumn(blockletMax,
                columnChunk.column.getDataType()).toString();
            if (columnChunk.column.getDataType().equals(DataTypes.TIMESTAMP)) {
              min = new java.sql.Timestamp(Long.parseLong(min) / 1000).toString();
              max = new java.sql.Timestamp(Long.parseLong(max) / 1000).toString();
            }
          } else {
            min = String.valueOf(DataTypeUtil
                .getMeasureObjectFromDataType(blockletMin, columnChunk.column.getDataType()));
            max = String.valueOf(DataTypeUtil
                .getMeasureObjectFromDataType(blockletMax, columnChunk.column.getDataType()));
          }
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
                minPercent,
                maxPercent,
                min,
                max}
        );
      }
    }
    printer.collectFormattedData();
  }

  private void printAllColumnStats() {
    if (!dataFiles.isEmpty()) {
      outPuts.add("");
      outPuts.add("## Statistics for All Columns");
      String[] header =
          new String[] { "Block", "Blocklet", "Column Name", "Meta Size", "Data Size" };
      ShardPrinter printer = new ShardPrinter(header, outPuts);
      for (Map.Entry<String, DataFile> entry : dataFiles.entrySet()) {
        DataFile dataFile = entry.getValue();
        List<ColumnSchema> columns = dataFile.getSchema();
        int columnNum = columns.size();
        int blockletNum = dataFile.getNumBlocklet();
        for (int j = 0; j < blockletNum; j++) {
          for (int i = 0; i < columnNum; i++) {
            printer.addRow(dataFile.getShardName(),
                new String[] { dataFile.getPartNo(), String.valueOf(j),
                    columns.get(i).getColumnName(),
                    Strings.formatSize(dataFile.getColumnMetaSizeInBytes(j, i)),
                    Strings.formatSize(dataFile.getColumnDataSizeInBytes(j, i)) });
          }
        }
      }
      printer.collectFormattedData();
    }
  }

  private void collectStats(String columnName) throws IOException {
    if (!collected) {
      for (DataFile dataFile : dataFiles.values()) {
        dataFile.initAllBlockletStats(columnName);
      }
      collectAllBlockletStats(dataFiles.values());
      collected = true;
    }
  }

  private void collectColumnChunkMeta(String columnName) throws IOException {
    for (Map.Entry<String, DataFile> entry : dataFiles.entrySet()) {
      DataFile file = entry.getValue();
      outPuts.add("");
      outPuts.add("## Page Meta for column '" + columnName + "' in file " + file.getFilePath());
      collectStats(columnName);
      for (int i = 0; i < file.getAllBlocklets().size(); i++) {
        DataFile.Blocklet blocklet = file.getAllBlocklets().get(i);
        DataChunk3 dataChunk3 = blocklet.getColumnChunk().getDataChunk3();
        List<DataChunk2> dataChunk2List = dataChunk3.getData_chunk_list();
        outPuts.add(String.format("Blocklet %d:", i));

        // There will be many pages, for debugging purpose,
        // just print 3 page for each blocklet is enough
        for (int j = 0; j < dataChunk2List.size() && j < 3; j++) {
          outPuts.add(String
              .format("Page %d (offset %d, length %d): %s", j, dataChunk3.page_offset.get(j),
                  dataChunk3.page_length.get(j), dataChunk2List.get(j).toString()));
        }
        outPuts.add("");
      }
    }
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
