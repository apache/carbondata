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
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.carbondata.common.Strings;
import org.apache.carbondata.core.datastore.block.BlockletInfos;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.datastore.chunk.AbstractRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.DimensionColumnPage;
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.impl.MeasureRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.reader.CarbonDataReaderFactory;
import org.apache.carbondata.core.datastore.chunk.reader.DimensionColumnChunkReader;
import org.apache.carbondata.core.datastore.chunk.reader.MeasureColumnChunkReader;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.metadata.blocklet.BlockletInfo;
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.util.DataFileFooterConverterV3;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.format.FileFooter3;
import org.apache.carbondata.format.FileHeader;

import org.apache.commons.cli.CommandLine;

class ScanBenchmark implements Command {

  private String dataFolder;
  private PrintStream out;
  private DataFile file;

  ScanBenchmark(String dataFolder, PrintStream out) {
    this.dataFolder = dataFolder;
    this.out = out;
  }

  @Override
  public void run(CommandLine line) throws IOException, MemoryException {
    if (line.hasOption("f")) {
      String filePath = line.getOptionValue("f");
      file = new DataFile(FileFactory.getCarbonFile(filePath));
    } else {
      FileCollector collector = new FileCollector(out);
      collector.collectFiles(dataFolder);
      if (collector.getNumDataFiles() == 0) {
        return;
      }
      Map<String, DataFile> dataFiles = collector.getDataFiles();
      file = dataFiles.entrySet().iterator().next().getValue();
    }

    out.println("\n## Benchmark");
    AtomicReference<FileHeader> fileHeaderRef = new AtomicReference<>();
    AtomicReference<FileFooter3> fileFoorterRef = new AtomicReference<>();
    AtomicReference<DataFileFooter> convertedFooterRef = new AtomicReference<>();

    // benchmark read header and footer time
    benchmarkOperation("ReadHeaderAndFooter", () -> {
      fileHeaderRef.set(file.readHeader());
      fileFoorterRef.set(file.readFooter());
    });
    FileHeader fileHeader = fileHeaderRef.get();
    FileFooter3 fileFooter = fileFoorterRef.get();

    // benchmark convert footer
    benchmarkOperation("ConvertFooter", () -> {
      convertFooter(fileHeader, fileFooter);
    });

    // benchmark read all meta and convert footer
    benchmarkOperation("ReadAllMetaAndConvertFooter", () -> {
      DataFileFooter footer = readAndConvertFooter(file);
      convertedFooterRef.set(footer);
    });

    if (line.hasOption("c")) {
      String columnName = line.getOptionValue("c");
      out.println("\nScan column '" + columnName + "'");

      DataFileFooter footer = convertedFooterRef.get();
      AtomicReference<AbstractRawColumnChunk> columnChunk = new AtomicReference<>();
      int columnIndex = file.getColumnIndex(columnName);
      boolean dimension = file.getColumn(columnName).isDimensionColumn();
      for (int i = 0; i < footer.getBlockletList().size(); i++) {
        int blockletId = i;
        out.println(String.format("Blocklet#%d: total size %s, %,d pages, %,d rows",
            blockletId,
            Strings.formatSize(file.getColumnDataSizeInBytes(blockletId, columnIndex)),
            footer.getBlockletList().get(blockletId).getNumberOfPages(),
            footer.getBlockletList().get(blockletId).getNumberOfRows()));
        benchmarkOperation("\tColumnChunk IO", () -> {
          columnChunk.set(readBlockletColumnChunkIO(footer, blockletId, columnIndex, dimension));
        });

        if (dimensionColumnChunkReader != null) {
          benchmarkOperation("\tDecompress Pages", () -> {
            decompressDimensionPages(columnChunk.get(),
                footer.getBlockletList().get(blockletId).getNumberOfPages());
          });
        } else {
          benchmarkOperation("\tDecompress Pages", () -> {
            decompressMeasurePages(columnChunk.get(),
                footer.getBlockletList().get(blockletId).getNumberOfPages());
          });
        }
      }
    }

  }

  interface Operation {
    void run() throws IOException, MemoryException;
  }

  private void benchmarkOperation(String opName, Operation op) throws IOException, MemoryException {
    long start, end;
    start = System.nanoTime();
    op.run();
    end = System.nanoTime();
    out.println(String.format("%s takes %,d us", opName, (end - start) / 1000));
  }

  private DataFileFooter readAndConvertFooter(DataFile file) throws IOException {
    int numBlocklets = file.getNumBlocklet();
    BlockletInfos blockletInfos = new BlockletInfos(numBlocklets, 0, numBlocklets);
    String segmentId = CarbonTablePath.DataFileUtil.getSegmentNo(file.getFilePath());
    TableBlockInfo blockInfo =
        new TableBlockInfo(file.getFilePath(), file.getFooterOffset(),
            segmentId, new String[]{"localhost"}, file.getFileSizeInBytes(),
            blockletInfos, ColumnarFormatVersion.V3, new String[0]);

    DataFileFooterConverterV3 converter = new DataFileFooterConverterV3();
    return converter.readDataFileFooter(blockInfo);
  }

  private DataFileFooter convertFooter(FileHeader fileHeader, FileFooter3 fileFooter) {
    DataFileFooterConverterV3 converter = new DataFileFooterConverterV3();
    return converter.convertDataFileFooter(fileHeader, fileFooter);
  }

  private DimensionColumnChunkReader dimensionColumnChunkReader;
  private MeasureColumnChunkReader measureColumnChunkReader;

  private AbstractRawColumnChunk readBlockletColumnChunkIO(
      DataFileFooter footer, int blockletId, int columnIndex, boolean dimension)
      throws IOException {
    BlockletInfo blockletInfo = footer.getBlockletList().get(blockletId);
    if (dimension) {
      dimensionColumnChunkReader = CarbonDataReaderFactory.getInstance()
          .getDimensionColumnChunkReader(ColumnarFormatVersion.V3, blockletInfo,
              footer.getSegmentInfo().getColumnCardinality(), file.getFilePath(), false);
      return dimensionColumnChunkReader.readRawDimensionChunk(file.getFileReader(), columnIndex);
    } else {
      columnIndex = columnIndex - file.numDimensions();
      assert (columnIndex >= 0);
      measureColumnChunkReader = CarbonDataReaderFactory.getInstance()
          .getMeasureColumnChunkReader(ColumnarFormatVersion.V3, blockletInfo,
              file.getFilePath(), false);
      return measureColumnChunkReader.readRawMeasureChunk(file.getFileReader(), columnIndex);
    }
  }

  private DimensionColumnPage[] decompressDimensionPages(
      AbstractRawColumnChunk rawColumnChunk, int numPages) throws IOException, MemoryException {
    DimensionColumnPage[] pages = new DimensionColumnPage[numPages];
    for (int i = 0; i < pages.length; i++) {
      pages[i] = dimensionColumnChunkReader.decodeColumnPage(
          (DimensionRawColumnChunk) rawColumnChunk, i);
    }
    return pages;
  }

  private ColumnPage[] decompressMeasurePages(
      AbstractRawColumnChunk rawColumnChunk, int numPages) throws IOException, MemoryException {
    ColumnPage[] pages = new ColumnPage[numPages];
    for (int i = 0; i < pages.length; i++) {
      pages[i] = measureColumnChunkReader.decodeColumnPage(
          (MeasureRawColumnChunk) rawColumnChunk, i);
    }
    return pages;
  }

}
