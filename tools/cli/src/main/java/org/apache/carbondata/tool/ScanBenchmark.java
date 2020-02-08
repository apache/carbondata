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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.carbondata.common.Strings;
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
  private DataFile file;
  private List<String> outPuts;

  ScanBenchmark(String dataFolder, List<String> outPuts) {
    this.dataFolder = dataFolder;
    this.outPuts = outPuts;
  }

  @Override
  public void run(CommandLine line) throws IOException {
    if (line.hasOption("f")) {
      String filePath = line.getOptionValue("f");
      file = new DataFile(FileFactory.getCarbonFile(filePath));
    } else {
      FileCollector collector = new FileCollector(outPuts);
      collector.collectFiles(dataFolder);
      if (collector.getNumDataFiles() == 0) {
        return;
      }
      Map<String, DataFile> dataFiles = collector.getDataFiles();
      Iterator<DataFile> iterator = dataFiles.values().iterator();
      // use the first file and close the rest
      file = iterator.next();
      while (iterator.hasNext()) {
        iterator.next().close();
      }
    }

    outPuts.add("\n## Benchmark");
    final AtomicReference<FileHeader> fileHeaderRef = new AtomicReference<>();
    final AtomicReference<FileFooter3> fileFoorterRef = new AtomicReference<>();
    final AtomicReference<DataFileFooter> convertedFooterRef = new AtomicReference<>();

    // benchmark read header and footer time
    benchmarkOperation("ReadHeaderAndFooter", new Operation() {
      @Override
      public void run() throws IOException {
        fileHeaderRef.set(file.readHeader());
        fileFoorterRef.set(file.readFooter());
      }
    });
    final FileHeader fileHeader = fileHeaderRef.get();
    final FileFooter3 fileFooter = fileFoorterRef.get();

    // benchmark convert footer
    benchmarkOperation("ConvertFooter", new Operation() {
      @Override
      public void run() {
        convertFooter(fileHeader, fileFooter);
      }
    });

    // benchmark read all meta and convert footer
    benchmarkOperation("ReadAllMetaAndConvertFooter", new Operation() {
      @Override
      public void run() throws IOException {
        DataFileFooter footer = readAndConvertFooter(file);
        convertedFooterRef.set(footer);
      }
    });

    if (line.hasOption("c")) {
      String columnName = line.getOptionValue("c");
      outPuts.add("\nScan column '" + columnName + "'");

      final DataFileFooter footer = convertedFooterRef.get();
      final AtomicReference<AbstractRawColumnChunk> columnChunk = new AtomicReference<>();
      final int columnIndex = file.getColumnIndex(columnName);
      final boolean dimension = file.getColumn(columnName).isDimensionColumn();
      for (int i = 0; i < footer.getBlockletList().size(); i++) {
        final int blockletId = i;
        outPuts.add(String.format("Blocklet#%d: total size %s, %,d pages, %,d rows",
            blockletId,
            Strings.formatSize(file.getColumnDataSizeInBytes(blockletId, columnIndex)),
            footer.getBlockletList().get(blockletId).getNumberOfPages(),
            footer.getBlockletList().get(blockletId).getNumberOfRows()));
        benchmarkOperation("\tColumnChunk IO", new Operation() {
          @Override
          public void run() throws IOException {
            columnChunk.set(readBlockletColumnChunkIO(footer, blockletId, columnIndex, dimension));
          }
        });

        if (dimensionColumnChunkReader != null) {
          benchmarkOperation("\tDecompress Pages", new Operation() {
            @Override
            public void run() throws IOException {
              decompressDimensionPages(columnChunk.get(),
                  footer.getBlockletList().get(blockletId).getNumberOfPages());
            }
          });
        } else {
          benchmarkOperation("\tDecompress Pages", new Operation() {
            @Override
            public void run() throws IOException {
              decompressMeasurePages(columnChunk.get(),
                  footer.getBlockletList().get(blockletId).getNumberOfPages());
            }
          });
        }
      }
    }
    file.close();
  }

  interface Operation {
    void run() throws IOException;
  }

  private void benchmarkOperation(String opName, Operation op) throws IOException {
    long start, end;
    start = System.nanoTime();
    op.run();
    end = System.nanoTime();
    outPuts.add(String.format("%s takes %,d us", opName, (end - start) / 1000));
  }

  private DataFileFooter readAndConvertFooter(DataFile file) throws IOException {
    String segmentId = CarbonTablePath.DataFileUtil.getSegmentNo(file.getFilePath());
    TableBlockInfo blockInfo =
        new TableBlockInfo(file.getFilePath(), file.getFooterOffset(),
            segmentId, new String[]{"localhost"}, file.getFileSizeInBytes(),
            ColumnarFormatVersion.V3, new String[0]);

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
              file.getFilePath(), false);
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
      AbstractRawColumnChunk rawColumnChunk, int numPages) throws IOException {
    DimensionColumnPage[] pages = new DimensionColumnPage[numPages];
    for (int i = 0; i < pages.length; i++) {
      pages[i] = dimensionColumnChunkReader.decodeColumnPage(
          (DimensionRawColumnChunk) rawColumnChunk, i, null);
    }
    return pages;
  }

  private ColumnPage[] decompressMeasurePages(
      AbstractRawColumnChunk rawColumnChunk, int numPages) throws IOException {
    ColumnPage[] pages = new ColumnPage[numPages];
    for (int i = 0; i < pages.length; i++) {
      pages[i] = measureColumnChunkReader.decodeColumnPage(
          (MeasureRawColumnChunk) rawColumnChunk, i, null);
    }
    return pages;
  }

}
