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

package org.apache.carbondata.core.util;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;

import org.apache.carbondata.core.cache.dictionary.DictionaryByteArrayWrapper;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.FileReader;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.reader.CarbonDataReaderFactory;
import org.apache.carbondata.core.datastore.chunk.reader.dimension.v3.DimensionChunkReaderV3;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.datastore.page.LazyColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageDecoder;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoderMeta;
import org.apache.carbondata.core.datastore.page.encoding.DefaultEncodingFactory;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.metadata.SegmentFileStore;
import org.apache.carbondata.core.metadata.ValueEncoderMeta;
import org.apache.carbondata.core.metadata.blocklet.BlockletInfo;
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.format.LocalDictionaryChunk;

public class CarbonTestUtil {

  public static ValueEncoderMeta createValueEncoderMeta() {
    ColumnarFormatVersion version =
        CarbonProperties.getInstance().getFormatVersion();

    switch (version) {
      case V3:
        return new ColumnPageEncoderMeta();
      default:
        throw new UnsupportedOperationException("unsupported version: " + version);
    }
  }

  /**
   * this method returns true if local dictionary is created for all the blocklets or not
   * @param storePath
   * @param blockindex
   * @return dimensionRawColumnChunks
   */
  public static ArrayList<DimensionRawColumnChunk> getDimRawChunk(String storePath,
      Integer blockindex) throws IOException {
    CarbonFile[] dataFiles = FileFactory.getCarbonFile(storePath).listFiles(new CarbonFileFilter() {
      @Override
      public boolean accept(CarbonFile file) {
        if (file.getName().endsWith(CarbonCommonConstants.FACT_FILE_EXT)) {
          return true;
        } else {
          return false;
        }
      }
    });
    ArrayList<DimensionRawColumnChunk> dimensionRawColumnChunks =
        read(dataFiles[0].getAbsolutePath(), blockindex);
    return dimensionRawColumnChunks;
  }

  public static ArrayList<DimensionRawColumnChunk> read(String filePath, Integer blockIndex)
      throws IOException {
    File carbonDataFiles = new File(filePath);
    ArrayList<DimensionRawColumnChunk> dimensionRawColumnChunks = new ArrayList<>();
    long offset = carbonDataFiles.length();
    DataFileFooterConverterV3 converter = new DataFileFooterConverterV3();
    FileReader fileReader = FileFactory.getFileHolder(FileFactory.getFileType(filePath));
    long actualOffset = fileReader.readLong(carbonDataFiles.getAbsolutePath(), offset - 8);
    TableBlockInfo blockInfo =
        new TableBlockInfo(carbonDataFiles.getAbsolutePath(), actualOffset, "0", new String[] {},
            carbonDataFiles.length(), ColumnarFormatVersion.V3, null);
    DataFileFooter dataFileFooter = converter.readDataFileFooter(blockInfo);
    List<BlockletInfo> blockletList = dataFileFooter.getBlockletList();
    ListIterator<BlockletInfo> iterator = blockletList.listIterator();
    while (iterator.hasNext()) {
      DimensionChunkReaderV3 dimensionColumnChunkReader =
          (DimensionChunkReaderV3) CarbonDataReaderFactory.getInstance()
              .getDimensionColumnChunkReader(ColumnarFormatVersion.V3, iterator.next(),
                  carbonDataFiles.getAbsolutePath(), false);
      dimensionRawColumnChunks
          .add(dimensionColumnChunkReader.readRawDimensionChunk(fileReader, blockIndex));
    }
    return dimensionRawColumnChunks;
  }

  public static Boolean validateDictionary(DimensionRawColumnChunk rawColumnPage, String[] data)
      throws IOException {
    LocalDictionaryChunk local_dictionary = rawColumnPage.getDataChunkV3().local_dictionary;
    if (null != local_dictionary) {
      String compressorName = CarbonMetadataUtil.getCompressorNameFromChunkMeta(
          rawColumnPage.getDataChunkV3().getData_chunk_list().get(0).getChunk_meta());
      List<org.apache.carbondata.format.Encoding> encodings =
          local_dictionary.getDictionary_meta().encoders;
      DefaultEncodingFactory encodingFactory =
          (DefaultEncodingFactory) DefaultEncodingFactory.getInstance();
      ColumnPageDecoder decoder = encodingFactory
          .createDecoder(encodings, local_dictionary.getDictionary_meta().getEncoder_meta(),
              compressorName);
      LazyColumnPage dictionaryPage = (LazyColumnPage) decoder
          .decode(local_dictionary.getDictionary_data(), 0,
              local_dictionary.getDictionary_data().length);
      HashMap<DictionaryByteArrayWrapper, Integer> dictionaryMap = new HashMap<>();
      BitSet usedDictionaryValues = BitSet.valueOf(
          CompressorFactory.getInstance().getCompressor(compressorName)
              .unCompressByte(local_dictionary.getDictionary_values()));
      int index = 0;
      int i = usedDictionaryValues.nextSetBit(0);
      while (i >= 0) {
        dictionaryMap.put(new DictionaryByteArrayWrapper(dictionaryPage.getBytes(index)), i);
        i = usedDictionaryValues.nextSetBit(i + 1);
        index += 1;
      }
      for (i = 0; i < data.length; i++) {
        if (null == dictionaryMap.get(new DictionaryByteArrayWrapper(
            data[i].getBytes(Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET))))) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  public static Boolean checkForLocalDictionary(
      List<DimensionRawColumnChunk> dimensionRawColumnChunks) {
    Boolean isLocalDictionaryGenerated = false;
    ListIterator<DimensionRawColumnChunk> iterator = dimensionRawColumnChunks.listIterator();
    while (iterator.hasNext()) {
      if (iterator.next().getDataChunkV3().isSetLocal_dictionary()) {
        isLocalDictionaryGenerated = true;
      }
    }
    return isLocalDictionaryGenerated;
  }

  public static int getSegmentFileCount(String tableName) throws IOException {
    CarbonTable carbonTable = CarbonMetadata.getInstance().getCarbonTable(tableName);
    CarbonFile segmentsFolder = FileFactory
        .getCarbonFile(CarbonTablePath.getSegmentFilesLocation(carbonTable.getTablePath()));
    assert (segmentsFolder.isFileExist());
    return segmentsFolder.listFiles(true).size();
  }

  public static int getIndexFileCount(String tableName, String segment) throws IOException {
    return getIndexFileCount(tableName, segment, null);
  }

  public static int getIndexFileCount(String tableName,
      String segment, String extension) throws IOException {
    if (extension == null) {
      extension = CarbonTablePath.INDEX_FILE_EXT;
    }
    CarbonTable table = CarbonMetadata.getInstance().getCarbonTable(tableName);
    String path = CarbonTablePath
        .getSegmentPath(table.getAbsoluteTableIdentifier().getTablePath(), segment);
    boolean recursive = false;
    if (table.isHivePartitionTable()) {
      path = table.getAbsoluteTableIdentifier().getTablePath();
      recursive = true;
    }
    List<CarbonFile> carbonFiles = FileFactory.getCarbonFile(path).listFiles(recursive,
        file -> file.getName().endsWith(CarbonTablePath.INDEX_FILE_EXT) || file.getName()
            .endsWith(CarbonTablePath.MERGE_INDEX_FILE_EXT));
    CarbonFile[] validIndexFiles = (CarbonFile[]) SegmentFileStore
        .getValidCarbonIndexFiles(carbonFiles.toArray(new CarbonFile[carbonFiles.size()]));
    String finalExtension = extension;
    return Arrays.stream(validIndexFiles).filter(file -> file.getName().endsWith(finalExtension))
        .toArray().length;
  }

  public static void copy(String oldLoc, String newLoc) throws IOException {
    CarbonFile oldFolder = FileFactory.getCarbonFile(oldLoc);
    FileFactory.mkdirs(newLoc, FileFactory.getConfiguration());
    CarbonFile[] oldFiles = oldFolder.listFiles();
    for (CarbonFile file : oldFiles) {
      Files.copy(Paths.get(file.getParentFile().getPath(), file.getName()),
          Paths.get(newLoc, file.getName()));
    }
  }
}
