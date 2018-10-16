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
package org.apache.carbondata.core.datastore.chunk.reader.dimension.v3;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.FileReader;
import org.apache.carbondata.core.datastore.chunk.DimensionColumnPage;
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.impl.FixedLengthDimensionColumnPage;
import org.apache.carbondata.core.datastore.chunk.impl.VariableLengthDimensionColumnPage;
import org.apache.carbondata.core.datastore.chunk.reader.dimension.AbstractChunkReaderV2V3Format;
import org.apache.carbondata.core.datastore.chunk.store.ColumnPageWrapper;
import org.apache.carbondata.core.datastore.chunk.store.DimensionChunkStoreFactory;
import org.apache.carbondata.core.datastore.columnar.UnBlockIndexer;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageDecoder;
import org.apache.carbondata.core.datastore.page.encoding.DefaultEncodingFactory;
import org.apache.carbondata.core.datastore.page.encoding.EncodingFactory;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.blocklet.BlockletInfo;
import org.apache.carbondata.core.scan.executor.util.QueryUtil;
import org.apache.carbondata.core.scan.result.vector.ColumnVectorInfo;
import org.apache.carbondata.core.util.CarbonMetadataUtil;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.format.DataChunk2;
import org.apache.carbondata.format.DataChunk3;
import org.apache.carbondata.format.Encoding;

import org.apache.commons.lang.ArrayUtils;

/**
 * Dimension column V3 Reader class which will be used to read and uncompress
 * V3 format data
 * data format
 * Data Format
 * <FileHeader>
 * <Column1 Data ChunkV3><Column1<Page1><Page2><Page3><Page4>>
 * <Column2 Data ChunkV3><Column2<Page1><Page2><Page3><Page4>>
 * <Column3 Data ChunkV3><Column3<Page1><Page2><Page3><Page4>>
 * <Column4 Data ChunkV3><Column4<Page1><Page2><Page3><Page4>>
 * <File Footer>
 */
public class CompressedDimensionChunkFileBasedReaderV3 extends AbstractChunkReaderV2V3Format {

  private EncodingFactory encodingFactory = DefaultEncodingFactory.getInstance();

  /**
   * end position of last dimension in carbon data file
   */
  private long lastDimensionOffsets;

  public CompressedDimensionChunkFileBasedReaderV3(BlockletInfo blockletInfo,
      int[] eachColumnValueSize, String filePath) {
    super(blockletInfo, eachColumnValueSize, filePath);
    lastDimensionOffsets = blockletInfo.getDimensionOffset();
  }

  /**
   * Below method will be used to read the dimension column data form carbon data file
   * Steps for reading
   * 1. Get the length of the data to be read
   * 2. Allocate the direct buffer
   * 3. read the data from file
   * 4. Get the data chunk object from data read
   * 5. Create the raw chunk object and fill the details
   *
   * @param fileReader          reader for reading the column from carbon data file
   * @param columnIndex blocklet index of the column in carbon data file
   * @return dimension raw chunk
   */
  public DimensionRawColumnChunk readRawDimensionChunk(FileReader fileReader,
      int columnIndex) throws IOException {
    // get the current dimension offset
    long currentDimensionOffset = dimensionChunksOffset.get(columnIndex);
    int length = 0;
    // to calculate the length of the data to be read
    // column other than last column we can subtract the offset of current column with
    // next column and get the total length.
    // but for last column we need to use lastDimensionOffset which is the end position
    // of the last dimension, we can subtract current dimension offset from lastDimesionOffset
    if (dimensionChunksOffset.size() - 1 == columnIndex) {
      length = (int) (lastDimensionOffsets - currentDimensionOffset);
    } else {
      length = (int) (dimensionChunksOffset.get(columnIndex + 1) - currentDimensionOffset);
    }
    ByteBuffer buffer = null;
    // read the data from carbon data file
    synchronized (fileReader) {
      buffer = fileReader.readByteBuffer(filePath, currentDimensionOffset, length);
    }
    // get the data chunk which will have all the details about the data pages
    DataChunk3 dataChunk = CarbonUtil.readDataChunk3(buffer, 0, length);
    return getDimensionRawColumnChunk(fileReader, columnIndex, 0, length, buffer,
        dataChunk);
  }

  protected DimensionRawColumnChunk getDimensionRawColumnChunk(FileReader fileReader,
      int columnIndex, long offset, int length, ByteBuffer buffer, DataChunk3 dataChunk) {
    // creating a raw chunks instance and filling all the details
    DimensionRawColumnChunk rawColumnChunk =
        new DimensionRawColumnChunk(columnIndex, buffer, offset, length, this);
    int numberOfPages = dataChunk.getPage_length().size();
    byte[][] maxValueOfEachPage = new byte[numberOfPages][];
    byte[][] minValueOfEachPage = new byte[numberOfPages][];
    boolean[] minMaxFlag = new boolean[minValueOfEachPage.length];
    Arrays.fill(minMaxFlag, true);
    int[] eachPageLength = new int[numberOfPages];
    for (int i = 0; i < minValueOfEachPage.length; i++) {
      maxValueOfEachPage[i] =
          dataChunk.getData_chunk_list().get(i).getMin_max().getMax_values().get(0).array();
      minValueOfEachPage[i] =
          dataChunk.getData_chunk_list().get(i).getMin_max().getMin_values().get(0).array();
      eachPageLength[i] = dataChunk.getData_chunk_list().get(i).getNumberOfRowsInpage();
      boolean isMinMaxFlagSet =
          dataChunk.getData_chunk_list().get(i).getMin_max().isSetMin_max_presence();
      if (isMinMaxFlagSet) {
        minMaxFlag[i] =
            dataChunk.getData_chunk_list().get(i).getMin_max().getMin_max_presence().get(0);
      }
    }
    rawColumnChunk.setDataChunkV3(dataChunk);
    rawColumnChunk.setFileReader(fileReader);
    rawColumnChunk.setPagesCount(dataChunk.getPage_length().size());
    rawColumnChunk.setMaxValues(maxValueOfEachPage);
    rawColumnChunk.setMinValues(minValueOfEachPage);
    rawColumnChunk.setMinMaxFlagArray(minMaxFlag);
    rawColumnChunk.setRowCount(eachPageLength);
    rawColumnChunk.setOffsets(ArrayUtils
        .toPrimitive(dataChunk.page_offset.toArray(new Integer[dataChunk.page_offset.size()])));
    return rawColumnChunk;
  }

  /**
   * Below method will be used to read the multiple dimension column data in group
   * and divide into dimension raw chunk object
   * Steps for reading
   * 1. Get the length of the data to be read
   * 2. Allocate the direct buffer
   * 3. read the data from file
   * 4. Get the data chunk object from file for each column
   * 5. Create the raw chunk object and fill the details for each column
   * 6. increment the offset of the data
   *
   * @param fileReader
   *        reader which will be used to read the dimension columns data from file
   * @param startBlockletColumnIndex
   *        blocklet index of the first dimension column
   * @param endBlockletColumnIndex
   *        blocklet index of the last dimension column
   * @ DimensionRawColumnChunk array
   */
  protected DimensionRawColumnChunk[] readRawDimensionChunksInGroup(FileReader fileReader,
      int startBlockletColumnIndex, int endBlockletColumnIndex) throws IOException {
    // to calculate the length of the data to be read
    // column we can subtract the offset of start column offset with
    // end column+1 offset and get the total length.
    long currentDimensionOffset = dimensionChunksOffset.get(startBlockletColumnIndex);
    ByteBuffer buffer = null;
    // read the data from carbon data file
    synchronized (fileReader) {
      buffer = fileReader.readByteBuffer(filePath, currentDimensionOffset,
          (int) (dimensionChunksOffset.get(endBlockletColumnIndex + 1) - currentDimensionOffset));
    }
    // create raw chunk for each dimension column
    DimensionRawColumnChunk[] dimensionDataChunks =
        new DimensionRawColumnChunk[endBlockletColumnIndex - startBlockletColumnIndex + 1];
    int index = 0;
    int runningLength = 0;
    for (int i = startBlockletColumnIndex; i <= endBlockletColumnIndex; i++) {
      int currentLength = (int) (dimensionChunksOffset.get(i + 1) - dimensionChunksOffset.get(i));
      DataChunk3 dataChunk =
          CarbonUtil.readDataChunk3(buffer, runningLength, dimensionChunksLength.get(i));
      dimensionDataChunks[index] =
          getDimensionRawColumnChunk(fileReader, i, runningLength, currentLength, buffer,
              dataChunk);
      runningLength += currentLength;
      index++;
    }
    return dimensionDataChunks;
  }

  /**
   * Below method will be used to convert the compressed dimension chunk raw data to actual data
   *
   * @param rawColumnPage dimension raw chunk
   * @param pageNumber              number
   * @return DimensionColumnPage
   */
  @Override public DimensionColumnPage decodeColumnPage(
      DimensionRawColumnChunk rawColumnPage, int pageNumber) throws IOException, MemoryException {
    return decodeColumnPage(rawColumnPage, pageNumber, null);
  }

  private DimensionColumnPage decodeColumnPage(
      DimensionRawColumnChunk rawColumnPage, int pageNumber,
      ColumnVectorInfo vectorInfo) throws IOException, MemoryException {
    // data chunk of blocklet column
    DataChunk3 dataChunk3 = rawColumnPage.getDataChunkV3();
    // get the data buffer
    ByteBuffer rawData = rawColumnPage.getRawData();
    DataChunk2 pageMetadata = dataChunk3.getData_chunk_list().get(pageNumber);
    String compressorName = CarbonMetadataUtil.getCompressorNameFromChunkMeta(
        pageMetadata.getChunk_meta());
    this.compressor = CompressorFactory.getInstance().getCompressor(compressorName);
    // calculating the start point of data
    // as buffer can contain multiple column data, start point will be datachunkoffset +
    // data chunk length + page offset
    int offset = (int) rawColumnPage.getOffSet() + dimensionChunksLength
        .get(rawColumnPage.getColumnIndex()) + dataChunk3.getPage_offset().get(pageNumber);
    // first read the data and uncompressed it
    return decodeDimension(rawColumnPage, rawData, pageMetadata, offset, vectorInfo);
  }

  @Override
  public void decodeColumnPageAndFillVector(DimensionRawColumnChunk dimensionRawColumnChunk,
      int pageNumber, ColumnVectorInfo vectorInfo) throws IOException, MemoryException {
    DimensionColumnPage columnPage =
        decodeColumnPage(dimensionRawColumnChunk, pageNumber, vectorInfo);
    columnPage.freeMemory();
  }

  private ColumnPage decodeDimensionByMeta(DataChunk2 pageMetadata, ByteBuffer pageData, int offset,
      boolean isLocalDictEncodedPage, ColumnVectorInfo vectorInfo, BitSet nullBitSet)
      throws IOException, MemoryException {
    List<Encoding> encodings = pageMetadata.getEncoders();
    List<ByteBuffer> encoderMetas = pageMetadata.getEncoder_meta();
    String compressorName = CarbonMetadataUtil.getCompressorNameFromChunkMeta(
        pageMetadata.getChunk_meta());
    ColumnPageDecoder decoder = encodingFactory.createDecoder(encodings, encoderMetas,
        compressorName, vectorInfo != null);
    if (vectorInfo != null) {
      return decoder
          .decodeAndFillVector(pageData.array(), offset, pageMetadata.data_page_length, vectorInfo,
              nullBitSet, isLocalDictEncodedPage);
    } else {
      return decoder
          .decode(pageData.array(), offset, pageMetadata.data_page_length, isLocalDictEncodedPage);
    }
  }

  protected DimensionColumnPage decodeDimension(DimensionRawColumnChunk rawColumnPage,
      ByteBuffer pageData, DataChunk2 pageMetadata, int offset, ColumnVectorInfo vectorInfo)
      throws IOException, MemoryException {
    List<Encoding> encodings = pageMetadata.getEncoders();
    if (CarbonUtil.isEncodedWithMeta(encodings)) {
      int[] invertedIndexes = new int[0];
      int[] invertedIndexesReverse = new int[0];
      // in case of no dictionary measure data types, if it is included in sort columns
      // then inverted index to be uncompressed
      boolean isExplicitSorted =
          CarbonUtil.hasEncoding(pageMetadata.encoders, Encoding.INVERTED_INDEX);
      int dataOffset = offset;
      if (encodings.contains(Encoding.INVERTED_INDEX)) {
        offset += pageMetadata.data_page_length;
        if (isExplicitSorted) {
          invertedIndexes = CarbonUtil
              .getUnCompressColumnIndex(pageMetadata.rowid_page_length, pageData, offset);
          // get the reverse index
          invertedIndexesReverse = CarbonUtil.getInvertedReverseIndex(invertedIndexes);
        }
      }
      BitSet nullBitSet = QueryUtil.getNullBitSet(pageMetadata.presence, this.compressor);
      ColumnPage decodedPage = decodeDimensionByMeta(pageMetadata, pageData, dataOffset,
          null != rawColumnPage.getLocalDictionary(), vectorInfo, nullBitSet);
      decodedPage.setNullBits(nullBitSet);
      return new ColumnPageWrapper(decodedPage, rawColumnPage.getLocalDictionary(), invertedIndexes,
          invertedIndexesReverse, isEncodedWithAdaptiveMeta(pageMetadata), isExplicitSorted);
    } else {
      // following code is for backward compatibility
      return decodeDimensionLegacy(rawColumnPage, pageData, pageMetadata, offset, vectorInfo);
    }
  }

  public boolean isEncodedWithAdaptiveMeta(DataChunk2 pageMetadata) {
    List<Encoding> encodings = pageMetadata.getEncoders();
    if (encodings != null && !encodings.isEmpty()) {
      Encoding encoding = encodings.get(0);
      switch (encoding) {
        case ADAPTIVE_INTEGRAL:
        case ADAPTIVE_DELTA_INTEGRAL:
        case ADAPTIVE_FLOATING:
        case ADAPTIVE_DELTA_FLOATING:
          return true;
      }
    }
    return false;
  }

  private DimensionColumnPage decodeDimensionLegacy(DimensionRawColumnChunk rawColumnPage,
      ByteBuffer pageData, DataChunk2 pageMetadata, int offset, ColumnVectorInfo vectorInfo)
      throws IOException, MemoryException {
    byte[] dataPage;
    int[] rlePage;
    int[] invertedIndexes = new int[0];
    int[] invertedIndexesReverse = new int[0];
    dataPage = compressor.unCompressByte(pageData.array(), offset, pageMetadata.data_page_length);
    offset += pageMetadata.data_page_length;
    // if row id block is present then read the row id chunk and uncompress it
    if (CarbonUtil.hasEncoding(pageMetadata.encoders, Encoding.INVERTED_INDEX)) {
      invertedIndexes = CarbonUtil
          .getUnCompressColumnIndex(pageMetadata.rowid_page_length, pageData, offset);
      offset += pageMetadata.rowid_page_length;
      if (vectorInfo == null) {
        // get the reverse index
        invertedIndexesReverse = CarbonUtil.getInvertedReverseIndex(invertedIndexes);
      }
    }
    // if rle is applied then read the rle block chunk and then uncompress
    //then actual data based on rle block
    if (CarbonUtil.hasEncoding(pageMetadata.encoders, Encoding.RLE)) {
      rlePage =
          CarbonUtil.getIntArray(pageData, offset, pageMetadata.rle_page_length);
      // uncompress the data with rle indexes
      dataPage = UnBlockIndexer.uncompressData(dataPage, rlePage,
          null == rawColumnPage.getLocalDictionary() ?
              eachColumnValueSize[rawColumnPage.getColumnIndex()] :
              CarbonCommonConstants.LOCAL_DICT_ENCODED_BYTEARRAY_SIZE);
    }

    DimensionColumnPage columnDataChunk = null;
    // if no dictionary column then first create a no dictionary column chunk
    // and set to data chunk instance
    if (!CarbonUtil.hasEncoding(pageMetadata.encoders, Encoding.DICTIONARY)) {
      DimensionChunkStoreFactory.DimensionStoreType dimStoreType =
          null != rawColumnPage.getLocalDictionary() ?
              DimensionChunkStoreFactory.DimensionStoreType.LOCAL_DICT :
              (CarbonUtil.hasEncoding(pageMetadata.encoders, Encoding.DIRECT_COMPRESS_VARCHAR) ?
                  DimensionChunkStoreFactory.DimensionStoreType.VARIABLE_INT_LENGTH :
                  DimensionChunkStoreFactory.DimensionStoreType.VARIABLE_SHORT_LENGTH);
      columnDataChunk =
          new VariableLengthDimensionColumnPage(dataPage, invertedIndexes, invertedIndexesReverse,
              pageMetadata.getNumberOfRowsInpage(), dimStoreType,
              rawColumnPage.getLocalDictionary(), vectorInfo);
    } else {
      // to store fixed length column chunk values
      columnDataChunk =
          new FixedLengthDimensionColumnPage(dataPage, invertedIndexes, invertedIndexesReverse,
              pageMetadata.getNumberOfRowsInpage(),
              eachColumnValueSize[rawColumnPage.getColumnIndex()], vectorInfo);
    }
    return columnDataChunk;
  }
}
