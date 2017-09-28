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
package org.apache.carbondata.processing.store.writer.v3;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.constants.CarbonV3DataFormatConstants;
import org.apache.carbondata.core.datastore.exception.CarbonDataWriterException;
import org.apache.carbondata.core.datastore.page.EncodedTablePage;
import org.apache.carbondata.core.datastore.page.encoding.EncodedColumnPage;
import org.apache.carbondata.core.metadata.blocklet.BlockletInfo;
import org.apache.carbondata.core.metadata.blocklet.index.BlockletBTreeIndex;
import org.apache.carbondata.core.metadata.blocklet.index.BlockletMinMaxIndex;
import org.apache.carbondata.core.metadata.index.BlockIndexInfo;
import org.apache.carbondata.core.util.CarbonMetadataUtil;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataFileFooterConverterV3;
import org.apache.carbondata.format.BlockletInfo3;
import org.apache.carbondata.format.FileFooter3;
import org.apache.carbondata.processing.store.CarbonFactDataHandlerModel;
import org.apache.carbondata.processing.store.TablePage;
import org.apache.carbondata.processing.store.writer.AbstractFactDataWriter;


/**
 * Below class will be used to write the data in V3 format
 * <Column1 Data ChunkV3><Column1<Page1><Page2><Page3><Page4>>
 * <Column2 Data ChunkV3><Column2<Page1><Page2><Page3><Page4>>
 * <Column3 Data ChunkV3><Column3<Page1><Page2><Page3><Page4>>
 * <Column4 Data ChunkV3><Column4<Page1><Page2><Page3><Page4>>
 */
public class CarbonFactDataWriterImplV3 extends AbstractFactDataWriter {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonFactDataWriterImplV3.class.getName());

  /**
   * persist the page data to be written in the file
   */
  private BlockletDataHolder blockletDataHolder;

  /**
   * Threshold of blocklet size in MB
   */
  private long blockletSizeThreshold;

  public CarbonFactDataWriterImplV3(CarbonFactDataHandlerModel model) {
    super(model);
    blockletSizeThreshold = Long.parseLong(CarbonProperties.getInstance()
        .getProperty(CarbonV3DataFormatConstants.BLOCKLET_SIZE_IN_MB,
            CarbonV3DataFormatConstants.BLOCKLET_SIZE_IN_MB_DEFAULT_VALUE))
        * CarbonCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR
        * CarbonCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR;
    if (blockletSizeThreshold > fileSizeInBytes) {
      blockletSizeThreshold = fileSizeInBytes;
      LOGGER.info("Blocklet size configure for table is: " + blockletSizeThreshold);
    }
    blockletDataHolder = new BlockletDataHolder();
  }

  @Override protected void writeBlockletInfoToFile(FileChannel channel, String filePath)
      throws CarbonDataWriterException {
    try {
      // get the current file position
      long currentPosition = channel.size();
      // get thrift file footer instance
      FileFooter3 convertFileMeta = CarbonMetadataUtil
          .convertFileFooterVersion3(blockletMetadata, blockletIndex, localCardinality,
              thriftColumnSchemaList.size());
      // fill the carbon index details
      fillBlockIndexInfoDetails(convertFileMeta.getNum_rows(), carbonDataFileName, currentPosition);
      // write the footer
      byte[] byteArray = CarbonUtil.getByteArray(convertFileMeta);
      ByteBuffer buffer =
          ByteBuffer.allocate(byteArray.length + CarbonCommonConstants.LONG_SIZE_IN_BYTE);
      buffer.put(byteArray);
      buffer.putLong(currentPosition);
      buffer.flip();
      channel.write(buffer);
    } catch (IOException e) {
      LOGGER.error(e, "Problem while writing the carbon file");
      throw new CarbonDataWriterException("Problem while writing the carbon file: ", e);
    }
  }

  /**
   * Below method will be used to write one table page data, invoked by Consumer
   * @param tablePage
   */
  @Override public void writeTablePage(TablePage tablePage)
      throws CarbonDataWriterException {
    // condition for writting all the pages
    if (!tablePage.isLastPage()) {
      boolean isAdded = false;
      // check if size more than blocklet size then write the page to file
      if (blockletDataHolder.getSize() + tablePage.getEncodedTablePage().getEncodedSize() >=
          blockletSizeThreshold) {
        // if blocklet size exceeds threshold, write blocklet data
        if (blockletDataHolder.getEncodedTablePages().size() == 0) {
          isAdded = true;
          addPageData(tablePage);
        }

        LOGGER.info("Number of Pages for blocklet is: " + blockletDataHolder.getNumberOfPagesAdded()
            + " :Rows Added: " + blockletDataHolder.getTotalRows());

        // write the data
        writeBlockletToFile();

      }
      if (!isAdded) {
        addPageData(tablePage);
      }
    } else {
      //for last blocklet check if the last page will exceed the blocklet size then write
      // existing pages and then last page

      if (tablePage.getPageSize() > 0) {
        addPageData(tablePage);
      }
      if (blockletDataHolder.getNumberOfPagesAdded() > 0) {
        LOGGER.info("Number of Pages for blocklet is: " + blockletDataHolder.getNumberOfPagesAdded()
            + " :Rows Added: " + blockletDataHolder.getTotalRows());
        writeBlockletToFile();
      }
    }
  }

  private void addPageData(TablePage tablePage) {
    blockletDataHolder.addPage(tablePage);
    if (listener != null) {
      if (pageId == 0) {
        listener.onBlockletStart(blockletId);
      }
      listener.onPageAdded(blockletId, pageId++, tablePage);
    }
  }



  /**
   * Write the collect blocklet data (blockletDataHolder) to file
   */
  private void writeBlockletToFile() {
    // get the list of all encoded table page
    List<EncodedTablePage> encodedTablePageList = blockletDataHolder.getEncodedTablePages();
    int numDimensions = encodedTablePageList.get(0).getNumDimensions();
    int numMeasures = encodedTablePageList.get(0).getNumMeasures();
    // get data chunks for all the column
    byte[][] dataChunkBytes = new byte[numDimensions + numMeasures][];
    long metadataSize = fillDataChunk(encodedTablePageList, dataChunkBytes);
    // calculate the total size of data to be written
    long blockletSize = blockletDataHolder.getSize() + metadataSize;
    // to check if data size will exceed the block size then create a new file
    createNewFileIfReachThreshold(blockletSize);

    // write data to file
    try {
      if (fileChannel.size() == 0) {
        // write the header if file is empty
        writeHeaderToFile(fileChannel);
      }
      writeBlockletToFile(fileChannel, dataChunkBytes);
      if (listener != null) {
        listener.onBlockletEnd(blockletId++);
      }
      pageId = 0;
    } catch (IOException e) {
      LOGGER.error(e, "Problem while writing file");
      throw new CarbonDataWriterException("Problem while writing file", e);
    }
    // clear the data holder
    blockletDataHolder.clear();

  }

  /**
   * Fill dataChunkBytes and return total size of page metadata
   */
  private long fillDataChunk(List<EncodedTablePage> encodedTablePageList, byte[][] dataChunkBytes) {
    int size = 0;
    int numDimensions = encodedTablePageList.get(0).getNumDimensions();
    int numMeasures = encodedTablePageList.get(0).getNumMeasures();
    int measureStartIndex = numDimensions;
    // calculate the size of data chunks
    try {
      for (int i = 0; i < numDimensions; i++) {
        dataChunkBytes[i] = CarbonUtil.getByteArray(
            CarbonMetadataUtil.getDimensionDataChunk3(encodedTablePageList, i));
        size += dataChunkBytes[i].length;
      }
      for (int i = 0; i < numMeasures; i++) {
        dataChunkBytes[measureStartIndex] = CarbonUtil.getByteArray(
            CarbonMetadataUtil.getMeasureDataChunk3(encodedTablePageList, i));
        size += dataChunkBytes[measureStartIndex].length;
        measureStartIndex++;
      }
    } catch (IOException e) {
      LOGGER.error(e, "Problem while getting the data chunks");
      throw new CarbonDataWriterException("Problem while getting the data chunks", e);
    }
    return size;
  }

  /**
   * write file header
   */
  private void writeHeaderToFile(FileChannel channel) throws IOException {
    byte[] fileHeader = CarbonUtil.getByteArray(
        CarbonMetadataUtil.getFileHeader(
            true, thriftColumnSchemaList, model.getSchemaUpdatedTimeStamp()));
    ByteBuffer buffer = ByteBuffer.wrap(fileHeader);
    channel.write(buffer);
  }

  /**
   * Write one blocklet data into file
   * File format:
   * <Column1 Data ChunkV3><Column1<Page1><Page2><Page3><Page4>>
   * <Column2 Data ChunkV3><Column2<Page1><Page2><Page3><Page4>>
   * <Column3 Data ChunkV3><Column3<Page1><Page2><Page3><Page4>>
   * <Column4 Data ChunkV3><Column4<Page1><Page2><Page3><Page4>>
   */
  private void writeBlockletToFile(FileChannel channel, byte[][] dataChunkBytes)
      throws IOException {
    long offset = channel.size();
    // to maintain the offset of each data chunk in blocklet
    List<Long> currentDataChunksOffset = new ArrayList<>();
    // to maintain the length of each data chunk in blocklet
    List<Integer> currentDataChunksLength = new ArrayList<>();
    List<EncodedTablePage> encodedTablePages = blockletDataHolder.getEncodedTablePages();
    int numberOfDimension = encodedTablePages.get(0).getNumDimensions();
    int numberOfMeasures = encodedTablePages.get(0).getNumMeasures();
    ByteBuffer buffer = null;
    long dimensionOffset = 0;
    long measureOffset = 0;
    int numberOfRows = 0;
    // calculate the number of rows in each blocklet
    for (EncodedTablePage encodedTablePage : encodedTablePages) {
      numberOfRows += encodedTablePage.getPageSize();
    }
    for (int i = 0; i < numberOfDimension; i++) {
      currentDataChunksOffset.add(offset);
      currentDataChunksLength.add(dataChunkBytes[i].length);
      buffer = ByteBuffer.wrap(dataChunkBytes[i]);
      channel.write(buffer);
      offset += dataChunkBytes[i].length;
      for (EncodedTablePage encodedTablePage : encodedTablePages) {
        EncodedColumnPage dimension = encodedTablePage.getDimension(i);
        buffer = dimension.getEncodedData();
        int bufferSize = buffer.limit();
        channel.write(buffer);
        offset += bufferSize;
      }
    }
    dimensionOffset = offset;
    int dataChunkStartIndex = encodedTablePages.get(0).getNumDimensions();
    for (int i = 0; i < numberOfMeasures; i++) {
      currentDataChunksOffset.add(offset);
      currentDataChunksLength.add(dataChunkBytes[dataChunkStartIndex].length);
      buffer = ByteBuffer.wrap(dataChunkBytes[dataChunkStartIndex]);
      channel.write(buffer);
      offset += dataChunkBytes[dataChunkStartIndex].length;
      dataChunkStartIndex++;
      for (EncodedTablePage encodedTablePage : encodedTablePages) {
        EncodedColumnPage measure = encodedTablePage.getMeasure(i);
        buffer = measure.getEncodedData();
        int bufferSize = buffer.limit();
        channel.write(buffer);
        offset += bufferSize;
      }
    }
    measureOffset = offset;
    blockletIndex.add(
        CarbonMetadataUtil.getBlockletIndex(
            encodedTablePages, model.getSegmentProperties().getMeasures()));
    BlockletInfo3 blockletInfo3 =
        new BlockletInfo3(numberOfRows, currentDataChunksOffset, currentDataChunksLength,
            dimensionOffset, measureOffset, blockletDataHolder.getEncodedTablePages().size());
    blockletMetadata.add(blockletInfo3);
  }

  /**
   * Below method will be used to fill the block info details
   *
   * @param numberOfRows       number of rows in file
   * @param carbonDataFileName The name of carbonData file
   * @param currentPosition    current offset
   */
  @Override
  protected void fillBlockIndexInfoDetails(long numberOfRows, String carbonDataFileName,
      long currentPosition) {
    int i = 0;
    DataFileFooterConverterV3 converterV3 = new DataFileFooterConverterV3();
    for (org.apache.carbondata.format.BlockletIndex index : blockletIndex) {
      BlockletInfo3 blockletInfo3 = blockletMetadata.get(i);
      BlockletInfo blockletInfo = converterV3.getBlockletInfo(blockletInfo3,
          model.getSegmentProperties().getDimensions().size());
      BlockletBTreeIndex bTreeIndex = new BlockletBTreeIndex(index.b_tree_index.getStart_key(),
          index.b_tree_index.getEnd_key());
      BlockletMinMaxIndex minMaxIndex = new BlockletMinMaxIndex();
      minMaxIndex.setMinValues(toByteArray(index.getMin_max_index().getMin_values()));
      minMaxIndex.setMaxValues(toByteArray(index.getMin_max_index().getMax_values()));
      org.apache.carbondata.core.metadata.blocklet.index.BlockletIndex bIndex =
          new org.apache.carbondata.core.metadata.blocklet.index.BlockletIndex(bTreeIndex,
              minMaxIndex);
      BlockIndexInfo biInfo =
          new BlockIndexInfo(numberOfRows, carbonDataFileName, currentPosition, bIndex,
              blockletInfo);
      blockIndexInfoList.add(biInfo);
      i++;
    }
  }

  private byte[][] toByteArray(List<ByteBuffer> buffers) {
    byte[][] arrays = new byte[buffers.size()][];
    for (int i = 0; i < arrays.length; i++) {
      arrays[i] = buffers.get(i).array();
    }
    return arrays;
  }

  /**
   * Method will be used to close the open file channel
   *
   * @throws CarbonDataWriterException
   */
  public void closeWriter() throws CarbonDataWriterException {
    commitCurrentFile(true);
    try {
      writeIndexFile();
    } catch (IOException e) {
      LOGGER.error(e, "Problem while writing the index file");
      throw new CarbonDataWriterException("Problem while writing the index file", e);
    }
    closeExecutorService();
  }

  @Override public void writeFooterToFile() throws CarbonDataWriterException {
    if (this.blockletMetadata.size() > 0) {
      writeBlockletInfoToFile(fileChannel, carbonDataFileTempPath);
    }
  }
}
