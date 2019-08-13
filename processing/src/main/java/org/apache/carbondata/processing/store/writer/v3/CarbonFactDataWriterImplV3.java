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
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.constants.CarbonVersionConstants;
import org.apache.carbondata.core.datastore.blocklet.BlockletEncodedColumnPage;
import org.apache.carbondata.core.datastore.blocklet.EncodedBlocklet;
import org.apache.carbondata.core.datastore.exception.CarbonDataWriterException;
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

import static org.apache.carbondata.core.constants.CarbonCommonConstants.TABLE_BLOCKLET_SIZE;
import static org.apache.carbondata.core.constants.CarbonV3DataFormatConstants.BLOCKLET_SIZE_IN_MB;
import static org.apache.carbondata.core.constants.CarbonV3DataFormatConstants.BLOCKLET_SIZE_IN_MB_DEFAULT_VALUE;
import static org.apache.carbondata.core.constants.SortScopeOptions.SortScope.NO_SORT;

import org.apache.log4j.Logger;

/**
 * Below class will be used to write the data in V3 format
 * <Column1 Data ChunkV3><Column1<Page1><Page2><Page3><Page4>>
 * <Column2 Data ChunkV3><Column2<Page1><Page2><Page3><Page4>>
 * <Column3 Data ChunkV3><Column3<Page1><Page2><Page3><Page4>>
 * <Column4 Data ChunkV3><Column4<Page1><Page2><Page3><Page4>>
 */
public class CarbonFactDataWriterImplV3 extends AbstractFactDataWriter {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(CarbonFactDataWriterImplV3.class.getName());

  /**
   * persist the page data to be written in the file
   */
  private BlockletDataHolder blockletDataHolder;

  /**
   * Threshold of blocklet size in MB
   */
  private long blockletSizeThreshold;

  /**
   * True if this file is sorted
   */
  private boolean isSorted;

  public CarbonFactDataWriterImplV3(CarbonFactDataHandlerModel model) {
    super(model);
    String blockletSize =
        model.getTableSpec().getCarbonTable().getTableInfo().getFactTable().getTableProperties()
            .get(TABLE_BLOCKLET_SIZE);
    if (blockletSize == null) {
      blockletSize = CarbonProperties.getInstance().getProperty(
          BLOCKLET_SIZE_IN_MB, BLOCKLET_SIZE_IN_MB_DEFAULT_VALUE);
    }
    blockletSizeThreshold = Long.parseLong(blockletSize) << 20;
    if (blockletSizeThreshold > fileSizeInBytes) {
      blockletSizeThreshold = fileSizeInBytes;
      LOGGER.info("Blocklet size configure for table is: " + blockletSizeThreshold);
    }
    blockletDataHolder = new BlockletDataHolder(fallbackExecutorService, model);
    if (model.getSortScope() != null) {
      isSorted = model.getSortScope() != NO_SORT;
    }
    LOGGER.info("Sort Scope : " + model.getSortScope());
  }

  @Override
  protected void writeFooterToFile() throws CarbonDataWriterException {
    try {
      // get the current file position
      long footerOffset = currentOffsetInFile;
      // get thrift file footer instance
      FileFooter3 convertFileMeta = CarbonMetadataUtil
          .convertFileFooterVersion3(blockletMetadata, blockletIndex, localCardinality,
              thriftColumnSchemaList.size());
      convertFileMeta.setIs_sort(isSorted);
      String appName = CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.CARBON_WRITTEN_BY_APPNAME);
      if (appName == null) {
        throw new CarbonDataWriterException(
            "DataLoading failed as CARBON_WRITTEN_BY_APPNAME is null");
      }
      convertFileMeta.putToExtra_info(CarbonCommonConstants.CARBON_WRITTEN_BY_FOOTER_INFO, appName);
      convertFileMeta.putToExtra_info(CarbonCommonConstants.CARBON_WRITTEN_VERSION,
          CarbonVersionConstants.CARBONDATA_VERSION);
      // write the footer
      byte[] byteArray = CarbonUtil.getByteArray(convertFileMeta);
      ByteBuffer buffer =
          ByteBuffer.allocate(byteArray.length + CarbonCommonConstants.LONG_SIZE_IN_BYTE);
      buffer.put(byteArray);
      buffer.putLong(footerOffset);
      buffer.flip();
      currentOffsetInFile += fileChannel.write(buffer);
      // fill the carbon index details
      fillBlockIndexInfoDetails(
          convertFileMeta.getNum_rows(), carbonDataFileName, footerOffset, currentOffsetInFile);
    } catch (IOException e) {
      LOGGER.error("Problem while writing the carbon file", e);
      throw new CarbonDataWriterException("Problem while writing the carbon file: ", e);
    }
  }

  /**
   * Below method will be used to write one table page data, invoked by Consumer
   * @param tablePage
   */
  @Override public void writeTablePage(TablePage tablePage)
      throws CarbonDataWriterException,IOException {

    // condition for writting all the pages
    if (!tablePage.isLastPage()) {
      boolean isAdded = false;
      // check if size more than blocklet size then write the page to file
      if (blockletDataHolder.getSize() + tablePage.getEncodedTablePage().getEncodedSize()
          >= blockletSizeThreshold) {
        // if blocklet size exceeds threshold, write blocklet data
        if (blockletDataHolder.getNumberOfPagesAdded() == 0) {
          isAdded = true;
          addPageData(tablePage);
        }

        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Number of Pages for blocklet is: " +
              blockletDataHolder.getNumberOfPagesAdded() +
              " :Rows Added: " + blockletDataHolder.getTotalRows());
        }

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

  private void addPageData(TablePage tablePage) throws IOException {
    blockletDataHolder.addPage(tablePage);
    if (listener != null &&
        model.getDatabaseName().equalsIgnoreCase(listener.getTblIdentifier().getDatabaseName()) &&
        model.getTableName().equalsIgnoreCase(listener.getTblIdentifier().getTableName())) {
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
    EncodedBlocklet encodedBlocklet = blockletDataHolder.getEncodedBlocklet();
    int numDimensions = encodedBlocklet.getNumberOfDimension();
    int numMeasures = encodedBlocklet.getNumberOfMeasure();

    // get data chunks for all the column
    byte[][] dataChunkBytes = new byte[numDimensions + numMeasures][];
    long metadataSize = fillDataChunk(encodedBlocklet, dataChunkBytes);
    // calculate the total size of data to be written
    long blockletSize = blockletDataHolder.getSize() + metadataSize;
    // to check if data size will exceed the block size then create a new file
    createNewFileIfReachThreshold(blockletSize);

    // write data to file
    try {
      if (currentOffsetInFile == 0) {
        // write the header if file is empty
        writeHeaderToFile();
      }
      writeBlockletToFile(dataChunkBytes);
      if (listener != null &&
          model.getDatabaseName().equalsIgnoreCase(listener.getTblIdentifier().getDatabaseName()) &&
          model.getTableName().equalsIgnoreCase(listener.getTblIdentifier().getTableName())) {
        listener.onBlockletEnd(blockletId++);
      }
      pageId = 0;
    } catch (IOException e) {
      LOGGER.error("Problem while writing file", e);
      throw new CarbonDataWriterException("Problem while writing file", e);
    } finally {
      // clear the data holder
      blockletDataHolder.clear();
    }

  }

  /**
   * Fill dataChunkBytes and return total size of page metadata
   */
  private long fillDataChunk(EncodedBlocklet encodedBlocklet, byte[][] dataChunkBytes) {
    int size = 0;
    int numDimensions = encodedBlocklet.getNumberOfDimension();
    int numMeasures = encodedBlocklet.getNumberOfMeasure();
    int measureStartIndex = numDimensions;
    // calculate the size of data chunks
    for (int i = 0; i < numDimensions; i++) {
      dataChunkBytes[i] =
          CarbonUtil.getByteArray(CarbonMetadataUtil.getDimensionDataChunk3(encodedBlocklet, i));
      size += dataChunkBytes[i].length;
    }
    for (int i = 0; i < numMeasures; i++) {
      dataChunkBytes[measureStartIndex] =
          CarbonUtil.getByteArray(CarbonMetadataUtil.getMeasureDataChunk3(encodedBlocklet, i));
      size += dataChunkBytes[measureStartIndex].length;
      measureStartIndex++;
    }
    return size;
  }

  /**
   * write file header
   */
  private void writeHeaderToFile() throws IOException {
    byte[] fileHeader = CarbonUtil.getByteArray(
        CarbonMetadataUtil.getFileHeader(
            true, thriftColumnSchemaList, model.getSchemaUpdatedTimeStamp()));
    ByteBuffer buffer = ByteBuffer.wrap(fileHeader);
    currentOffsetInFile += fileChannel.write(buffer);
  }

  /**
   * Write one blocklet data into file
   * File format:
   * <Column1 Data ChunkV3><Column1<Page1><Page2><Page3><Page4>>
   * <Column2 Data ChunkV3><Column2<Page1><Page2><Page3><Page4>>
   * <Column3 Data ChunkV3><Column3<Page1><Page2><Page3><Page4>>
   * <Column4 Data ChunkV3><Column4<Page1><Page2><Page3><Page4>>
   */
  private void writeBlockletToFile(byte[][] dataChunkBytes)
      throws IOException {
    long offset = currentOffsetInFile;
    // to maintain the offset of each data chunk in blocklet
    List<Long> currentDataChunksOffset = new ArrayList<>();
    // to maintain the length of each data chunk in blocklet
    List<Integer> currentDataChunksLength = new ArrayList<>();
    EncodedBlocklet encodedBlocklet = blockletDataHolder.getEncodedBlocklet();
    int numberOfDimension = encodedBlocklet.getNumberOfDimension();
    int numberOfMeasures = encodedBlocklet.getNumberOfMeasure();
    ByteBuffer buffer = null;
    long dimensionOffset = 0;
    long measureOffset = 0;
    for (int i = 0; i < numberOfDimension; i++) {
      currentDataChunksOffset.add(offset);
      currentDataChunksLength.add(dataChunkBytes[i].length);
      buffer = ByteBuffer.wrap(dataChunkBytes[i]);
      currentOffsetInFile += fileChannel.write(buffer);
      offset += dataChunkBytes[i].length;
      BlockletEncodedColumnPage blockletEncodedColumnPage =
          encodedBlocklet.getEncodedDimensionColumnPages().get(i);
      for (EncodedColumnPage dimensionPage : blockletEncodedColumnPage
          .getEncodedColumnPageList()) {
        buffer = dimensionPage.getEncodedData();
        int bufferSize = buffer.limit();
        currentOffsetInFile += fileChannel.write(buffer);
        offset += bufferSize;
      }
    }
    dimensionOffset = offset;
    int dataChunkStartIndex = encodedBlocklet.getNumberOfDimension();
    for (int i = 0; i < numberOfMeasures; i++) {
      currentDataChunksOffset.add(offset);
      currentDataChunksLength.add(dataChunkBytes[dataChunkStartIndex].length);
      buffer = ByteBuffer.wrap(dataChunkBytes[dataChunkStartIndex]);
      currentOffsetInFile += fileChannel.write(buffer);
      offset += dataChunkBytes[dataChunkStartIndex].length;
      dataChunkStartIndex++;
      BlockletEncodedColumnPage blockletEncodedColumnPage =
          encodedBlocklet.getEncodedMeasureColumnPages().get(i);
      for (EncodedColumnPage measurePage : blockletEncodedColumnPage
          .getEncodedColumnPageList()) {
        buffer = measurePage.getEncodedData();
        int bufferSize = buffer.limit();
        currentOffsetInFile += fileChannel.write(buffer);
        offset += bufferSize;
      }
    }
    measureOffset = offset;
    blockletIndex.add(
        CarbonMetadataUtil.getBlockletIndex(
            encodedBlocklet, model.getSegmentProperties().getMeasures()));
    BlockletInfo3 blockletInfo3 =
        new BlockletInfo3(encodedBlocklet.getBlockletSize(), currentDataChunksOffset,
            currentDataChunksLength, dimensionOffset, measureOffset,
            encodedBlocklet.getNumberOfPages());
    // Avoid storing as integer in encodedBocklet,
    // but in thrift store as int for large number of rows future support
    List<Integer> rowList = new ArrayList<>(encodedBlocklet.getRowCountInPage().size());
    for (int rows : encodedBlocklet.getRowCountInPage()) {
      rowList.add(rows);
    }
    blockletInfo3.setRow_count_in_page(rowList);
    blockletMetadata.add(blockletInfo3);
  }

  /**
   * Below method will be used to fill the block info details
   *
   * @param numberOfRows       number of rows in file
   * @param carbonDataFileName The name of carbonData file
   * @param footerOffset       footer offset
   * @param fileSize           file size
   */
  @Override
  protected void fillBlockIndexInfoDetails(long numberOfRows, String carbonDataFileName,
      long footerOffset, long fileSize) {
    int i = 0;
    DataFileFooterConverterV3 converterV3 = new DataFileFooterConverterV3();
    for (org.apache.carbondata.format.BlockletIndex index : blockletIndex) {
      BlockletInfo3 blockletInfo3 = blockletMetadata.get(i);
      BlockletInfo blockletInfo = converterV3.getBlockletInfo(blockletInfo3,
          model.getSegmentProperties().getDimensions().size());
      BlockletBTreeIndex bTreeIndex = new BlockletBTreeIndex(index.b_tree_index.getStart_key(),
          index.b_tree_index.getEnd_key());
      BlockletMinMaxIndex minMaxIndex =
          new BlockletMinMaxIndex(index.getMin_max_index().getMin_values(),
              index.getMin_max_index().getMax_values(),
              index.getMin_max_index().getMin_max_presence());
      org.apache.carbondata.core.metadata.blocklet.index.BlockletIndex bIndex =
          new org.apache.carbondata.core.metadata.blocklet.index.BlockletIndex(bTreeIndex,
              minMaxIndex);
      BlockIndexInfo biInfo =
          new BlockIndexInfo(numberOfRows, carbonDataFileName, footerOffset, bIndex,
              blockletInfo, fileSize);
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
    CarbonDataWriterException exception = null;
    try {
      commitCurrentFile(true);
      writeIndexFile();
    } catch (Exception e) {
      LOGGER.error("Problem while writing the index file", e);
      exception = new CarbonDataWriterException("Problem while writing the index file", e);
    } finally {
      try {
        closeExecutorService();
      } catch (CarbonDataWriterException e) {
        if (null == exception) {
          exception = e;
        }
      }
    }
    if (null != exception) {
      throw exception;
    }
  }

  @Override
  public void writeFooter() throws CarbonDataWriterException {
    if (this.blockletMetadata.size() > 0) {
      writeFooterToFile();
    }
  }
}
