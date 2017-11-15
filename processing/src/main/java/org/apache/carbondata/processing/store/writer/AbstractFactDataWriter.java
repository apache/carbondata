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

package org.apache.carbondata.processing.store.writer;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.exception.CarbonDataWriterException;
import org.apache.carbondata.core.keygenerator.mdkey.NumberCompressor;
import org.apache.carbondata.core.metadata.converter.SchemaConverter;
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl;
import org.apache.carbondata.core.metadata.index.BlockIndexInfo;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.util.CarbonMergerUtil;
import org.apache.carbondata.core.util.CarbonMetadataUtil;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonThreadFactory;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.core.writer.CarbonIndexFileWriter;
import org.apache.carbondata.format.BlockIndex;
import org.apache.carbondata.format.BlockletInfo3;
import org.apache.carbondata.format.IndexHeader;
import org.apache.carbondata.processing.datamap.DataMapWriterListener;
import org.apache.carbondata.processing.store.CarbonFactDataHandlerModel;

import org.apache.commons.lang3.ArrayUtils;

public abstract class AbstractFactDataWriter implements CarbonFactDataWriter {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(AbstractFactDataWriter.class.getName());

  /**
   * file channel
   */
  protected FileChannel fileChannel;
  /**
   * The temp path of carbonData file used on executor
   */
  protected String carbonDataFileTempPath;

  /**
   * The name of carbonData file (blockId)
   */
  protected String carbonDataFileName;

  /**
   * The sequence number of blocklet inside one block
   */
  protected int blockletId = 0;

  /**
   * The sequence number of page inside one blocklet
   */
  protected int pageId = 0;

  /**
   * Local cardinality for the segment
   */
  protected int[] localCardinality;
  /**
   * thrift column schema
   */
  protected List<org.apache.carbondata.format.ColumnSchema> thriftColumnSchemaList;
  protected NumberCompressor numberCompressor;
  protected CarbonFactDataHandlerModel model;
  protected List<List<Long>> dataChunksOffsets;
  protected List<List<Short>> dataChunksLength;
  /**
   * data file size;
   */
  protected long fileSizeInBytes;
  /**
   * file count will be used to give sequence number to the data file
   */
  private int fileCount;
  /**
   * executorService
   */
  private ExecutorService executorService;
  /**
   * executorService
   */
  private List<Future<Void>> executorServiceSubmitList;
  /**
   * data block size for one carbon data file
   */
  private long blockSizeThreshold;
  /**
   * file size at any given point
   */
  private long currentFileSize;

  protected FileOutputStream fileOutputStream;

  protected List<BlockIndexInfo> blockIndexInfoList;

  /**
   * list of metadata for V3 format
   */
  protected List<BlockletInfo3> blockletMetadata;

  /**
   * list of blocklet index
   */
  protected List<org.apache.carbondata.format.BlockletIndex> blockletIndex;

  /**
   * listener to write data map
   */
  protected DataMapWriterListener listener;

  public AbstractFactDataWriter(CarbonFactDataHandlerModel model) {
    this.model = model;
    blockIndexInfoList = new ArrayList<>();
    // get max file size;
    CarbonProperties propInstance = CarbonProperties.getInstance();
    // if blocksize=2048, then 2048*1024*1024 will beyond the range of Int
    this.fileSizeInBytes =
        (long) this.model.getBlockSizeInMB() * CarbonCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR
            * CarbonCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR;

    // size reserved in one file for writing block meta data. It will be in percentage
    int spaceReservedForBlockMetaSize = Integer.parseInt(propInstance
        .getProperty(CarbonCommonConstants.CARBON_BLOCK_META_RESERVED_SPACE,
            CarbonCommonConstants.CARBON_BLOCK_META_RESERVED_SPACE_DEFAULT));
    this.blockSizeThreshold =
        fileSizeInBytes - (fileSizeInBytes * spaceReservedForBlockMetaSize) / 100;
    LOGGER.info("Total file size: " + fileSizeInBytes + " and dataBlock Size: " +
        blockSizeThreshold);

    this.executorService = Executors.newFixedThreadPool(1,
        new CarbonThreadFactory("LocalToHDFSCopyPool:" + this.model.getTableName()));
    executorServiceSubmitList = new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    // in case of compaction we will pass the cardinality.
    this.localCardinality = this.model.getColCardinality();

    //TODO: We should delete the levelmetadata file after reading here.
    // so only data loading flow will need to read from cardinality file.
    if (null == this.localCardinality) {
      this.localCardinality = CarbonMergerUtil
          .getCardinalityFromLevelMetadata(this.model.getStoreLocation(),
              this.model.getTableName());
      List<Integer> cardinalityList = new ArrayList<Integer>();
      thriftColumnSchemaList = getColumnSchemaListAndCardinality(cardinalityList, localCardinality,
          this.model.getWrapperColumnSchema());
      localCardinality =
          ArrayUtils.toPrimitive(cardinalityList.toArray(new Integer[cardinalityList.size()]));
    } else { // for compaction case
      List<Integer> cardinalityList = new ArrayList<Integer>();
      thriftColumnSchemaList = getColumnSchemaListAndCardinality(cardinalityList, localCardinality,
          this.model.getWrapperColumnSchema());
    }
    this.numberCompressor = new NumberCompressor(Integer.parseInt(CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.BLOCKLET_SIZE,
            CarbonCommonConstants.BLOCKLET_SIZE_DEFAULT_VAL)));
    this.dataChunksOffsets = new ArrayList<>();
    this.dataChunksLength = new ArrayList<>();
    blockletMetadata = new ArrayList<BlockletInfo3>();
    blockletIndex = new ArrayList<>();
    listener = this.model.getDataMapWriterlistener();
  }

  /**
   * This method will be used to update the file channel with new file if exceeding block size
   * threshold, new file will be created once existing file reached the file size limit This
   * method will first check whether existing file size is exceeded the file
   * size limit if yes then write the leaf metadata to file then set the
   * current file size to 0 close the existing file channel get the new file
   * name and get the channel for new file
   *
   * @param blockletSizeToBeAdded data size of one block
   * @throws CarbonDataWriterException if any problem
   */
  protected void createNewFileIfReachThreshold(long blockletSizeToBeAdded)
      throws CarbonDataWriterException {
    if ((currentFileSize + blockletSizeToBeAdded) >= blockSizeThreshold && currentFileSize != 0) {
      // set the current file size to zero
      LOGGER.info("Writing data to file as max file size reached for file: "
          + carbonDataFileTempPath + " .Data block size: " + currentFileSize);
      // write meta data to end of the existing file
      writeBlockletInfoToFile(fileChannel, carbonDataFileTempPath);
      this.currentFileSize = 0;
      this.dataChunksOffsets = new ArrayList<>();
      this.dataChunksLength = new ArrayList<>();
      this.blockletMetadata = new ArrayList<>();
      this.blockletIndex = new ArrayList<>();
      commitCurrentFile(false);
      // initialize the new channel
      initializeWriter();
    }
    currentFileSize += blockletSizeToBeAdded;
  }

  private void notifyDataMapBlockStart() {
    if (listener != null) {
      listener.onBlockStart(carbonDataFileName, constructFactFileFullPath());
    }
  }

  private void notifyDataMapBlockEnd() {
    if (listener != null) {
      listener.onBlockEnd(carbonDataFileName);
    }
    blockletId = 0;
  }

  private String constructFactFileFullPath() {
    String factFilePath =
        this.model.getCarbonDataDirectoryPath() + File.separator + this.carbonDataFileName;
    return factFilePath;
  }
  /**
   * Finish writing current file. It will flush stream, copy and rename temp file to final file
   * @param copyInCurrentThread set to false if want to do data copy in a new thread
   */
  protected void commitCurrentFile(boolean copyInCurrentThread) {
    notifyDataMapBlockEnd();
    CarbonUtil.closeStreams(this.fileOutputStream, this.fileChannel);
    if (copyInCurrentThread) {
      CarbonUtil.copyCarbonDataFileToCarbonStorePath(
          carbonDataFileTempPath, model.getCarbonDataDirectoryPath(),
          fileSizeInBytes);
    } else {
      executorServiceSubmitList.add(executorService.submit(new CopyThread(carbonDataFileTempPath)));
    }
  }

  /**
   * This method will be used to initialize the channel
   *
   * @throws CarbonDataWriterException
   */
  public void initializeWriter() throws CarbonDataWriterException {
    // update the filename with new new sequence
    // increment the file sequence counter
    initFileCount();

    //each time we initialize writer, we choose a local temp location randomly
    String[] tempFileLocations = model.getStoreLocation();
    String chosenTempLocation = tempFileLocations[new Random().nextInt(tempFileLocations.length)];
    LOGGER.info("Randomly choose factdata temp location: " + chosenTempLocation);

    this.carbonDataFileName = CarbonTablePath
        .getCarbonDataFileName(fileCount, model.getCarbonDataFileAttributes().getTaskId(),
            model.getBucketId(), model.getTaskExtension(),
            "" + model.getCarbonDataFileAttributes().getFactTimeStamp());
    this.carbonDataFileTempPath = chosenTempLocation + File.separator + carbonDataFileName;
    this.fileCount++;
    try {
      // open channel for new data file
      fileOutputStream = new FileOutputStream(this.carbonDataFileTempPath, true);
      this.fileChannel = fileOutputStream.getChannel();
    } catch (FileNotFoundException fileNotFoundException) {
      throw new CarbonDataWriterException("Problem while getting the FileChannel for Leaf File",
          fileNotFoundException);
    }
    notifyDataMapBlockStart();
  }

  private int initFileCount() {
    int fileInitialCount = 0;
    FileFilter fileFilter = new FileFilter() {
      @Override public boolean accept(File pathVal) {
        if (!pathVal.isDirectory() && pathVal.getName().startsWith(model.getTableName())
            && pathVal.getName().contains(CarbonCommonConstants.FACT_FILE_EXT)) {
          return true;
        }
        return false;
      }
    };

    List<File> dataFileList = new ArrayList<File>();
    for (String tempLoc : model.getStoreLocation()) {
      File[] subFiles = new File(tempLoc).listFiles(fileFilter);
      if (null != subFiles && subFiles.length > 0) {
        dataFileList.addAll(Arrays.asList(subFiles));
      }
    }

    File[] dataFiles = new File[dataFileList.size()];
    dataFileList.toArray(dataFiles);
    if (dataFiles != null && dataFiles.length > 0) {
      // since files are in different directory, we should only compare the file name
      // and ignore the directory
      Arrays.sort(dataFiles, new Comparator<File>() {
        @Override public int compare(File o1, File o2) {
          return o1.getName().compareTo(o2.getName());
        }
      });
      String dataFileName = dataFiles[dataFiles.length - 1].getName();
      try {
        fileInitialCount = Integer
            .parseInt(dataFileName.substring(dataFileName.lastIndexOf('_') + 1).split("\\.")[0]);
      } catch (NumberFormatException ex) {
        fileInitialCount = 0;
      }
      fileInitialCount++;
    }
    return fileInitialCount;
  }

  /**
   * This method will write metadata at the end of file file format in thrift format
   */
  protected abstract void writeBlockletInfoToFile(
      FileChannel channel, String filePath) throws CarbonDataWriterException;

  /**
   * Below method will be used to fill the vlock info details
   *
   * @param numberOfRows    number of rows in file
   * @param carbonDataFileName The name of carbonData file
   * @param currentPosition current offset
   */
  protected abstract void fillBlockIndexInfoDetails(long numberOfRows, String carbonDataFileName,
      long currentPosition);

  public static List<org.apache.carbondata.format.ColumnSchema> getColumnSchemaListAndCardinality(
      List<Integer> cardinality, int[] dictionaryColumnCardinality,
      List<ColumnSchema> wrapperColumnSchemaList) {
    List<org.apache.carbondata.format.ColumnSchema> columnSchemaList =
        new ArrayList<org.apache.carbondata.format.ColumnSchema>(
            CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    SchemaConverter schemaConverter = new ThriftWrapperSchemaConverterImpl();
    int counter = 0;
    for (int i = 0; i < wrapperColumnSchemaList.size(); i++) {
      columnSchemaList
          .add(schemaConverter.fromWrapperToExternalColumnSchema(wrapperColumnSchemaList.get(i)));
      if (CarbonUtil.hasEncoding(wrapperColumnSchemaList.get(i).getEncodingList(),
          org.apache.carbondata.core.metadata.encoder.Encoding.DICTIONARY)) {
        cardinality.add(dictionaryColumnCardinality[counter]);
        counter++;
      } else if (!wrapperColumnSchemaList.get(i).isDimensionColumn()) {
        continue;
      } else {
        cardinality.add(-1);
      }
    }
    return columnSchemaList;
  }

  /**
   * Below method will be used to write the idex file
   *
   * @throws IOException               throws io exception if any problem while writing
   * @throws CarbonDataWriterException data writing
   */
  protected void writeIndexFile() throws IOException, CarbonDataWriterException {
    // get the header
    IndexHeader indexHeader = CarbonMetadataUtil
        .getIndexHeader(localCardinality, thriftColumnSchemaList, model.getBucketId(),
            model.getSchemaUpdatedTimeStamp());
    // get the block index info thrift
    List<BlockIndex> blockIndexThrift = CarbonMetadataUtil.getBlockIndexInfo(blockIndexInfoList);
    // randomly choose a temp location for index file
    String[] tempLocations = model.getStoreLocation();
    String chosenTempLocation = tempLocations[new Random().nextInt(tempLocations.length)];
    LOGGER.info("Randomly choose index file location: " + chosenTempLocation);

    String fileName = chosenTempLocation + File.separator + CarbonTablePath
        .getCarbonIndexFileName(model.getCarbonDataFileAttributes().getTaskId(),
            model.getBucketId(), model.getTaskExtension(),
            "" + model.getCarbonDataFileAttributes().getFactTimeStamp());
    CarbonIndexFileWriter writer = new CarbonIndexFileWriter();
    // open file
    writer.openThriftWriter(fileName);
    // write the header first
    writer.writeThrift(indexHeader);
    // write the indexes
    for (BlockIndex blockIndex : blockIndexThrift) {
      writer.writeThrift(blockIndex);
    }
    writer.close();
    // copy from temp to actual store location
    CarbonUtil.copyCarbonDataFileToCarbonStorePath(fileName,
            model.getCarbonDataDirectoryPath(),
            fileSizeInBytes);
  }

  /**
   * This method will close the executor service which is used for copying carbon
   * data files to carbon store path
   *
   * @throws CarbonDataWriterException
   */
  protected void closeExecutorService() throws CarbonDataWriterException {
    try {
      listener.finish();
      executorService.shutdown();
      executorService.awaitTermination(2, TimeUnit.HOURS);
      for (int i = 0; i < executorServiceSubmitList.size(); i++) {
        executorServiceSubmitList.get(i).get();
      }
    } catch (InterruptedException | ExecutionException | IOException e) {
      LOGGER.error(e, "Error while finishing writer");
      throw new CarbonDataWriterException(e.getMessage());
    }
  }



  /**
   * This method will copy the carbon data file from local store location to
   * carbon store location
   */
  private final class CopyThread implements Callable<Void> {

    /**
     * complete path along with file name which needs to be copied to
     * carbon store path
     */
    private String fileName;

    private CopyThread(String fileName) {
      this.fileName = fileName;
    }

    /**
     * Computes a result, or throws an exception if unable to do so.
     *
     * @return computed result
     * @throws Exception if unable to compute a result
     */
    @Override public Void call() throws Exception {
      CarbonUtil.copyCarbonDataFileToCarbonStorePath(
          fileName,
          model.getCarbonDataDirectoryPath(),
          fileSizeInBytes);
      return null;
    }

  }
}
