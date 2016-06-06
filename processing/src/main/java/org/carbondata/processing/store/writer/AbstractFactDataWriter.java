/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.carbondata.processing.store.writer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.CarbonTableIdentifier;
import org.carbondata.core.carbon.metadata.converter.SchemaConverter;
import org.carbondata.core.carbon.metadata.converter.ThriftWrapperSchemaConverterImpl;
import org.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema;
import org.carbondata.core.carbon.path.CarbonStorePath;
import org.carbondata.core.carbon.path.CarbonTablePath;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.file.manager.composite.FileData;
import org.carbondata.core.file.manager.composite.IFileManagerComposite;
import org.carbondata.core.metadata.BlockletInfoColumnar;
import org.carbondata.core.util.CarbonMergerUtil;
import org.carbondata.core.util.CarbonMetadataUtil;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.core.writer.CarbonFooterWriter;
import org.carbondata.format.FileFooter;
import org.carbondata.processing.store.CarbonDataFileAttributes;
import org.carbondata.processing.store.writer.exception.CarbonDataWriterException;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.io.IOUtils;

public abstract class AbstractFactDataWriter<T> implements CarbonFactDataWriter<T>

{

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(AbstractFactDataWriter.class.getName());

  /**
   * dfs.bytes-per-checksum
   * HDFS checksum length, block size for a file should be exactly divisible
   * by this value
   */
  private static final int HDFS_CHECKSUM_LENGTH = 512;
  /**
   * measure count
   */
  protected int measureCount;
  /**
   * file channel
   */
  protected FileChannel fileChannel;
  /**
   * this will be used for holding blocklet metadata
   */
  protected List<BlockletInfoColumnar> blockletInfoList;
  /**
   * keyBlockSize
   */
  protected int[] keyBlockSize;
  protected boolean[] isNoDictionary;
  /**
   * mdkeySize
   */
  protected int mdkeySize;
  /**
   * file name
   */
  protected String fileName;
  /**
   * Local cardinality for the segment
   */
  protected int[] localCardinality;
  protected String databaseName;
  /**
   * thrift column schema
   */
  protected List<org.carbondata.format.ColumnSchema> thriftColumnSchemaList;
  /**
   * tabel name
   */
  private String tableName;
  /**
   * data file size;
   */
  private long fileSizeInBytes;
  /**
   * file count will be used to give sequence number to the data file
   */
  private int fileCount;
  /**
   * File manager
   */
  private IFileManagerComposite fileManager;
  /**
   * Store Location
   */
  private String storeLocation;
  /**
   * executorService
   */
  private ExecutorService executorService;

  /**
   * executorService
   */
  private List<Future<Void>> executorServiceSubmitList;
  /**
   * data file attributes which will used for file construction
   */
  private CarbonDataFileAttributes carbonDataFileAttributes;
  private CarbonTablePath carbonTablePath;
  /**
   * data directory location in carbon store path
   */
  private String carbonDataDirectoryPath;
  /**
   * data block size for one carbon data file
   */
  private long dataBlockSize;
  /**
   * file size at any given point
   */
  private long currentFileSize;
  /**
   * size reserved in one file for writing block meta data. It will be in percentage
   */
  private int spaceReservedForBlockMetaSize;
  private FileOutputStream fileOutputStream;

  public AbstractFactDataWriter(String storeLocation, int measureCount, int mdKeyLength,
      String tableName, IFileManagerComposite fileManager, int[] keyBlockSize,
      CarbonDataFileAttributes carbonDataFileAttributes, List<ColumnSchema> columnSchema,
      String carbonDataDirectoryPath, int[] colCardinality) {

    // measure count
    this.measureCount = measureCount;
    // table name
    this.tableName = tableName;

    this.storeLocation = storeLocation;
    this.blockletInfoList =
        new ArrayList<BlockletInfoColumnar>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    // get max file size;
    CarbonProperties propInstance = CarbonProperties.getInstance();
    this.fileSizeInBytes = Long.parseLong(propInstance
        .getProperty(CarbonCommonConstants.MAX_FILE_SIZE,
            CarbonCommonConstants.MAX_FILE_SIZE_DEFAULT_VAL))
        * CarbonCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR
        * CarbonCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR * 1L;
    this.spaceReservedForBlockMetaSize = Integer.parseInt(propInstance
        .getProperty(CarbonCommonConstants.CARBON_BLOCK_META_RESERVED_SPACE,
            CarbonCommonConstants.CARBON_BLOCK_META_RESERVED_SPACE_DEFAULT));
    this.dataBlockSize = fileSizeInBytes - (fileSizeInBytes * spaceReservedForBlockMetaSize) / 100;
    LOGGER.info("Total file size: " + fileSizeInBytes + " and dataBlock Size: " + dataBlockSize);
    this.fileManager = fileManager;
    this.carbonDataDirectoryPath = carbonDataDirectoryPath;
    this.keyBlockSize = keyBlockSize;
    this.mdkeySize = mdKeyLength;
    this.executorService = Executors.newFixedThreadPool(1);
    executorServiceSubmitList = new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    // in case of compaction we will pass the cardinality.
    this.localCardinality = colCardinality;
    this.carbonDataFileAttributes = carbonDataFileAttributes;
    CarbonTableIdentifier tableIdentifier = new CarbonTableIdentifier(databaseName, tableName);
    carbonTablePath = CarbonStorePath.getCarbonTablePath(storeLocation, tableIdentifier);
    //TODO: We should delete the levelmetadata file after reading here.
    // so only data loading flow will need to read from cardinality file.
    if (null == this.localCardinality) {
      this.localCardinality =
          CarbonMergerUtil.getCardinalityFromLevelMetadata(storeLocation, tableName);
      List<Integer> cardinalityList = new ArrayList<Integer>();
      thriftColumnSchemaList =
          getColumnSchemaListAndCardinality(cardinalityList, localCardinality, columnSchema);
      localCardinality =
          ArrayUtils.toPrimitive(cardinalityList.toArray(new Integer[cardinalityList.size()]));
    } else { // for compaction case
      List<Integer> cardinalityList = new ArrayList<Integer>();
      thriftColumnSchemaList =
          getColumnSchemaListAndCardinality(cardinalityList, localCardinality, columnSchema);
    }
  }

  /**
   * @param isNoDictionary the isNoDictionary to set
   */
  public void setIsNoDictionary(boolean[] isNoDictionary) {
    this.isNoDictionary = isNoDictionary;
  }

  /**
   * This method will be used to update the file channel with new file; new
   * file will be created once existing file reached the file size limit This
   * method will first check whether existing file size is exceeded the file
   * size limit if yes then write the leaf metadata to file then set the
   * current file size to 0 close the existing file channel get the new file
   * name and get the channel for new file
   *
   * @param blockletDataSize  data size of one block
   * @throws CarbonDataWriterException if any problem
   */
  protected void updateBlockletFileChannel(long blockletDataSize) throws CarbonDataWriterException {
    if ((currentFileSize + blockletDataSize) >= dataBlockSize && currentFileSize != 0) {
      // set the current file size to zero
      LOGGER.info("Writing data to file as max file size reached for file: " + fileName
          + " .Data block size: " + currentFileSize);
      // write meta data to end of the existing file
      writeBlockletInfoToFile(blockletInfoList, fileChannel, fileName);
      this.currentFileSize = 0;
      blockletInfoList =
          new ArrayList<BlockletInfoColumnar>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
      CarbonUtil.closeStreams(this.fileOutputStream, this.fileChannel);
      // rename carbon data file from in progress status to actual
      renameCarbonDataFile();
      executorServiceSubmitList.add(executorService
          .submit(new CopyThread(this.fileName.substring(0, this.fileName.lastIndexOf('.')))));
      // initialize the new channel
      initializeWriter();
    }
    currentFileSize += blockletDataSize;
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
    String carbonDataFileName = carbonTablePath
        .getCarbonDataFileName(fileCount, carbonDataFileAttributes.getTaskId(),
            carbonDataFileAttributes.getFactTimeStamp());
    String actualFileNameVal = carbonDataFileName + CarbonCommonConstants.FILE_INPROGRESS_STATUS;
    FileData fileData = new FileData(actualFileNameVal, this.storeLocation);
    fileManager.add(fileData);
    this.fileName = storeLocation + File.separator + carbonDataFileName
        + CarbonCommonConstants.FILE_INPROGRESS_STATUS;
    this.fileCount++;
    try {
      // open channel for new data file
      fileOutputStream = new FileOutputStream(this.fileName, true);
      this.fileChannel = fileOutputStream.getChannel();
    } catch (FileNotFoundException fileNotFoundException) {
      throw new CarbonDataWriterException("Problem while getting the FileChannel for Leaf File",
          fileNotFoundException);
    }
  }

  private int initFileCount() {
    int fileInitialCount = 0;
    File[] dataFiles = new File(storeLocation).listFiles(new FileFilter() {

      @Override public boolean accept(File pathVal) {
        if (!pathVal.isDirectory() && pathVal.getName().startsWith(tableName) && pathVal.getName()
            .contains(CarbonCommonConstants.FACT_FILE_EXT)) {
          return true;
        }
        return false;
      }
    });
    if (dataFiles != null && dataFiles.length > 0) {
      Arrays.sort(dataFiles);
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
  protected void writeBlockletInfoToFile(List<BlockletInfoColumnar> infoList, FileChannel channel,
      String filePath) throws CarbonDataWriterException {
    try {
      long currentPosition = channel.size();
      CarbonFooterWriter writer = new CarbonFooterWriter(filePath);
      FileFooter convertFileMeta = CarbonMetadataUtil
          .convertFileFooter(infoList, localCardinality.length, localCardinality,
              thriftColumnSchemaList);
      writer.writeFooter(convertFileMeta, currentPosition);
    } catch (IOException e) {
      throw new CarbonDataWriterException("Problem while writing the carbon file: ", e);
    }

  }

  protected List<org.carbondata.format.ColumnSchema> getColumnSchemaListAndCardinality(
      List<Integer> cardinality, int[] dictionaryColumnCardinality,
      List<ColumnSchema> wrapperColumnSchemaList) {
    List<org.carbondata.format.ColumnSchema> columnSchemaList =
        new ArrayList<org.carbondata.format.ColumnSchema>(
            CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    SchemaConverter schemaConverter = new ThriftWrapperSchemaConverterImpl();
    int counter = 0;
    for (int i = 0; i < wrapperColumnSchemaList.size(); i++) {
      columnSchemaList
          .add(schemaConverter.fromWrapperToExternalColumnSchema(wrapperColumnSchemaList.get(i)));
      if (CarbonUtil.hasEncoding(wrapperColumnSchemaList.get(i).getEncodingList(),
          org.carbondata.core.carbon.metadata.encoder.Encoding.DICTIONARY)) {
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
   * This method will be used to get the blocklet metadata
   *
   * @return BlockletInfo - blocklet metadata
   */
  protected BlockletInfoColumnar getBlockletInfo(NodeHolder nodeHolder, long offset) {
    // create the info object for leaf entry
    BlockletInfoColumnar infoObj = new BlockletInfoColumnar();
    // add total entry count
    infoObj.setNumberOfKeys(nodeHolder.getEntryCount());

    // add the key array length
    infoObj.setKeyLengths(nodeHolder.getKeyLengths());
    //add column min max data
    infoObj.setColumnMaxData(nodeHolder.getColumnMaxData());
    infoObj.setColumnMinData(nodeHolder.getColumnMinData());
    long[] keyOffSets = new long[nodeHolder.getKeyLengths().length];

    for (int i = 0; i < keyOffSets.length; i++) {
      keyOffSets[i] = offset;
      offset += nodeHolder.getKeyLengths()[i];
    }
    // add key offset
    infoObj.setKeyOffSets(keyOffSets);

    // add measure length
    infoObj.setMeasureLength(nodeHolder.getMeasureLenght());

    long[] msrOffset = new long[this.measureCount];

    for (int i = 0; i < this.measureCount; i++) {
      msrOffset[i] = offset;
      // now increment the offset by adding measure length to get the next
      // measure offset;
      offset += nodeHolder.getMeasureLenght()[i];
    }
    // add measure offset
    infoObj.setMeasureOffset(msrOffset);
    infoObj.setIsSortedKeyColumn(nodeHolder.getIsSortedKeyBlock());
    infoObj.setKeyBlockIndexLength(nodeHolder.getKeyBlockIndexLength());
    long[] keyBlockIndexOffsets = new long[nodeHolder.getKeyBlockIndexLength().length];
    for (int i = 0; i < keyBlockIndexOffsets.length; i++) {
      keyBlockIndexOffsets[i] = offset;
      offset += nodeHolder.getKeyBlockIndexLength()[i];
    }
    infoObj.setKeyBlockIndexOffSets(keyBlockIndexOffsets);
    // set startkey
    infoObj.setStartKey(nodeHolder.getStartKey());
    // set end key
    infoObj.setEndKey(nodeHolder.getEndKey());
    infoObj.setCompressionModel(nodeHolder.getCompressionModel());
    // return leaf metadata
    return infoObj;
  }

  /**
   * Method will be used to close the open file channel
   *
   * @throws CarbonDataWriterException
   */
  public void closeWriter() throws CarbonDataWriterException {
    CarbonUtil.closeStreams(this.fileOutputStream, this.fileChannel);
    renameCarbonDataFile();
    copyCarbonDataFileToCarbonStorePath(this.fileName.substring(0, this.fileName.lastIndexOf('.')));
    closeExecutorService();
  }

  /**
   * This method will close the executor service which is used for copying carbon
   * data files to carbon store path
   *
   * @throws CarbonDataWriterException
   */
  private void closeExecutorService() throws CarbonDataWriterException {
    executorService.shutdown();
    try {
      executorService.awaitTermination(2, TimeUnit.HOURS);
    } catch (InterruptedException e) {
      throw new CarbonDataWriterException(e.getMessage());
    }
    for (int i = 0; i < executorServiceSubmitList.size(); i++) {
      try {
        executorServiceSubmitList.get(i).get();
      } catch (InterruptedException e) {
        throw new CarbonDataWriterException(e.getMessage());
      } catch (ExecutionException e) {
        throw new CarbonDataWriterException(e.getMessage());
      }
    }
  }

  /**
   * This method will rename carbon data file from in progress status to normal
   *
   * @throws CarbonDataWriterException
   */
  private void renameCarbonDataFile() throws CarbonDataWriterException {
    File origFile = new File(this.fileName.substring(0, this.fileName.lastIndexOf('.')));
    File curFile = new File(this.fileName);
    if (!curFile.renameTo(origFile)) {
      throw new CarbonDataWriterException("Problem while renaming the file");
    }
  }

  /**
   * This method will copy the given file to carbon store location
   *
   * @param localFileName local file name with full path
   * @throws CarbonDataWriterException
   */
  private void copyCarbonDataFileToCarbonStorePath(String localFileName)
      throws CarbonDataWriterException {
    long copyStartTime = System.currentTimeMillis();
    LOGGER.info("Copying " + localFileName + " --> " + carbonDataDirectoryPath);
    try {
      CarbonFile localCarbonFile =
          FileFactory.getCarbonFile(localFileName, FileFactory.getFileType(localFileName));
      String carbonFilePath = carbonDataDirectoryPath + localFileName
          .substring(localFileName.lastIndexOf(File.separator));
      copyLocalFileToCarbonStore(carbonFilePath, localFileName,
          CarbonCommonConstants.BYTEBUFFER_SIZE,
          getMaxOfBlockAndFileSize(fileSizeInBytes, localCarbonFile.getSize()));
    } catch (IOException e) {
      throw new CarbonDataWriterException(
          "Problem while copying file from local store to carbon store");
    }
    LOGGER.info(
        "Total copy time (ms) to copy file " + localFileName + " is " + (System.currentTimeMillis()
            - copyStartTime));
  }

  /**
   * This method will read the local carbon data file and write to carbon data file in HDFS
   *
   * @param carbonStoreFilePath
   * @param localFilePath
   * @param bufferSize
   * @param blockSize
   * @throws IOException
   */
  private void copyLocalFileToCarbonStore(String carbonStoreFilePath, String localFilePath,
      int bufferSize, long blockSize) throws IOException {
    DataOutputStream dataOutputStream = null;
    DataInputStream dataInputStream = null;
    try {
      LOGGER.debug(
          "HDFS file block size for file: " + carbonStoreFilePath + " is " + blockSize + " (bytes");
      dataOutputStream = FileFactory
          .getDataOutputStream(carbonStoreFilePath, FileFactory.getFileType(carbonStoreFilePath),
              bufferSize, blockSize);
      dataInputStream = FileFactory
          .getDataInputStream(localFilePath, FileFactory.getFileType(localFilePath), bufferSize);
      IOUtils.copyBytes(dataInputStream, dataOutputStream, bufferSize);
    } finally {
      CarbonUtil.closeStreams(dataInputStream, dataOutputStream);
    }
  }

  /**
   * This method will return max of block size and file size
   *
   * @param blockSize
   * @param fileSize
   * @return
   */
  private static long getMaxOfBlockAndFileSize(long blockSize, long fileSize) {
    long maxSize = blockSize;
    if (fileSize > blockSize) {
      maxSize = fileSize;
    }
    // block size should be exactly divisible by 512 which is  maintained by HDFS as bytes
    // per checksum, dfs.bytes-per-checksum=512 must divide block size
    long remainder = maxSize % HDFS_CHECKSUM_LENGTH;
    if (remainder > 0) {
      maxSize = maxSize + HDFS_CHECKSUM_LENGTH - remainder;
    }
    return maxSize;
  }

  /**
   * Write leaf meta data to File.
   *
   * @throws CarbonDataWriterException
   */
  @Override public void writeBlockletInfoToFile() throws CarbonDataWriterException {
    writeBlockletInfoToFile(this.blockletInfoList, fileChannel, fileName);
  }

  /**
   * This method will be used to write leaf data to file
   * file format
   * <key><measure1><measure2>....
   *
   * @throws CarbonDataWriterException
   * @throws CarbonDataWriterException throws new CarbonDataWriterException if any problem
   */
  protected void writeDataToFile(NodeHolder nodeHolder) throws CarbonDataWriterException {
    // write data to file and get its offset
    long offset = writeDataToFile(nodeHolder, fileChannel);
    // get the blocklet info for currently added blocklet
    BlockletInfoColumnar blockletInfo = getBlockletInfo(nodeHolder, offset);
    // add blocklet info to list
    blockletInfoList.add(blockletInfo);
    // calculate the current size of the file
  }

  protected abstract long writeDataToFile(NodeHolder nodeHolder, FileChannel channel)
      throws CarbonDataWriterException;

  @Override public int getLeafMetadataSize() {
    return blockletInfoList.size();
  }

  @Override public String getTempStoreLocation() {
    return this.fileName;
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
      copyCarbonDataFileToCarbonStorePath(fileName);
      return null;
    }
  }
}
