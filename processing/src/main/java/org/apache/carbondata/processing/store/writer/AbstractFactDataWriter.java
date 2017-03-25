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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
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

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.columnar.IndexStorage;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.keygenerator.mdkey.NumberCompressor;
import org.apache.carbondata.core.metadata.BlockletInfoColumnar;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.blocklet.index.BlockletBTreeIndex;
import org.apache.carbondata.core.metadata.blocklet.index.BlockletIndex;
import org.apache.carbondata.core.metadata.blocklet.index.BlockletMinMaxIndex;
import org.apache.carbondata.core.metadata.converter.SchemaConverter;
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl;
import org.apache.carbondata.core.metadata.index.BlockIndexInfo;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonMergerUtil;
import org.apache.carbondata.core.util.CarbonMetadataUtil;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.NodeHolder;
import org.apache.carbondata.core.util.path.CarbonStorePath;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.core.writer.CarbonIndexFileWriter;
import org.apache.carbondata.format.BlockIndex;
import org.apache.carbondata.format.BlockletInfo3;
import org.apache.carbondata.format.IndexHeader;
import org.apache.carbondata.processing.mdkeygen.file.FileData;
import org.apache.carbondata.processing.store.writer.exception.CarbonDataWriterException;

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
   * file channel
   */
  protected FileChannel fileChannel;
  /**
   * this will be used for holding blocklet metadata
   */
  protected List<BlockletInfoColumnar> blockletInfoList;
  protected boolean[] isNoDictionary;
  /**
   * file name
   */
  protected String fileName;

  /**
   * The name of carbonData file
   */
  protected String carbonDataFileName;

  /**
   * Local cardinality for the segment
   */
  protected int[] localCardinality;
  /**
   * thrift column schema
   */
  protected List<org.apache.carbondata.format.ColumnSchema> thriftColumnSchemaList;
  protected NumberCompressor numberCompressor;
  protected CarbonDataWriterVo dataWriterVo;
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
  private CarbonTablePath carbonTablePath;
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

  public AbstractFactDataWriter(CarbonDataWriterVo dataWriterVo) {
    this.dataWriterVo = dataWriterVo;
    this.blockletInfoList =
        new ArrayList<BlockletInfoColumnar>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    blockIndexInfoList = new ArrayList<>();
    // get max file size;
    CarbonProperties propInstance = CarbonProperties.getInstance();
    // if blocksize=2048, then 2048*1024*1024 will beyond the range of Int
    this.fileSizeInBytes =
        (long) dataWriterVo.getTableBlocksize() * CarbonCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR
            * CarbonCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR;
    this.spaceReservedForBlockMetaSize = Integer.parseInt(propInstance
        .getProperty(CarbonCommonConstants.CARBON_BLOCK_META_RESERVED_SPACE,
            CarbonCommonConstants.CARBON_BLOCK_META_RESERVED_SPACE_DEFAULT));
    this.dataBlockSize = fileSizeInBytes - (fileSizeInBytes * spaceReservedForBlockMetaSize) / 100;
    LOGGER.info("Total file size: " + fileSizeInBytes + " and dataBlock Size: " + dataBlockSize);

    this.executorService = Executors.newFixedThreadPool(1);
    executorServiceSubmitList = new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    // in case of compaction we will pass the cardinality.
    this.localCardinality = dataWriterVo.getColCardinality();
    CarbonTable carbonTable = CarbonMetadata.getInstance().getCarbonTable(
        dataWriterVo.getDatabaseName() + CarbonCommonConstants.UNDERSCORE + dataWriterVo
            .getTableName());
    carbonTablePath = CarbonStorePath.getCarbonTablePath(dataWriterVo.getStoreLocation(),
        carbonTable.getCarbonTableIdentifier());
    //TODO: We should delete the levelmetadata file after reading here.
    // so only data loading flow will need to read from cardinality file.
    if (null == this.localCardinality) {
      this.localCardinality = CarbonMergerUtil
          .getCardinalityFromLevelMetadata(dataWriterVo.getStoreLocation(),
              dataWriterVo.getTableName());
      List<Integer> cardinalityList = new ArrayList<Integer>();
      thriftColumnSchemaList = getColumnSchemaListAndCardinality(cardinalityList, localCardinality,
          dataWriterVo.getWrapperColumnSchemaList());
      localCardinality =
          ArrayUtils.toPrimitive(cardinalityList.toArray(new Integer[cardinalityList.size()]));
    } else { // for compaction case
      List<Integer> cardinalityList = new ArrayList<Integer>();
      thriftColumnSchemaList = getColumnSchemaListAndCardinality(cardinalityList, localCardinality,
          dataWriterVo.getWrapperColumnSchemaList());
    }
    this.numberCompressor = new NumberCompressor(Integer.parseInt(CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.BLOCKLET_SIZE,
            CarbonCommonConstants.BLOCKLET_SIZE_DEFAULT_VAL)));
    this.dataChunksOffsets = new ArrayList<>();
    this.dataChunksLength = new ArrayList<>();
    blockletMetadata = new ArrayList<BlockletInfo3>();
    blockletIndex = new ArrayList<>();
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
    // convert to make block size more readable.
    String readableBlockSize = ByteUtil.convertByteToReadable(blockSize);
    String readableFileSize = ByteUtil.convertByteToReadable(fileSize);
    String readableMaxSize = ByteUtil.convertByteToReadable(maxSize);
    LOGGER.info(
        "The configured block size is " + readableBlockSize + ", the actual carbon file size is "
            + readableFileSize + ", choose the max value " + readableMaxSize
            + " as the block size on HDFS");
    return maxSize;
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
   * @param blockletDataSize data size of one block
   * @throws CarbonDataWriterException if any problem
   */
  protected void updateBlockletFileChannel(long blockletDataSize) throws CarbonDataWriterException {
    if ((currentFileSize + blockletDataSize) >= dataBlockSize && currentFileSize != 0) {
      // set the current file size to zero
      LOGGER.info("Writing data to file as max file size reached for file: " + fileName
          + " .Data block size: " + currentFileSize);
      // write meta data to end of the existing file
      writeBlockletInfoToFile(fileChannel, fileName);
      this.currentFileSize = 0;
      blockletInfoList =
          new ArrayList<BlockletInfoColumnar>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
      this.dataChunksOffsets = new ArrayList<>();
      this.dataChunksLength = new ArrayList<>();
      this.blockletMetadata = new ArrayList<>();
      this.blockletIndex = new ArrayList<>();
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
    this.carbonDataFileName = carbonTablePath
        .getCarbonDataFileName(fileCount, dataWriterVo.getCarbonDataFileAttributes().getTaskId(),
            dataWriterVo.getBucketNumber(), dataWriterVo.getTaskExtension(),
            dataWriterVo.getCarbonDataFileAttributes().getFactTimeStamp());
    String actualFileNameVal = carbonDataFileName + CarbonCommonConstants.FILE_INPROGRESS_STATUS;
    FileData fileData = new FileData(actualFileNameVal, dataWriterVo.getStoreLocation());
    dataWriterVo.getFileManager().add(fileData);
    this.fileName = dataWriterVo.getStoreLocation() + File.separator + carbonDataFileName
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
    File[] dataFiles = new File(dataWriterVo.getStoreLocation()).listFiles(new FileFilter() {
      @Override public boolean accept(File pathVal) {
        if (!pathVal.isDirectory() && pathVal.getName().startsWith(dataWriterVo.getTableName())
            && pathVal.getName().contains(CarbonCommonConstants.FACT_FILE_EXT)) {
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
  protected abstract void writeBlockletInfoToFile(
      FileChannel channel, String filePath) throws CarbonDataWriterException;

  /**
   * Below method will be used to fill the vlock info details
   *
   * @param numberOfRows    number of rows in file
   * @param filePath        file path
   * @param currentPosition current offset
   */
  protected void fillBlockIndexInfoDetails(long numberOfRows,
      String filePath, long currentPosition) {

    // as min-max will change for each blocklet and second blocklet min-max can be lesser than
    // the first blocklet so we need to calculate the complete block level min-max by taking
    // the min value of each column and max value of each column
    byte[][] currentMinValue = blockletInfoList.get(0).getColumnMinData().clone();
    byte[][] currentMaxValue = blockletInfoList.get(0).getColumnMaxData().clone();
    byte[][] minValue = null;
    byte[][] maxValue = null;
    for (int i = 1; i < blockletInfoList.size(); i++) {
      minValue = blockletInfoList.get(i).getColumnMinData();
      maxValue = blockletInfoList.get(i).getColumnMaxData();
      for (int j = 0; j < maxValue.length; j++) {
        if (ByteUtil.UnsafeComparer.INSTANCE.compareTo(currentMinValue[j], minValue[j]) > 0) {
          currentMinValue[j] = minValue[j].clone();
        }
        if (ByteUtil.UnsafeComparer.INSTANCE.compareTo(currentMaxValue[j], maxValue[j]) < 0) {
          currentMaxValue[j] = maxValue[j].clone();
        }
      }
    }
    // start and end key we can take based on first blocklet
    // start key will be the block start key as
    // it is the least key and end blocklet end key will be the block end key as it is the max key
    BlockletBTreeIndex btree = new BlockletBTreeIndex(blockletInfoList.get(0).getStartKey(),
        blockletInfoList.get(blockletInfoList.size() - 1).getEndKey());
    BlockletMinMaxIndex minmax = new BlockletMinMaxIndex();
    minmax.setMinValues(currentMinValue);
    minmax.setMaxValues(currentMaxValue);
    BlockletIndex blockletIndex = new BlockletIndex(btree, minmax);
    BlockIndexInfo blockIndexInfo =
        new BlockIndexInfo(numberOfRows, filePath, currentPosition, blockletIndex);
    blockIndexInfoList.add(blockIndexInfo);
  }

  protected List<org.apache.carbondata.format.ColumnSchema> getColumnSchemaListAndCardinality(
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
   * Method will be used to close the open file channel
   *
   * @throws CarbonDataWriterException
   */
  public void closeWriter() throws CarbonDataWriterException {
    CarbonUtil.closeStreams(this.fileOutputStream, this.fileChannel);
    if (this.blockletInfoList.size() > 0) {
      renameCarbonDataFile();
      copyCarbonDataFileToCarbonStorePath(
          this.fileName.substring(0, this.fileName.lastIndexOf('.')));
      try {
        writeIndexFile();
      } catch (IOException e) {
        throw new CarbonDataWriterException("Problem while writing the index file", e);
      }
    }
    closeExecutorService();
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
        .getIndexHeader(localCardinality, thriftColumnSchemaList, dataWriterVo.getBucketNumber());
    // get the block index info thrift
    List<BlockIndex> blockIndexThrift = CarbonMetadataUtil.getBlockIndexInfo(blockIndexInfoList);
    String fileName = dataWriterVo.getStoreLocation() + File.separator + carbonTablePath
        .getCarbonIndexFileName(dataWriterVo.getCarbonDataFileAttributes().getTaskId(),
            dataWriterVo.getBucketNumber(), dataWriterVo.getTaskExtension(),
            dataWriterVo.getCarbonDataFileAttributes().getFactTimeStamp());
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
    copyCarbonDataFileToCarbonStorePath(fileName);
  }

  /**
   * This method will close the executor service which is used for copying carbon
   * data files to carbon store path
   *
   * @throws CarbonDataWriterException
   */
  protected void closeExecutorService() throws CarbonDataWriterException {
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
  protected void renameCarbonDataFile() throws CarbonDataWriterException {
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
  protected void copyCarbonDataFileToCarbonStorePath(String localFileName)
      throws CarbonDataWriterException {
    long copyStartTime = System.currentTimeMillis();
    LOGGER.info("Copying " + localFileName + " --> " + dataWriterVo.getCarbonDataDirectoryPath());
    try {
      CarbonFile localCarbonFile =
          FileFactory.getCarbonFile(localFileName, FileFactory.getFileType(localFileName));
      String carbonFilePath = dataWriterVo.getCarbonDataDirectoryPath() + localFileName
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
      CarbonUtil.closeStream(dataInputStream);
      CarbonUtil.closeStream(dataOutputStream);
    }
  }

  /**
   * Write leaf meta data to File.
   *
   * @throws CarbonDataWriterException
   */
  @Override public void writeBlockletInfoToFile() throws CarbonDataWriterException {
    if (this.blockletInfoList.size() > 0) {
      writeBlockletInfoToFile(fileChannel, fileName);
    }
  }

  /**
   * This method will be used to write leaf data to file
   * file format
   * <key><measure1><measure2>....
   *
   * @throws CarbonDataWriterException
   * @throws CarbonDataWriterException throws new CarbonDataWriterException if any problem
   */
  public abstract void writeBlockletData(NodeHolder nodeHolder) throws CarbonDataWriterException;

  protected byte[][] fillAndCompressedKeyBlockData(IndexStorage[] keyStorageArray,
      int entryCount) {
    byte[][] keyBlockData = new byte[keyStorageArray.length][];
    int destPos = 0;
    int keyBlockSizePosition = -1;
    for (int i = 0; i < keyStorageArray.length; i++) {
      destPos = 0;
      //handling for high card dims
      if (!dataWriterVo.getIsComplexType()[i] && !dataWriterVo.getIsDictionaryColumn()[i]) {
        int totalLength = 0;
        // calc size of the total bytes in all the colmns.
        for (int k = 0; k < keyStorageArray[i].getKeyBlock().length; k++) {
          byte[] colValue = keyStorageArray[i].getKeyBlock()[k];
          totalLength += colValue.length;
        }
        keyBlockData[i] = new byte[totalLength];

        for (int j = 0; j < keyStorageArray[i].getKeyBlock().length; j++) {
          int length = keyStorageArray[i].getKeyBlock()[j].length;
          System
              .arraycopy(keyStorageArray[i].getKeyBlock()[j], 0, keyBlockData[i], destPos, length);
          destPos += length;
        }
      } else {
        keyBlockSizePosition++;
        if (dataWriterVo.getAggBlocks()[i]) {
          keyBlockData[i] = new byte[keyStorageArray[i].getTotalSize()];
          for (int j = 0; j < keyStorageArray[i].getKeyBlock().length; j++) {
            System.arraycopy(keyStorageArray[i].getKeyBlock()[j], 0, keyBlockData[i], destPos,
                keyStorageArray[i].getKeyBlock()[j].length);
            destPos += keyStorageArray[i].getKeyBlock()[j].length;
          }
        } else {
          if (dataWriterVo.getIsComplexType()[i]) {
            keyBlockData[i] = new byte[keyStorageArray[i].getKeyBlock().length * dataWriterVo
                .getKeyBlockSize()[keyBlockSizePosition]];
          } else {
            keyBlockData[i] =
                new byte[entryCount * dataWriterVo.getKeyBlockSize()[keyBlockSizePosition]];
          }
          for (int j = 0; j < keyStorageArray[i].getKeyBlock().length; j++) {
            System.arraycopy(keyStorageArray[i].getKeyBlock()[j], 0, keyBlockData[i], destPos,
                dataWriterVo.getKeyBlockSize()[keyBlockSizePosition]);
            destPos += dataWriterVo.getKeyBlockSize()[keyBlockSizePosition];
          }
        }
      }
      keyBlockData[i] = CompressorFactory.getInstance().getCompressor()
          .compressByte(keyBlockData[i]);
    }
    return keyBlockData;
  }

  /**
   * Below method will be used to update the min or max value
   * by removing the length from it
   *
   * @return min max value without length
   */
  protected byte[] updateMinMaxForNoDictionary(byte[] valueWithLength) {
    ByteBuffer buffer = ByteBuffer.wrap(valueWithLength);
    byte[] actualValue = new byte[buffer.getShort()];
    buffer.get(actualValue);
    return actualValue;
  }

  /**
   * Below method will be used to update the no dictionary start and end key
   *
   * @param key key to be updated
   * @return return no dictionary key
   */
  protected byte[] updateNoDictionaryStartAndEndKey(byte[] key) {
    if (key.length == 0) {
      return key;
    }
    // add key to byte buffer remove the length part of the data
    ByteBuffer buffer = ByteBuffer.wrap(key, 2, key.length - 2);
    // create a output buffer without length
    ByteBuffer output = ByteBuffer.allocate(key.length - 2);
    short numberOfByteToStorLength = 2;
    // as length part is removed, so each no dictionary value index
    // needs to be reshuffled by 2 bytes
    for (int i = 0; i < dataWriterVo.getNoDictionaryCount(); i++) {
      output.putShort((short) (buffer.getShort() - numberOfByteToStorLength));
    }
    // copy the data part
    while (buffer.hasRemaining()) {
      output.put(buffer.get());
    }
    output.rewind();
    return output.array();
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
