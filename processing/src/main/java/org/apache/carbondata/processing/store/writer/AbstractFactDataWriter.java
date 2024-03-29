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

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.constants.CarbonLoadOptionConstants;
import org.apache.carbondata.core.datastore.exception.CarbonDataWriterException;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.converter.SchemaConverter;
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl;
import org.apache.carbondata.core.metadata.index.BlockIndexInfo;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.segmentmeta.BlockColumnMetaDataInfo;
import org.apache.carbondata.core.segmentmeta.SegmentMetaDataInfoStats;
import org.apache.carbondata.core.util.CarbonMetadataUtil;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonThreadFactory;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataLoadMetrics;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.core.writer.CarbonIndexFileWriter;
import org.apache.carbondata.format.BlockIndex;
import org.apache.carbondata.format.BlockletInfo3;
import org.apache.carbondata.format.IndexHeader;
import org.apache.carbondata.processing.index.IndexWriterListener;
import org.apache.carbondata.processing.store.CarbonFactDataHandlerModel;

import static org.apache.carbondata.core.constants.SortScopeOptions.SortScope.NO_SORT;

import org.apache.log4j.Logger;

public abstract class AbstractFactDataWriter implements CarbonFactDataWriter {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(AbstractFactDataWriter.class.getName());

  /**
   * file channel to write
   */
  protected WritableByteChannel fileChannel;
  protected long currentOffsetInFile;
  /**
   * The path of CarbonData file to write in hdfs/s3
   */
  private String carbonDataFileStorePath;
  /**
   * The temp path of carbonData file used on executor
   */
  private String carbonDataFileTempPath;

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
   * thrift column schema
   */
  protected List<org.apache.carbondata.format.ColumnSchema> thriftColumnSchemaList;
  protected CarbonFactDataHandlerModel model;
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

  protected DataOutputStream fileOutputStream;

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
   * listener to write index
   */
  protected IndexWriterListener listener;
  /**
   * Whether directly write fact data to store path
   */
  private boolean enableDirectlyWriteDataToStorePath = false;

  protected ExecutorService fallbackExecutorService;

  private DataLoadMetrics metrics;

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
    LOGGER
        .info("Total file size: " + fileSizeInBytes + " and dataBlock Size: " + blockSizeThreshold);

    // whether to directly write fact data to HDFS
    String directlyWriteData2Hdfs = propInstance
        .getProperty(CarbonLoadOptionConstants.ENABLE_CARBON_LOAD_DIRECT_WRITE_TO_STORE_PATH,
            CarbonLoadOptionConstants.ENABLE_CARBON_LOAD_DIRECT_WRITE_TO_STORE_PATH_DEFAULT);
    this.enableDirectlyWriteDataToStorePath = "TRUE".equalsIgnoreCase(directlyWriteData2Hdfs);

    if (enableDirectlyWriteDataToStorePath) {
      LOGGER.info("Carbondata will directly write fact data to store path.");
    } else {
      LOGGER.info("Carbondata will write temporary fact data to local disk.");
    }

    this.executorService = Executors.newFixedThreadPool(1,
        new CarbonThreadFactory("CompleteHDFSBackendPool:" + this.model.getTableName(),
                true));
    executorServiceSubmitList = new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    // in case of compaction we will pass the cardinality.

    thriftColumnSchemaList = getColumnSchemaListAndCardinality(this.model.getWrapperColumnSchema());
    blockletMetadata = new ArrayList<BlockletInfo3>();
    blockletIndex = new ArrayList<>();
    listener = this.model.getIndexWriterlistener();
    if (model.getColumnLocalDictGenMap().size() > 0) {
      int numberOfCores = 1;
      if (model.getNumberOfCores() > 1) {
        numberOfCores = model.getNumberOfCores() / 2;
      }
      fallbackExecutorService = Executors.newFixedThreadPool(numberOfCores, new CarbonThreadFactory(
          "FallbackPool:" + model.getTableName() + ", range: " + model.getBucketId(),
              true));
    }
    this.metrics = this.model.getMetrics();
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
      String activeFile =
          enableDirectlyWriteDataToStorePath ? carbonDataFileStorePath : carbonDataFileTempPath;
      LOGGER.info("Writing data to file as max file size reached for file: "
          + activeFile + ". Data block size: " + currentFileSize);
      // write meta data to end of the existing file
      writeFooterToFile();
      this.blockletMetadata = new ArrayList<>();
      this.blockletIndex = new ArrayList<>();
      commitCurrentFile(false);
      // initialize the new channel
      this.currentFileSize = 0;
      initializeWriter();
    }
    currentFileSize += blockletSizeToBeAdded;
  }

  private void notifyBlockStart() {
    if (listener != null) {
      try {
        listener.onBlockStart(carbonDataFileName);
      } catch (IOException e) {
        throw new CarbonDataWriterException("Problem while writing index", e);
      }
    }
  }

  private void notifyBlockEnd() {
    if (listener != null) {
      try {
        listener.onBlockEnd(carbonDataFileName);
      } catch (IOException e) {
        throw new CarbonDataWriterException("Problem while writing index", e);
      }
    }
  }

  /**
   * Finish writing current file. It will flush stream, copy and rename temp file to final file
   * @param copyInCurrentThread set to false if want to do data copy in a new thread
   */
  protected void commitCurrentFile(boolean copyInCurrentThread) {
    notifyBlockEnd();
    CarbonUtil.closeStreams(this.fileOutputStream, this.fileChannel);
    if (!enableDirectlyWriteDataToStorePath) {
      try {
        if (currentFileSize == 0) {
          handleEmptyDataFile(carbonDataFileTempPath);
        } else {
          if (copyInCurrentThread) {
            CarbonUtil.copyCarbonDataFileToCarbonStorePath(carbonDataFileTempPath,
                model.getCarbonDataDirectoryPath(), fileSizeInBytes, metrics);
            FileFactory.deleteFile(carbonDataFileTempPath);
          } else {
            executorServiceSubmitList
                .add(executorService.submit(new CompleteHdfsBackendThread(carbonDataFileTempPath)));
          }
        }
      } catch (IOException e) {
        LOGGER.error(e);
      }
    } else {
      try {
        if (currentFileSize == 0) {
          handleEmptyDataFile(carbonDataFileStorePath);
        } else {
          // In case if direct write data to store path is enabled, carbonData files will be written
          // directly to partition path. Need to add partition path and carbon data file size info
          // to load metrics
          if (model.getTableSpec().getCarbonTable().isHivePartitionTable() && model
              .getCarbonDataDirectoryPath().endsWith(".tmp") && carbonDataFileStorePath
              .endsWith(CarbonCommonConstants.FACT_FILE_EXT)) {
            if (metrics != null) {
              // get partition directory path
              String partitionPath = model.getCarbonDataDirectoryPath()
                  .substring(0, model.getCarbonDataDirectoryPath().lastIndexOf(File.separator));
              metrics.addToPartitionPath(partitionPath);
              String carbonFilePath = partitionPath + carbonDataFileStorePath
                  .substring(carbonDataFileStorePath.lastIndexOf(File.separator));
              addOutputFilesInfoToMetrics(carbonFilePath);
            }
          }
        }
      } catch (IOException e) {
        LOGGER.error(e);
      }
    }
  }

  private void handleEmptyDataFile(String filePath) throws IOException {
    FileFactory.deleteFile(filePath);
    if (blockIndexInfoList.size() > 0 && blockIndexInfoList.get(blockIndexInfoList.size() - 1)
        .getFileName().equals(carbonDataFileName)) {
      // no need add this entry in index file
      blockIndexInfoList.remove(blockIndexInfoList.size() - 1);
      // TODO: currently there is no implementation for notifyBlockEnd(),
      // hence no impact, once implementation is done. Need to undo it in this case.
    }
  }

  /**
   * This method will be used to initialize the channel
   *
   * @throws CarbonDataWriterException
   */
  @Override
  public void initializeWriter() throws CarbonDataWriterException {
    this.carbonDataFileName = CarbonTablePath
        .getCarbonDataFileName(fileCount, model.getCarbonDataFileAttributes().getTaskId(),
            model.getBucketId(), model.getTaskExtension(),
            "" + model.getCarbonDataFileAttributes().getFactTimeStamp(), model.getSegmentId(),
            model.getColumnCompressor());
    this.carbonDataFileStorePath = model.getCarbonDataDirectoryPath() + File.separator
        + carbonDataFileName;
    try {
      FileFactory.mkdirs(model.getCarbonDataDirectoryPath());
      if (enableDirectlyWriteDataToStorePath) {
        if (model.getTableSpec().getCarbonTable().isHivePartitionTable() && model
            .getCarbonDataDirectoryPath().endsWith(".tmp")) {
          // set carbonData file store path to partition path instead of tmp directory
          carbonDataFileStorePath = model.getCarbonDataDirectoryPath()
              .substring(0, model.getCarbonDataDirectoryPath().lastIndexOf(File.separator))
              + File.separator + carbonDataFileName;
        }
        // the block size will be twice the block_size specified by user to make sure that
        // one carbondata file only consists exactly one HDFS block.
        fileOutputStream = FileFactory
            .getDataOutputStream(carbonDataFileStorePath, CarbonCommonConstants.BYTEBUFFER_SIZE,
                fileSizeInBytes * 2);
      } else {
        //each time we initialize writer, we choose a local temp location randomly
        String[] tempFileLocations = model.getStoreLocation();
        String chosenTempLocation =
            tempFileLocations[new Random().nextInt(tempFileLocations.length)];
        LOGGER.info("Randomly choose factdata temp location: " + chosenTempLocation);
        carbonDataFileTempPath = chosenTempLocation + File.separator + carbonDataFileName;
        fileOutputStream = FileFactory
            .getDataOutputStream(carbonDataFileTempPath, CarbonCommonConstants.BYTEBUFFER_SIZE,
                true);
      }

      this.fileCount++;
      // open channel for new data file
      this.fileChannel = Channels.newChannel(fileOutputStream);
      this.currentOffsetInFile = 0;
    } catch (IOException ex) {
      throw new CarbonDataWriterException(
          "Problem while getting the channel for fact data file", ex);
    }
    notifyBlockStart();
  }

  /**
   * This method will write metadata at the end of file file format in thrift format
   */
  protected abstract void writeFooterToFile() throws CarbonDataWriterException;

  /**
   * Below method will be used to fill the vlock info details
   *
   * @param numberOfRows    number of rows in file
   * @param carbonDataFileName The name of carbonData file
   * @param footerOffset footer offset
   * @param fileSize
   */
  protected abstract void fillBlockIndexInfoDetails(long numberOfRows, String carbonDataFileName,
      long footerOffset, long fileSize);

  public static List<org.apache.carbondata.format.ColumnSchema> getColumnSchemaListAndCardinality(
      List<ColumnSchema> wrapperColumnSchemaList) {
    List<org.apache.carbondata.format.ColumnSchema> columnSchemaList =
        new ArrayList<org.apache.carbondata.format.ColumnSchema>(
            CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    SchemaConverter schemaConverter = new ThriftWrapperSchemaConverterImpl();
    for (int i = 0; i < wrapperColumnSchemaList.size(); i++) {
      columnSchemaList
          .add(schemaConverter.fromWrapperToExternalColumnSchema(wrapperColumnSchemaList.get(i)));
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
    if (blockIndexInfoList.size() == 0) {
      // no need to write index file, if data file is not there.
      return;
    }
    // get the header
    IndexHeader indexHeader = CarbonMetadataUtil
        .getIndexHeader(thriftColumnSchemaList, model.getBucketId(),
            model.getSchemaUpdatedTimeStamp());
    indexHeader.setIs_sort(model.getSortScope() != null && model.getSortScope() != NO_SORT);
    // get the block index info thrift
    List<BlockIndex> blockIndexThrift = CarbonMetadataUtil.getBlockIndexInfo(blockIndexInfoList);
    // get all block minmax and add to segmentMinMaxMap
    CarbonTable carbonTable = model.getTableSpec().getCarbonTable();
    if (null != model.getSegmentId() && !carbonTable.isHivePartitionTable() && !carbonTable
        .isIndexTable()) {
      for (BlockIndexInfo blockIndex : blockIndexInfoList) {
        byte[][] min = blockIndex.getBlockletIndex().getMinMaxIndex().getMinValues();
        byte[][] max = blockIndex.getBlockletIndex().getMinMaxIndex().getMaxValues();
        BlockColumnMetaDataInfo blockColumnMetaDataInfo =
            new BlockColumnMetaDataInfo(thriftColumnSchemaList, min, max);
        SegmentMetaDataInfoStats.getInstance()
            .setBlockMetaDataInfo(model.getTableName(), model.getSegmentId(),
                blockColumnMetaDataInfo, this.model.getWrapperColumnSchema());
      }
    }
    String indexFileName;
    if (enableDirectlyWriteDataToStorePath) {
      String rawFileName = model.getCarbonDataDirectoryPath() + CarbonCommonConstants.FILE_SEPARATOR
          + CarbonTablePath.getCarbonIndexFileName(model.getCarbonDataFileAttributes().getTaskId(),
          model.getBucketId(), model.getTaskExtension(),
          "" + model.getCarbonDataFileAttributes().getFactTimeStamp(), model.getSegmentId());
      indexFileName = FileFactory.getUpdatedFilePath(rawFileName);
    } else {
      // randomly choose a temp location for index file
      String[] tempLocations = model.getStoreLocation();
      String chosenTempLocation = tempLocations[new Random().nextInt(tempLocations.length)];
      LOGGER.info("Randomly choose index file location: " + chosenTempLocation);
      indexFileName = chosenTempLocation + File.separator + CarbonTablePath
          .getCarbonIndexFileName(model.getCarbonDataFileAttributes().getTaskId(),
              model.getBucketId(), model.getTaskExtension(),
              "" + model.getCarbonDataFileAttributes().getFactTimeStamp(), model.getSegmentId());
    }

    CarbonIndexFileWriter writer = new CarbonIndexFileWriter();
    // open file
    writer.openThriftWriter(indexFileName);
    // write the header first
    writer.writeThrift(indexHeader);
    // write the indexes
    for (BlockIndex blockIndex : blockIndexThrift) {
      writer.writeThrift(blockIndex);
    }
    writer.close();
    if (!enableDirectlyWriteDataToStorePath) {
      CarbonUtil
          .copyCarbonDataFileToCarbonStorePath(indexFileName, model.getCarbonDataDirectoryPath(),
              fileSizeInBytes, metrics);
      FileFactory.deleteFile(indexFileName);
    } else if (model.getTableSpec().getCarbonTable().isHivePartitionTable() && model
        .getCarbonDataDirectoryPath().endsWith(".tmp")) {
      if (metrics != null) {
        addOutputFilesInfoToMetrics(indexFileName);
      }
    }
  }

  private void addOutputFilesInfoToMetrics(String carbonFilePath) {
    // Store the number of files written by each task.
    metrics.incrementCount();
    // Store the files written by each task to metrics.
    long targetSize = FileFactory.getCarbonFile(carbonFilePath).getSize();
    metrics.addToOutputFiles(carbonFilePath + ":" + targetSize);
    metrics.addOutputBytes(targetSize);
  }

  /**
   * This method will close the executor service which is used for copying carbon
   * data files to carbon store path
   *
   * @throws CarbonDataWriterException
   */
  protected void closeExecutorService() throws CarbonDataWriterException {
    CarbonDataWriterException exception = null;
    if (listener != null) {
      try {
        listener.finish();
        listener = null;
      } catch (IOException e) {
        exception = new CarbonDataWriterException(e);
      }
    }
    try {
      executorService.shutdown();
      executorService.awaitTermination(2, TimeUnit.HOURS);
      for (int i = 0; i < executorServiceSubmitList.size(); i++) {
        executorServiceSubmitList.get(i).get();
      }
    } catch (InterruptedException | ExecutionException e) {
      if (null == exception) {
        exception = new CarbonDataWriterException(e);
      }
    }
    if (null != fallbackExecutorService) {
      fallbackExecutorService.shutdownNow();
    }
    if (exception != null) {
      throw exception;
    }
  }

  /**
   * This method will complete hdfs backend storage for this file.
   * It may copy the carbon data file from local store location to carbon store location,
   * it may also complete the remaining replications for the existing hdfs file.
   */
  private final class CompleteHdfsBackendThread implements Callable<Void> {

    /**
     * complete path along with file name which needs to be copied to
     * carbon store path
     */
    private String fileName;

    private CompleteHdfsBackendThread(String fileName) {
      this.fileName = fileName;
    }

    /**
     * Computes a result, or throws an exception if unable to do so.
     *
     * @return computed result
     * @throws Exception if unable to compute a result
     */
    @Override
    public Void call() throws Exception {
      CarbonUtil.copyCarbonDataFileToCarbonStorePath(fileName, model.getCarbonDataDirectoryPath(),
          fileSizeInBytes, metrics);
      FileFactory.deleteFile(fileName);
      return null;
    }
  }
}
