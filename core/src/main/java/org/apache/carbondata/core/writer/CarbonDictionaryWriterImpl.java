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

package org.apache.carbondata.core.writer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.reader.CarbonDictionaryColumnMetaChunk;
import org.apache.carbondata.core.reader.CarbonDictionaryMetadataReader;
import org.apache.carbondata.core.reader.CarbonDictionaryMetadataReaderImpl;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.HDFSLeaseUtils;
import org.apache.carbondata.format.ColumnDictionaryChunk;
import org.apache.carbondata.format.ColumnDictionaryChunkMeta;

import org.apache.thrift.TBase;

/**
 * This class is responsible for writing the dictionary file and its metadata
 */
public class CarbonDictionaryWriterImpl implements CarbonDictionaryWriter {

  /**
   * LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonDictionaryWriterImpl.class.getName());

  /**
   * list which will hold values upto maximum of one dictionary chunk size
   */
  private List<ByteBuffer> oneDictionaryChunkList;

  /**
   * Meta object which will hold last segment entry details
   */
  private CarbonDictionaryColumnMetaChunk chunkMetaObjectForLastSegmentEntry;

  /**
   * dictionary file and meta thrift writer
   */
  private ThriftWriter dictionaryThriftWriter;

  /**
   * column identifier
   */
  protected DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier;

  /**
   * dictionary file path
   */
  protected String dictionaryFilePath;

  /**
   * dictionary metadata file path
   */
  protected String dictionaryMetaFilePath;

  /**
   * start offset of dictionary chunk  for a segment
   */
  private long chunk_start_offset;

  /**
   * end offset of a dictionary chunk for a segment
   */
  private long chunk_end_offset;

  /**
   * total dictionary value record count for one segment
   */
  private int totalRecordCount;

  /**
   * total thrift object chunk count written for one segment
   */
  private int chunk_count;

  /**
   * chunk size for a dictionary file after which data will be written to disk
   */
  private int dictionary_one_chunk_size;

  /**
   * flag to check whether write method is called for first time
   */
  private boolean isFirstTime;

  private static final Charset defaultCharset = Charset.forName(
      CarbonCommonConstants.DEFAULT_CHARSET);

  /**
   * Constructor
   *
   * @param dictionaryColumnUniqueIdentifier column unique identifier
   */
  public CarbonDictionaryWriterImpl(
      DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier) {
    this.dictionaryColumnUniqueIdentifier = dictionaryColumnUniqueIdentifier;
    this.isFirstTime = true;
  }

  /**
   * This method will write the data in thrift format to disk. This method will be guided by
   * parameter dictionary_one_chunk_size and data will be divided into chunks
   * based on this parameter
   *
   * @param value unique dictionary value
   * @throws IOException if an I/O error occurs
   */
  @Override public void write(String value) throws IOException {
    write(value.getBytes(defaultCharset));
  }

  /**
   * This method will write the data in thrift format to disk. This method will be guided by
   * parameter dictionary_one_chunk_size and data will be divided into chunks
   * based on this parameter
   *
   * @param value unique dictionary value
   * @throws IOException if an I/O error occurs
   */
  private void write(byte[] value) throws IOException {
    if (isFirstTime) {
      init();
      isFirstTime = false;
    }

    if (value.length > CarbonCommonConstants.MAX_CHARS_PER_COLUMN_DEFAULT) {
      throw new IOException("Dataload failed, String size cannot exceed "
          + CarbonCommonConstants.MAX_CHARS_PER_COLUMN_DEFAULT + " bytes");
    }
    // if one chunk size is equal to list size then write the data to file
    checkAndWriteDictionaryChunkToFile();
    oneDictionaryChunkList.add(ByteBuffer.wrap(value));
    totalRecordCount++;
  }

  /**
   * This method will write the data in thrift format to disk. This method will not be guided by
   * parameter dictionary_one_chunk_size and complete data will be written as one chunk
   *
   * @param valueList list of byte array. Each byte array is unique dictionary value
   * @throws IOException if an I/O error occurs
   */
  @Override public void write(List<byte[]> valueList) throws IOException {
    if (isFirstTime) {
      init();
      isFirstTime = false;
    }
    for (byte[] value : valueList) {
      oneDictionaryChunkList.add(ByteBuffer.wrap(value));
      totalRecordCount++;
    }
  }

  /**
   * write dictionary metadata file and close thrift object
   *
   * @throws IOException if an I/O error occurs
   */
  @Override public void close() throws IOException {
    if (null != dictionaryThriftWriter && dictionaryThriftWriter.isOpen()) {
      try {
        // if stream is open then only need to write dictionary file.
        writeDictionaryFile();
      } finally {
        // close the thrift writer for dictionary file
        closeThriftWriter();
      }
    }
  }

  /**
   * check if the threshold has been reached for the number of
   * values that can kept in memory and then flush the data to file
   *
   * @throws IOException if an I/O error occurs
   */
  private void checkAndWriteDictionaryChunkToFile() throws IOException {
    if (oneDictionaryChunkList.size() >= dictionary_one_chunk_size) {
      writeDictionaryFile();
      createChunkList();
    }
  }

  /**
   * This method will serialize the object of dictionary file
   *
   * @throws IOException if an I/O error occurs
   */
  private void writeDictionaryFile() throws IOException {
    ColumnDictionaryChunk columnDictionaryChunk = new ColumnDictionaryChunk();
    columnDictionaryChunk.setValues(oneDictionaryChunkList);
    writeThriftObject(columnDictionaryChunk);
  }

  /**
   * This method will check and created the directory path where dictionary file has to be created
   *
   * @throws IOException if an I/O error occurs
   */
  private void init() throws IOException {
    initDictionaryChunkSize();
    initPaths();
    boolean dictFileExists = CarbonUtil.isFileExists(this.dictionaryFilePath);
    if (dictFileExists && CarbonUtil.isFileExists(this.dictionaryMetaFilePath)) {
      this.chunk_start_offset = CarbonUtil.getFileSize(this.dictionaryFilePath);
      validateDictionaryFileOffsetWithLastSegmentEntryOffset();
    } else if (dictFileExists) {
      FileFactory.getCarbonFile(dictionaryFilePath, FileFactory.getFileType(dictionaryFilePath))
          .delete();
    }
    openThriftWriter(this.dictionaryFilePath);
    createChunkList();
  }

  protected void initPaths() {
    this.dictionaryFilePath = dictionaryColumnUniqueIdentifier.getDictionaryFilePath();
    this.dictionaryMetaFilePath = dictionaryColumnUniqueIdentifier.getDictionaryMetaFilePath();
  }

  /**
   * initialize the value of dictionary chunk that can be kept in memory at a time
   */
  private void initDictionaryChunkSize() {
    dictionary_one_chunk_size = CarbonUtil.getDictionaryChunkSize();
  }

  /**
   * initialise one dictionary size chunk list and increment chunk count
   */
  private void createChunkList() {
    this.oneDictionaryChunkList = new ArrayList<ByteBuffer>(dictionary_one_chunk_size);
    chunk_count++;
  }

  /**
   * if file already exists then read metadata file and
   * validate the last entry end offset with file size. If
   * they are not equal that means some invalid data is present which needs
   * to be truncated
   *
   * @throws IOException if an I/O error occurs
   */
  private void validateDictionaryFileOffsetWithLastSegmentEntryOffset() throws IOException {
    // read last dictionary chunk meta entry from dictionary metadata file
    chunkMetaObjectForLastSegmentEntry = getChunkMetaObjectForLastSegmentEntry();
    int bytesToTruncate = 0;
    if (null != chunkMetaObjectForLastSegmentEntry) {
      bytesToTruncate =
          (int) (chunk_start_offset - chunkMetaObjectForLastSegmentEntry.getEnd_offset());
    }
    if (bytesToTruncate > 0) {
      LOGGER.info("some inconsistency in dictionary file for column "
          + this.dictionaryColumnUniqueIdentifier.getColumnIdentifier());
      // truncate the dictionary data till chunk meta end offset
      FileFactory.FileType fileType = FileFactory.getFileType(this.dictionaryFilePath);
      CarbonFile carbonFile = FileFactory.getCarbonFile(this.dictionaryFilePath, fileType);
      boolean truncateSuccess = carbonFile
          .truncate(this.dictionaryFilePath, chunkMetaObjectForLastSegmentEntry.getEnd_offset());
      if (!truncateSuccess) {
        LOGGER.info("Diction file not truncated successfully for column "
            + this.dictionaryColumnUniqueIdentifier.getColumnIdentifier());
      }
    }
  }

  /**
   * This method will write the dictionary metadata file for a given column
   *
   * @throws IOException if an I/O error occurs
   */
  private void writeDictionaryMetadataFile() throws IOException {
    // Format of dictionary metadata file
    // min, max, start offset, end offset and chunk count
    int min_surrogate_key = 0;
    int max_surrogate_key = 0;
    // case 1: first time dictionary writing
    // previousMax = 0, totalRecordCount = 5, min = 1, max= 5
    // case2: file already exists
    // previousMax = 5, totalRecordCount = 10, min = 6, max = 15
    // case 3: no unique values, total records 0
    // previousMax = 15, totalRecordCount = 0, min = 15, max = 15
    // both min and max equal to previous max
    if (null != chunkMetaObjectForLastSegmentEntry) {
      if (0 == totalRecordCount) {
        min_surrogate_key = chunkMetaObjectForLastSegmentEntry.getMax_surrogate_key();
      } else {
        min_surrogate_key = chunkMetaObjectForLastSegmentEntry.getMax_surrogate_key() + 1;
      }
      max_surrogate_key =
          chunkMetaObjectForLastSegmentEntry.getMax_surrogate_key() + totalRecordCount;
    } else {
      if (totalRecordCount > 0) {
        min_surrogate_key = 1;
      }
      max_surrogate_key = totalRecordCount;
    }
    ColumnDictionaryChunkMeta dictionaryChunkMeta =
        new ColumnDictionaryChunkMeta(min_surrogate_key, max_surrogate_key, chunk_start_offset,
            chunk_end_offset, chunk_count);
    try {
      openThriftWriter(this.dictionaryMetaFilePath);
      // write dictionary metadata file
      writeThriftObject(dictionaryChunkMeta);
      LOGGER.info("Dictionary metadata file written successfully for column "
          + this.dictionaryColumnUniqueIdentifier.getColumnIdentifier() + " at path "
          + this.dictionaryMetaFilePath);
    } finally {
      closeThriftWriter();
    }
  }

  /**
   * open thrift writer for writing dictionary chunk/meta object
   *
   * @param dictionaryFile can be dictionary file name or dictionary metadata file name
   * @throws IOException if an I/O error occurs
   */
  private void openThriftWriter(String dictionaryFile) throws IOException {
    // create thrift writer instance
    dictionaryThriftWriter = new ThriftWriter(dictionaryFile, true);
    // open the file stream
    try {
      dictionaryThriftWriter.open();
    } catch (IOException e) {
      // Cases to handle
      // 1. Handle File lease recovery
      if (HDFSLeaseUtils.checkExceptionMessageForLeaseRecovery(e.getMessage())) {
        LOGGER.error(e, "Lease recovery exception encountered for file: " + dictionaryFile);
        boolean leaseRecovered = HDFSLeaseUtils.recoverFileLease(dictionaryFile);
        if (leaseRecovered) {
          // try to open output stream again after recovering the lease on file
          dictionaryThriftWriter.open();
        } else {
          throw e;
        }
      } else {
        throw e;
      }
    }
  }

  /**
   * This method will write the thrift object to a file
   *
   * @param dictionaryThriftObject can be dictionary thrift object or dictionary metadata
   *                               thrift object
   * @throws IOException if an I/O error occurs
   */
  private void writeThriftObject(TBase dictionaryThriftObject) throws IOException {
    dictionaryThriftWriter.write(dictionaryThriftObject);
  }

  /**
   * close dictionary thrift writer
   */
  private void closeThriftWriter() throws IOException {
    if (null != dictionaryThriftWriter) {
      dictionaryThriftWriter.close();
    }
  }

  /**
   * This method will read the dictionary chunk metadata thrift object for last entry
   *
   * @return last entry of dictionary meta chunk
   * @throws IOException if an I/O error occurs
   */
  private CarbonDictionaryColumnMetaChunk getChunkMetaObjectForLastSegmentEntry()
      throws IOException {
    CarbonDictionaryColumnMetaChunk carbonDictionaryColumnMetaChunk = null;
    CarbonDictionaryMetadataReader columnMetadataReaderImpl = getDictionaryMetadataReader();
    try {
      // read the last segment entry for dictionary metadata
      carbonDictionaryColumnMetaChunk =
          columnMetadataReaderImpl.readLastEntryOfDictionaryMetaChunk();
    } finally {
      // Close metadata reader
      columnMetadataReaderImpl.close();
    }
    return carbonDictionaryColumnMetaChunk;
  }

  /**
   * @return
   */
  protected CarbonDictionaryMetadataReader getDictionaryMetadataReader() {
    return new CarbonDictionaryMetadataReaderImpl(dictionaryColumnUniqueIdentifier);
  }

  @Override public void commit() throws IOException {
    if (null != dictionaryThriftWriter && dictionaryThriftWriter.isOpen()) {
      this.chunk_end_offset = CarbonUtil.getFileSize(this.dictionaryFilePath);
      writeDictionaryMetadataFile();
    }
  }
}
