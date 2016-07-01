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

package org.carbondata.core.writer;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.carbondata.core.carbon.CarbonTableIdentifier;
import org.carbondata.core.carbon.ColumnIdentifier;
import org.carbondata.core.carbon.path.CarbonStorePath;
import org.carbondata.core.carbon.path.CarbonTablePath;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.reader.CarbonDictionaryColumnMetaChunk;
import org.carbondata.core.reader.CarbonDictionaryMetadataReaderImpl;
import org.carbondata.core.reader.CarbonDictionaryReaderImpl;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.format.ColumnDictionaryChunkMeta;

import mockit.Mock;
import mockit.MockUp;

import org.apache.thrift.TBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * This class will test the functionality writing and
 * reading a dictionary and its corresponding metadata file
 */
public class CarbonDictionaryWriterImplTest {

  private static final String PROPERTY_FILE_NAME = "carbonTest.properties";

  private CarbonTableIdentifier carbonTableIdentifier;

  private String databaseName;

  private String tableName;

  private String carbonStorePath;

  private ColumnIdentifier columnIdentifier;

  private Properties props;

  /**
   * dictionary file path
   */
  private String dictionaryFilePath;

  /**
   * dictionary metadata file path
   */
  private String dictionaryMetaFilePath;

  private List<String> dataSet1;

  private List<String> dataSet2;

  private List<String> dataSet3;

  @Before public void setUp() throws Exception {
    init();
    this.databaseName = props.getProperty("database", "testSchema");
    this.tableName = props.getProperty("tableName", "carbon");
    this.carbonStorePath = props.getProperty("storePath", "carbonStore");
    this.columnIdentifier = new ColumnIdentifier("Name", null, null);
    carbonTableIdentifier = new CarbonTableIdentifier(databaseName, tableName, UUID.randomUUID().toString());
    deleteStorePath();
    prepareDataSet();
  }

  @After public void tearDown() throws Exception {
    carbonTableIdentifier = null;
    deleteStorePath();
  }

  /**
   * prepare the dataset required for running test cases
   */
  private void prepareDataSet() {
    dataSet1 = Arrays.asList(new String[] { "a", "b" });
    dataSet2 = Arrays.asList(new String[] { "c", "d" });
    dataSet3 = Arrays.asList(new String[] { "e", "f" });
  }

  /**
   * test writers write functionality for a column specific
   * to a table in a database
   */
  @Test public void testWriteForNormalColumn() throws IOException {
    // second parameter is chunk count which is for the number of
    // thrift objects written for a segment
    processColumnValuesForOneChunk(1);
  }

  /**
   * test writers write functionality for a column shared across tables
   * in a database
   */
  @Test public void testWriteForSharedColumn() throws IOException {
    // second parameter is chunk count which is for the number of
    // thrift objects written for a segment
    processColumnValuesForOneChunk(1);
  }

  /**
   * test writing multiple dictionary chunks for a single segment
   */
  @Test public void testWriteMultipleChunksForOneSegment() throws IOException {
    deleteStorePath();
    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.DICTIONARY_ONE_CHUNK_SIZE, "1");
    // prepare dictionary writer object
    CarbonDictionaryWriterImpl writer = prepareWriter();
    writeDictionaryFile(writer, dataSet1);
    // record file size from where data has to be read
    long end_offset = CarbonUtil.getFileSize(this.dictionaryFilePath);
    // read metadata chunks from file
    List<CarbonDictionaryColumnMetaChunk> carbonDictionaryColumnMetaChunks =
        readDictionaryMetadataFile();
    assertTrue(1 == carbonDictionaryColumnMetaChunks.size());
    // prepare retrieved chunk metadata
    long start_offset = 0L;
    CarbonDictionaryColumnMetaChunk expected =
        new CarbonDictionaryColumnMetaChunk(1, dataSet1.size(), start_offset, end_offset,
            dataSet1.size());
    // validate chunk metadata - actual and expected
    for (CarbonDictionaryColumnMetaChunk chunk : carbonDictionaryColumnMetaChunks) {
      validateDictionaryMetadata(chunk, expected);
    }
    //assert for chunk count
    List<byte[]> dictionaryValues = readDictionaryFile(0L, 0L);
    // prepare expected dictionary chunk list
    List<String> actual = convertByteArrayListToStringValueList(dictionaryValues);
    assertTrue(dataSet1.size() == actual.size());
    // validate the dictionary data
    compareDictionaryData(actual, dataSet1);
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.DICTIONARY_ONE_CHUNK_SIZE,
        CarbonCommonConstants.DICTIONARY_ONE_CHUNK_SIZE_DEFAULT);
  }

  /**
   * prepare the dictionary writer object
   */
  private CarbonDictionaryWriterImpl prepareWriter() throws IOException {
    initDictionaryDirPaths();
    return new CarbonDictionaryWriterImpl(this.carbonStorePath, carbonTableIdentifier,
        columnIdentifier);
  }

  /**
   * this method will test the write method in case of any exception
   */
  @Test public void testWriteThrowException() throws IOException {
    final String failureMessage = "write operation failed";
    // mock write method of writer and throw exception
    new MockUp<CarbonDictionaryWriterImpl>() {
      @Mock public void write(String value) throws IOException {
        throw new IOException(failureMessage);
      }
    };
    // prepare the writer
    CarbonDictionaryWriterImpl writer = prepareWriter();
    try {
      for (String value : dataSet1) {
        // exception should be thrown when write method is called
        writer.write(value);
      }
    } catch (IOException e) {
      assertTrue(failureMessage.equals(e.getMessage()));
    } finally {
      writer.close();
    }
  }

  /**
   * This method will test the truncate functionality
   */
  @Test public void testTruncateOperation() throws IOException {
    // delete store path
    deleteStorePath();
    // prepare first dictionary chunk
    // prepare dictionary writer object
    CarbonDictionaryWriterImpl writer = prepareWriter();
    writeDictionaryFile(writer, dataSet1);
    long endOffsetAfterFirstDictionaryChunk = CarbonUtil.getFileSize(dictionaryFilePath);
    // maintain the offset till end offset of first chunk
    writer = prepareWriter();
    writeDictionaryFile(writer, dataSet2);
    // prepare first column meta chunk object
    ColumnDictionaryChunkMeta firstDictionaryChunkMeta =
        new ColumnDictionaryChunkMeta(1, 2, 0, endOffsetAfterFirstDictionaryChunk, 1);
    // overwrite the dictionary meta chunk file to test the truncate operation
    overwriteDictionaryMetaFile(firstDictionaryChunkMeta, dictionaryMetaFilePath);
    writer = prepareWriter();
    // in the next step truncate operation will be tested while writing dictionary file
    writeDictionaryFile(writer, dataSet3);
    // read dictionary file
    List<byte[]> dictionaryValues = readDictionaryFile(0L, 0L);
    List<String> actual = convertByteArrayListToStringValueList(dictionaryValues);
    List<String> expected = new ArrayList<>(4);
    expected.addAll(dataSet1);
    expected.addAll(dataSet3);
    // validate the data retrieved and it should match dataset1
    compareDictionaryData(actual, expected);
  }

  /**
   * This method will overwrite a given file with data provided
   */
  private void overwriteDictionaryMetaFile(ColumnDictionaryChunkMeta firstDictionaryChunkMeta,
      String dictionaryFile) throws IOException {
    ThriftWriter thriftMetaChunkWriter = new ThriftWriter(dictionaryFile, false);
    thriftMetaChunkWriter.open();
    thriftMetaChunkWriter.write(firstDictionaryChunkMeta);
    thriftMetaChunkWriter.close();
  }

  /**
   * this method will test the reading of dictionary file from a given offset
   */
  @Test public void testReadingOfDictionaryChunkFromAnOffset() throws Exception {
    // delete store path
    deleteStorePath();
    // prepare the writer to write dataset1
    CarbonDictionaryWriterImpl writer = prepareWriter();
    // write dataset1 data
    writeDictionaryFile(writer, dataSet1);
    // prepare the writer to write dataset2
    writer = prepareWriter();
    // write dataset2
    writeDictionaryFile(writer, dataSet2);
    // record the offset from where data has to be read
    long dictionaryFileOffsetToRead = CarbonUtil.getFileSize(this.dictionaryFilePath);
    // prepare writer to write dataset3
    writer = prepareWriter();
    // write dataset 3
    writeDictionaryFile(writer, dataSet3);
    // read dictionary chunk from dictionary file
    List<byte[]> dictionaryData = readDictionaryFile(dictionaryFileOffsetToRead, 0L);
    // prepare the retrieved data
    List<String> actual = convertByteArrayListToStringValueList(dictionaryData);
    // compare dictionary data set
    compareDictionaryData(actual, dataSet3);
    // read chunk metadata file
    List<CarbonDictionaryColumnMetaChunk> carbonDictionaryColumnMetaChunks =
        readDictionaryMetadataFile();
    // assert for metadata chunk size
    assertTrue(3 == carbonDictionaryColumnMetaChunks.size());
  }

  /**
   * this method will test the reading of dictionary file between start and end offset
   */
  @Test public void testReadingOfDictionaryChunkBetweenStartAndEndOffset() throws Exception {
    // delete store path
    deleteStorePath();
    // prepare the writer to write dataset1
    CarbonDictionaryWriterImpl writer = prepareWriter();
    // write dataset1 data
    writeDictionaryFile(writer, dataSet1);
    // record dictionary file start offset
    long dictionaryStartOffset = CarbonUtil.getFileSize(this.dictionaryFilePath);
    // prepare the writer to write dataset2
    writer = prepareWriter();
    // write dataset2
    writeDictionaryFile(writer, dataSet2);
    // record the end offset for dictionary file
    long dictionaryFileEndOffset = CarbonUtil.getFileSize(this.dictionaryFilePath);
    // prepare writer to write dataset3
    writer = prepareWriter();
    // write dataset 3
    writeDictionaryFile(writer, dataSet3);
    // read dictionary chunk from dictionary file
    List<byte[]> dictionaryData =
        readDictionaryFile(dictionaryStartOffset, dictionaryFileEndOffset);
    // prepare the retrieved data
    List<String> actual = convertByteArrayListToStringValueList(dictionaryData);
    // compare dictionary data set
    compareDictionaryData(actual, dataSet2);
    // read chunk metadata file
    List<CarbonDictionaryColumnMetaChunk> carbonDictionaryColumnMetaChunks =
        readDictionaryMetadataFile();
    // assert for metadata chunk size
    assertTrue(3 == carbonDictionaryColumnMetaChunks.size());
    CarbonDictionaryColumnMetaChunk expected =
        new CarbonDictionaryColumnMetaChunk(3, 4, dictionaryStartOffset, dictionaryFileEndOffset,
            1);
    validateDictionaryMetadata(carbonDictionaryColumnMetaChunks.get(1), expected);
  }

  /**
   * This method will convert list of byte array to list of string
   */
  private List<String> convertByteArrayListToStringValueList(List<byte[]> dictionaryByteArrayList) {
    List<String> valueList = new ArrayList<>(dictionaryByteArrayList.size());
    for (byte[] value : dictionaryByteArrayList) {
      valueList.add(new String(value, Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET)));
    }
    return valueList;
  }

  /**
   * this method will write the data into a file
   */
  private void writeDictionaryFile(CarbonDictionaryWriterImpl writer, List<String> list)
      throws IOException {
    try {
      for (String value : list) {
        writer.write(value);
      }
    } finally {
      writer.close();
    }
  }

  /**
   * this method will test the functionality of writing and reading one dictionary chunk
   */
  private void processColumnValuesForOneChunk(int chunkCountForSegment) throws IOException {
    // delete store path
    deleteStorePath();
    // prepare writer
    CarbonDictionaryWriterImpl writer = prepareWriter();
    // write the data into file
    // test write api for passing list of byte array
    writer.write(convertStringListToByteArray(dataSet1));
    // close the writer
    writer.close();
    // record end offset of file
    long end_offset = CarbonUtil.getFileSize(this.dictionaryFilePath);
    // read dictionary chunk from dictionary file
    List<byte[]> dictionaryData = readDictionaryFile(0L, 0L);
    // prepare the retrieved data
    List<String> actual = convertByteArrayListToStringValueList(dictionaryData);
    // compare the expected and actual data
    compareDictionaryData(actual, dataSet1);
    // read dictionary metadata chunks
    List<CarbonDictionaryColumnMetaChunk> carbonDictionaryColumnMetaChunks =
        readDictionaryMetadataFile();
    // assert
    assertTrue(1 == carbonDictionaryColumnMetaChunks.size());
    long start_offset = 0L;
    // validate actual chunk metadata with expected
    CarbonDictionaryColumnMetaChunk expected =
        new CarbonDictionaryColumnMetaChunk(1, 2, start_offset, end_offset, 1);
    for (CarbonDictionaryColumnMetaChunk chunk : carbonDictionaryColumnMetaChunks) {
      validateDictionaryMetadata(chunk, expected);
    }
  }

  /**
   * this method will convert list of string to list of byte array
   */
  private List<byte[]> convertStringListToByteArray(List<String> valueList) {
    List<byte[]> byteArrayList = new ArrayList<>(valueList.size());
    for (String value : valueList) {
      byteArrayList.add(value.getBytes(Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET)));
    }
    return byteArrayList;
  }

  /**
   * this method will validate the dictionary chunk metadata
   */
  private void validateDictionaryMetadata(CarbonDictionaryColumnMetaChunk actual,
      CarbonDictionaryColumnMetaChunk expected) {
    assertTrue(expected.getMin_surrogate_key() == actual.getMin_surrogate_key());
    assertTrue(expected.getMax_surrogate_key() == actual.getMax_surrogate_key());
    assertTrue(expected.getStart_offset() == actual.getStart_offset());
    assertTrue(expected.getEnd_offset() == actual.getEnd_offset());
    assertTrue(expected.getChunk_count() == actual.getChunk_count());
  }

  /**
   * this method will validate the dictionary data
   */
  private void compareDictionaryData(List<String> actual, List<String> expected) {
    assertTrue(expected.size() == actual.size());
    for (int i = 0; i < actual.size(); i++) {
      assertTrue(actual.get(i).equals(expected.get(i)));
    }
  }

  /**
   * This method will read dictionary metadata file and return the dictionary meta chunks
   *
   * @return list of dictionary metadata chunks
   * @throws IOException read and close method throws IO excpetion
   */
  private List<CarbonDictionaryColumnMetaChunk> readDictionaryMetadataFile() throws IOException {
    CarbonDictionaryMetadataReaderImpl columnMetadataReaderImpl =
        new CarbonDictionaryMetadataReaderImpl(this.carbonStorePath, this.carbonTableIdentifier,
            this.columnIdentifier);
    List<CarbonDictionaryColumnMetaChunk> dictionaryMetaChunkList = null;
    // read metadata file
    try {
      dictionaryMetaChunkList = columnMetadataReaderImpl.read();
    } finally {
      // close the metadata reader
      columnMetadataReaderImpl.close();
    }
    return dictionaryMetaChunkList;
  }

  /**
   * This method will be used to read the dictionary file from a given offset
   */
  private List<byte[]> readDictionaryFile(long dictionaryStartOffset, long dictionaryEndOffset)
      throws IOException {
    CarbonDictionaryReaderImpl dictionaryReader =
        new CarbonDictionaryReaderImpl(this.carbonStorePath, this.carbonTableIdentifier,
            this.columnIdentifier);
    List<byte[]> dictionaryValues = new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    try {
      if (0 == dictionaryEndOffset) {
        dictionaryValues = dictionaryReader.read(dictionaryStartOffset);
      } else {
        dictionaryValues = dictionaryReader.read(dictionaryStartOffset, dictionaryEndOffset);
      }
    } finally {
      dictionaryReader.close();
    }
    return dictionaryValues;
  }

  /**
   * this method will delete the store path
   */
  private void deleteStorePath() {
    FileFactory.FileType fileType = FileFactory.getFileType(this.carbonStorePath);
    CarbonFile carbonFile = FileFactory.getCarbonFile(this.carbonStorePath, fileType);
    deleteRecursiveSilent(carbonFile);
  }

  /**
   * this method will delete the folders recursively
   */
  private static void deleteRecursiveSilent(CarbonFile f) {
    if (f.isDirectory()) {
      if (f.listFiles() != null) {
        for (CarbonFile c : f.listFiles()) {
          deleteRecursiveSilent(c);
        }
      }
    }
    if (f.exists() && !f.delete()) {
      return;
    }
  }

  /**
   * this method will read the property file for required details
   * like dbName, tableName, etc
   */
  private void init() {
    InputStream in = null;
    props = new Properties();
    try {
      URI uri = getClass().getClassLoader().getResource(PROPERTY_FILE_NAME).toURI();
      File file = new File(uri);
      in = new FileInputStream(file);
      props.load(in);
    } catch (IOException e) {
      e.printStackTrace();
    } catch (URISyntaxException e) {
      e.printStackTrace();
    } finally {
      CarbonUtil.closeStreams(in);
    }
  }

  /**
   * this method will form the dictionary directory paths
   */
  private void initDictionaryDirPaths() throws IOException {
    CarbonTablePath carbonTablePath =
        CarbonStorePath.getCarbonTablePath(this.carbonStorePath, carbonTableIdentifier);
    String dictionaryLocation = carbonTablePath.getMetadataDirectoryPath();
    FileFactory.FileType fileType = FileFactory.getFileType(dictionaryLocation);
    if(!FileFactory.isFileExist(dictionaryLocation, fileType)) {
      FileFactory.mkdirs(dictionaryLocation, fileType);
    }
    this.dictionaryFilePath = carbonTablePath.getDictionaryFilePath(columnIdentifier.getColumnId());
    this.dictionaryMetaFilePath = carbonTablePath.getDictionaryMetaFilePath(columnIdentifier.getColumnId());
  }
}
