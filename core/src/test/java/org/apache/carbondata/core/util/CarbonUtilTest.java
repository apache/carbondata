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
package org.apache.carbondata.core.util;

import mockit.Mock;
import mockit.MockUp;

import org.apache.carbondata.core.carbon.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.carbon.datastore.chunk.DimensionChunkAttributes;
import org.apache.carbondata.core.carbon.datastore.chunk.impl.FixedLengthDimensionDataChunk;
import org.apache.carbondata.core.carbon.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.carbon.metadata.blocklet.datachunk.DataChunk;
import org.apache.carbondata.core.carbon.metadata.datatype.DataType;
import org.apache.carbondata.core.carbon.metadata.encoder.Encoding;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.datastorage.store.columnar.ColumnGroupModel;
import org.apache.carbondata.core.datastorage.store.compression.ValueCompressionModel;
import org.apache.carbondata.core.datastorage.store.filesystem.LocalCarbonFile;
import org.apache.carbondata.core.datastorage.store.impl.FileFactory;
import org.apache.carbondata.core.metadata.ValueEncoderMeta;
import org.apache.carbondata.scan.executor.infos.KeyStructureInfo;
import org.apache.carbondata.scan.model.QueryDimension;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.pentaho.di.core.exception.KettleException;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static junit.framework.TestCase.*;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;

public class CarbonUtilTest {

  private static DimensionChunkAttributes chunkAttribute;

  @BeforeClass public static void setUp() throws Exception {
    new File("../core/src/test/resources/testFile.txt").createNewFile();
    new File("../core/src/test/resources/testDatabase").mkdirs();
    chunkAttribute = new DimensionChunkAttributes();
  }

  @Test public void testGetBitLengthForDimensionGiveProperValue() {
    int[] cardinality = { 200, 1, 10000, 1, 10, 3 };
    int[] dimensionBitLength =
        CarbonUtil.getDimensionBitLength(cardinality, new int[] { 1, 1, 3, 1 });
    int[] expectedOutPut = { 8, 8, 14, 2, 8, 8 };
    for (int i = 0; i < dimensionBitLength.length; i++) {
      assertEquals(expectedOutPut[i], dimensionBitLength[i]);
    }
  }

  @Test(expected = IOException.class) public void testCloseStreams() throws IOException {
    FileReader stream = new FileReader("../core/src/test/resources/testFile.txt");
    BufferedReader br = new BufferedReader(stream);
    CarbonUtil.closeStreams(br);
    br.ready();
  }

  @Test public void testToGetCardinalityForPerIncrLessThan0() {
    int result = CarbonUtil.getIncrementedCardinality(6);
    assertEquals(result, 3);
  }

  @Test public void testToGetCardinality() {
    int result = CarbonUtil.getIncrementedCardinality(10);
    assertEquals(result, 4);
  }

  @Test public void testToGetCardinalityForArray() {
    int[] cardinality = { 10, 20, 0, 6 };
    int[] actualResult = CarbonUtil.getIncrementedCardinality(cardinality);
    int[] expectedResult = { 4, 5, 1, 3 };
    for (int i = 0; i < cardinality.length; i++) {
      assertEquals(actualResult[i], expectedResult[i]);
    }
  }

  @Test public void testToGetColGroupModel() {
    int[][] cardinality = { { 10, 20, 30 }, { 20, 30 }, {} };
    ColumnGroupModel actualResult = CarbonUtil.getColGroupModel(cardinality);
    assertEquals(actualResult.getNoOfColumnStore(), 3);
    int[] expectedResult = { 3, 2, 0 };
    for (int i = 0; i < actualResult.getColumnSplit().length; i++) {
      assertEquals(actualResult.getColumnSplit()[i], expectedResult[i]);
    }
  }

  @Test public void testToGetIncrementedCardinalityFullyFilled() {
    int[] cardinality = { 200, 20, 0, 10 };
    int[] actualResult = CarbonUtil.getIncrementedCardinalityFullyFilled(cardinality);
    int[] expectedResult = { 8, 8, 64, 8 };
    for (int i = 0; i < cardinality.length; i++) {
      assertEquals(actualResult[i], expectedResult[i]);
    }
  }

  @Test public void testToDeleteFolderForValidPath()
      throws CarbonUtilException, InterruptedException {
    File testDir = new File("../core/src/test/resources/testDir");
    testDir.mkdirs();
    CarbonUtil.deleteFoldersAndFiles(testDir);
    assertTrue(!testDir.isDirectory());
  }

  @Test(expected = CarbonUtilException.class) public void testToDeleteFolderWithIOException()
      throws CarbonUtilException, InterruptedException {
    File testDir = new File("../core/src/test/resources/testDir");
    new MockUp<UserGroupInformation>() {
      @SuppressWarnings("unused") @Mock public UserGroupInformation getLoginUser()
          throws IOException {
        throw new IOException();
      }
    };
    CarbonUtil.deleteFoldersAndFiles(testDir);
  }

  @Test(expected = CarbonUtilException.class)
  public void testToDeleteFolderWithInterruptedException()
      throws CarbonUtilException, InterruptedException {
    File testDir = new File("../core/src/test/resources/testDir");
    new MockUp<UserGroupInformation>() {
      @SuppressWarnings("unused") @Mock public UserGroupInformation getLoginUser()
          throws InterruptedException {
        throw new InterruptedException();
      }
    };
    CarbonUtil.deleteFoldersAndFiles(testDir);
  }

  @Test public void testToDeleteFileForValidPath()
      throws CarbonUtilException, InterruptedException {
    File testDir = new File("../core/src/test/resources/testDir/testFile.csv");
    testDir.mkdirs();
    CarbonUtil.deleteFoldersAndFiles(testDir);
    assertTrue(!testDir.isFile());
  }

  @Test public void testToDeleteFoldersAndFilesForValidFolder()
      throws CarbonUtilException, InterruptedException {
    String folderPath = "../core/src/test/resources/testDir/carbonDir";
    new File(folderPath).mkdirs();
    LocalCarbonFile testDir = new LocalCarbonFile(folderPath);
    CarbonUtil.deleteFoldersAndFiles(testDir);
    assertTrue(!testDir.exists());
  }

  @Test(expected = CarbonUtilException.class) public void testToDeleteFoldersAndFilesWithIOException()
      throws CarbonUtilException, InterruptedException {
    LocalCarbonFile testDir = new LocalCarbonFile("../core/src/test/resources/testDir/carbonDir");
    new MockUp<UserGroupInformation>() {
      @SuppressWarnings("unused") @Mock public UserGroupInformation getLoginUser()
          throws IOException {
        throw new IOException();
      }
    };
    CarbonUtil.deleteFoldersAndFiles(testDir);
  }

  @Test(expected = CarbonUtilException.class) public void testToDeleteFoldersAndFilesWithInterruptedException()
      throws CarbonUtilException, InterruptedException {
    LocalCarbonFile testDir = new LocalCarbonFile("../core/src/test/resources/testDir/carbonDir");
    new MockUp<UserGroupInformation>() {
      @SuppressWarnings("unused") @Mock public UserGroupInformation getLoginUser()
          throws InterruptedException {
        throw new InterruptedException();
      }
    };
    CarbonUtil.deleteFoldersAndFiles(testDir);
  }

  @Test public void testToDeleteFoldersAndFilesForValidCarbonFile()
      throws CarbonUtilException, InterruptedException {
    LocalCarbonFile testDir =
        new LocalCarbonFile("../core/src/test/resources/testDir/testCarbonFile");
    testDir.createNewFile();
    CarbonUtil.deleteFoldersAndFiles(testDir);
    assertTrue(!testDir.exists());
  }

  @Test public void testToGetBadLogPath() throws CarbonUtilException, InterruptedException {
    new MockUp<CarbonProperties>() {
      @SuppressWarnings("unused") @Mock public String getProperty(String key) {
        return "../unibi-solutions/system/carbon/badRecords";
      }
    };
    String badLogStoreLocation = CarbonUtil.getBadLogPath("badLogPath");
    assertEquals(badLogStoreLocation, "../unibi-solutions/system/carbon/badRecords/badLogPath");
  }

  @Test public void testToDeleteFoldersAndFilesForCarbonFileSilently()
      throws CarbonUtilException, InterruptedException {
    LocalCarbonFile testDir = new LocalCarbonFile("../core/src/test/resources/testDir");
    testDir.createNewFile();
    CarbonUtil.deleteFoldersAndFilesSilent(testDir);
    assertTrue(!testDir.exists());
  }

  @Test(expected = CarbonUtilException.class)
  public void testToDeleteFoldersAndFilesSintlyWithIOException()
      throws CarbonUtilException, IOException {
    new MockUp<UserGroupInformation>() {
      @SuppressWarnings("unused") @Mock public UserGroupInformation getLoginUser()
          throws IOException {
        throw new IOException();
      }
    };
    LocalCarbonFile testDir =
        new LocalCarbonFile("../unibi-solutions/system/carbon/badRecords/badLogPath");
    CarbonUtil.deleteFoldersAndFilesSilent(testDir);
  }

  @Test(expected = CarbonUtilException.class)
  public void testToDeleteFoldersAndFilesSintlyWithInterruptedException()
      throws CarbonUtilException, IOException {
    new MockUp<UserGroupInformation>() {
      @SuppressWarnings("unused") @Mock public UserGroupInformation getLoginUser()
          throws InterruptedException {
        throw new InterruptedException();
      }
    };
    LocalCarbonFile testDir =
        new LocalCarbonFile("../unibi-solutions/system/carbon/badRecords/badLogPath");
    CarbonUtil.deleteFoldersAndFilesSilent(testDir);
  }

  @Test public void testToDeleteFiles() throws IOException, CarbonUtilException {
    String baseDirectory = "../core/src/test/resources/";
    File file1 = new File(baseDirectory + "File1.txt");
    File file2 = new File(baseDirectory + "File2.txt");
    file1.createNewFile();
    file2.createNewFile();
    File[] files = { file1, file2 };
    CarbonUtil.deleteFiles(files);
    assertTrue(!file1.exists());
    assertTrue(!file2.exists());
  }

  @Test public void testToGetNextLesserValue() {
    byte[] dataChunks = { new Byte("5"), new Byte("15"), new Byte("30"), new Byte("50") };
    byte[] compareValues = { new Byte("5"), new Byte("15"), new Byte("30") };
    new MockUp<ByteUtil.UnsafeComparer>() {
      @SuppressWarnings("unused") @Mock
      public int compareTo(byte[] buffer1, int offset1, int length1, byte[] buffer2, int offset2,
          int length2) {
        return -1;
      }
    };
    FixedLengthDimensionDataChunk fixedLengthDataChunk =
        new FixedLengthDimensionDataChunk(dataChunks, chunkAttribute);
    int result = CarbonUtil.nextLesserValueToTarget(1, fixedLengthDataChunk, compareValues);
    assertEquals(result, 0);
  }

  @Test public void testToGetNextLesserValueToTarget() {
    byte[] dataChunks = { new Byte("5"), new Byte("15"), new Byte("30"), new Byte("50") };
    byte[] compareValues = { new Byte("5"), new Byte("15"), new Byte("30") };
    new MockUp<ByteUtil.UnsafeComparer>() {
      @SuppressWarnings("unused") @Mock
      public int compareTo(byte[] buffer1, int offset1, int length1, byte[] buffer2, int offset2,
          int length2) {
        return 1;
      }
    };
    FixedLengthDimensionDataChunk fixedLengthDataChunk =
        new FixedLengthDimensionDataChunk(dataChunks, chunkAttribute);
    int result = CarbonUtil.nextLesserValueToTarget(1, fixedLengthDataChunk, compareValues);
    assertEquals(result, -1);
  }

  @Test public void testToGetnextGreaterValue() {
    byte[] dataChunks = { new Byte("5"), new Byte("15"), new Byte("30"), new Byte("50") };
    byte[] compareValues = { new Byte("5"), new Byte("15"), new Byte("30") };
    new MockUp<ByteUtil.UnsafeComparer>() {
      @SuppressWarnings("unused") @Mock
      public int compareTo(byte[] buffer1, int offset1, int length1, byte[] buffer2, int offset2,
          int length2) {
        return 1;
      }
    };
    FixedLengthDimensionDataChunk fixedLengthDataChunk =
        new FixedLengthDimensionDataChunk(dataChunks, chunkAttribute);
    int result = CarbonUtil.nextGreaterValueToTarget(1, fixedLengthDataChunk, compareValues, 4);
    assertEquals(result, 2);
  }

  @Test public void testToConvertToIntegerList() {
    int[] integerArray = { 10, 20, 30, 40 };
    List<Integer> integerList = CarbonUtil.convertToIntegerList(integerArray);
    for (int i = 0; i < integerArray.length; i++) {
      assertEquals(integerArray[i], (int) integerList.get(i));
    }
  }

  @Test public void testToGetnextGreaterValueToTarget() {
    byte[] dataChunks = { new Byte("5"), new Byte("15"), new Byte("30"), new Byte("50") };
    byte[] compareValues = { new Byte("5"), new Byte("15"), new Byte("30") };
    new MockUp<ByteUtil.UnsafeComparer>() {
      @SuppressWarnings("unused") @Mock
      public int compareTo(byte[] buffer1, int offset1, int length1, byte[] buffer2, int offset2,
          int length2) {
        return 0;
      }
    };
    FixedLengthDimensionDataChunk fixedLengthDataChunk =
        new FixedLengthDimensionDataChunk(dataChunks, chunkAttribute);
    int result = CarbonUtil.nextGreaterValueToTarget(1, fixedLengthDataChunk, compareValues, 4);
    assertEquals(result, 4);
  }

  @Test public void testToWriteLevelCardinalityFile() throws KettleException {
    int[] dimCardinality = { 10, 20, 30, 40 };
    CarbonUtil.writeLevelCardinalityFile("../core/src/test/resources/testDatabase", "testTable",
        dimCardinality);
    assertTrue(new File("../core/src/test/resources/testDatabase/levelmetadata_testTable.metadata")
        .exists());
  }

  @Test public void testToGetCardinalityFromLevelMetadataFile() throws CarbonUtilException {
    int[] cardinality = CarbonUtil.getCardinalityFromLevelMetadataFile(
        "../core/src/test/resources/testDatabase/levelmetadata_testTable.metadata");
    int[] expectedCardinality = { 10, 20, 30, 40 };
    for (int i = 0; i < cardinality.length; i++) {
      assertEquals(cardinality[i], expectedCardinality[i]);
    }
  }

  @Test public void testToGetCardinalityFromLevelMetadataFileForInvalidPath()
      throws CarbonUtilException {
    int[] cardinality = CarbonUtil.getCardinalityFromLevelMetadataFile("");
    assertEquals(cardinality, null);
  }

  @Test public void testToUnescapeChar() {
    String[] input = { "\\001", "\\t", "\\r", "\\b", "\\n", "\\f" };
    String[] output = { "\001", "\t", "\r", "\b", "\n", "\f" };
    for (int i = 0; i < input.length; i++) {
      assertEquals(CarbonUtil.unescapeChar(input[i]), output[i]);
    }
  }

  @Test public void testForDelimiterConverter() {
    String[] input =
        { "|", "*", ".", ":", "^", "\\", "$", "+", "?", "(", ")", "{", "}", "[", "]", "'" };
    String[] expectedResult =
        { "\\|", "\\*", "\\.", "\\:", "\\^", "\\\\", "\\$", "\\+", "\\?", "\\(", "\\)", "\\{",
            "\\}", "\\[", "\\]", "'" };
    for (int i = 0; i < input.length; i++) {
      assertEquals(CarbonUtil.delimiterConverter(input[i]), expectedResult[i]);
    }
  }

  @Test public void testToCheckAndAppendHDFSUrlWithNoBlackSlash() {
    new MockUp<FileFactory>() {
      @SuppressWarnings("unused") @Mock public FileFactory.FileType getFileType(String path) {
        return FileFactory.FileType.LOCAL;
      }
    };
    new MockUp<CarbonProperties>() {
      @SuppressWarnings("unused") @Mock public String getProperty(String key) {
        return "BASE_URL";
      }
    };
    String hdfsURL = CarbonUtil.checkAndAppendHDFSUrl("../core/src/test/resources/testDatabase");
    assertEquals(hdfsURL, "BASE_URL/../core/src/test/resources/testDatabase");
  }

  @Test public void testToCheckAndAppendHDFSUrlWithBlackSlash() {
    new MockUp<FileFactory>() {
      @SuppressWarnings("unused") @Mock public FileFactory.FileType getFileType(String path) {
        return FileFactory.FileType.LOCAL;
      }
    };
    new MockUp<CarbonProperties>() {
      @SuppressWarnings("unused") @Mock public String getProperty(String key) {
        return "BASE_URL/";
      }
    };
    String hdfsURL = CarbonUtil.checkAndAppendHDFSUrl("../core/src/test/resources/testDatabase");
    assertEquals(hdfsURL, "BASE_URL/../core/src/test/resources/testDatabase");
  }

  @Test public void testToCheckAndAppendHDFSUrlWithNull() {
    new MockUp<FileFactory>() {
      @SuppressWarnings("unused") @Mock public FileFactory.FileType getFileType(String path) {
        return FileFactory.FileType.LOCAL;
      }
    };
    new MockUp<CarbonProperties>() {
      @SuppressWarnings("unused") @Mock public String getProperty(String key) {
        return null;
      }
    };
    String hdfsURL = CarbonUtil.checkAndAppendHDFSUrl("../core/src/test/resources/testDatabase");
    assertEquals(hdfsURL, "../core/src/test/resources/testDatabase");
  }

  @Test public void testForisFileExists() {
    assertTrue(CarbonUtil.isFileExists("../core/src/test/resources/testFile.txt"));
  }

  @Test public void testForisFileExistsWithException() {
    new MockUp<FileFactory>() {
      @SuppressWarnings("unused") @Mock public FileFactory.FileType getFileType(String path)
          throws IOException {
        throw new IOException();
      }
    };
    assertTrue(!CarbonUtil.isFileExists("../core/src/test/resources/testFile.txt"));
  }

  @Test public void testToCheckAndCreateFolder() {
    boolean exists = CarbonUtil.checkAndCreateFolder("../core/src/test/resources/testDatabase");
    boolean created = CarbonUtil.checkAndCreateFolder("../core/src/test/resources/newDatabase");
    assertTrue(exists);
    assertTrue(created);
  }

  @Test public void testToCheckAndCreateFolderWithException() {
    new MockUp<FileFactory>() {
      @SuppressWarnings("unused") @Mock public FileFactory.FileType getFileType(String path)
          throws IOException {
        throw new IOException();
      }
    };
    boolean exists = CarbonUtil.checkAndCreateFolder("../core/src/test/resources/testDatabase1");
    assertTrue(!exists);
  }

  @Test public void testToGetFileSize() {
    assertEquals(CarbonUtil.getFileSize("../core/src/test/resources/testFile.txt"), 0);
  }

  @Test public void testForHasEncoding() {
    List<Encoding> encodingList = new ArrayList<>();
    encodingList.add(Encoding.INVERTED_INDEX);
    encodingList.add(Encoding.DICTIONARY);
    encodingList.add(Encoding.DELTA);
    assertTrue(CarbonUtil.hasEncoding(encodingList, Encoding.DICTIONARY));
    assertTrue(!CarbonUtil.hasEncoding(encodingList, Encoding.BIT_PACKED));
  }

  @Test public void testForHasDataTypes() {
    DataType[] dataTypes = { DataType.DECIMAL, DataType.BOOLEAN, DataType.INT };
    assertTrue(CarbonUtil.hasDataType(DataType.BOOLEAN, dataTypes));
    assertTrue(!CarbonUtil.hasDataType(DataType.DATE, dataTypes));
  }

  @Test public void testForHasComplexDataTypes() {
    assertTrue(CarbonUtil.hasComplexDataType(DataType.ARRAY));
    assertTrue(!CarbonUtil.hasComplexDataType(DataType.DATE));
  }

  @Test public void testToGetDictionaryEncodingArray() {
    QueryDimension column1 = new QueryDimension("Column1");
    QueryDimension column2 = new QueryDimension("Column2");
    ColumnSchema column1Schema = new ColumnSchema();
    ColumnSchema column2Schema = new ColumnSchema();
    column1Schema.setColumnName("Column1");
    List<Encoding> encoding = new ArrayList<>();
    encoding.add(Encoding.DICTIONARY);
    column1Schema.setEncodingList(encoding);
    column1.setDimension(new CarbonDimension(column1Schema, 1, 1, 1, 1));

    column2Schema.setColumnName("Column2");
    List<Encoding> encoding2 = new ArrayList<>();
    encoding2.add(Encoding.DELTA);
    column2Schema.setEncodingList(encoding2);
    column2.setDimension(new CarbonDimension(column2Schema, 1, 1, 1, 1));

    QueryDimension[] queryDimensions = { column1, column2 };

    boolean[] dictionaryEncoding = CarbonUtil.getDictionaryEncodingArray(queryDimensions);
    boolean[] expectedDictionaryEncoding = { true, false };
    for (int i = 0; i < dictionaryEncoding.length; i++) {
      assertEquals(dictionaryEncoding[i], expectedDictionaryEncoding[i]);
    }
  }

  @Test public void testToGetDirectDictionaryEncodingArray() {
    QueryDimension column1 = new QueryDimension("Column1");
    QueryDimension column2 = new QueryDimension("Column2");
    ColumnSchema column1Schema = new ColumnSchema();
    ColumnSchema column2Schema = new ColumnSchema();
    column1Schema.setColumnName("Column1");
    List<Encoding> encoding = new ArrayList<>();
    encoding.add(Encoding.DIRECT_DICTIONARY);
    column1Schema.setEncodingList(encoding);
    column1.setDimension(new CarbonDimension(column1Schema, 1, 1, 1, 1));

    column2Schema.setColumnName("Column2");
    List<Encoding> encoding2 = new ArrayList<>();
    encoding2.add(Encoding.DELTA);
    column2Schema.setEncodingList(encoding2);
    column2.setDimension(new CarbonDimension(column2Schema, 1, 1, 1, 1));

    QueryDimension[] queryDimensions = { column1, column2 };

    boolean[] dictionaryEncoding = CarbonUtil.getDirectDictionaryEncodingArray(queryDimensions);
    boolean[] expectedDictionaryEncoding = { true, false };
    for (int i = 0; i < dictionaryEncoding.length; i++) {
      assertEquals(dictionaryEncoding[i], expectedDictionaryEncoding[i]);
    }
  }

  @Test public void testToGetComplexDataTypeArray() {
    QueryDimension column1 = new QueryDimension("Column1");
    QueryDimension column2 = new QueryDimension("Column2");
    ColumnSchema column1Schema = new ColumnSchema();
    ColumnSchema column2Schema = new ColumnSchema();
    column1Schema.setColumnName("Column1");
    column1Schema.setDataType(DataType.DATE);
    column1.setDimension(new CarbonDimension(column1Schema, 1, 1, 1, 1));

    column2Schema.setColumnName("Column2");
    column2Schema.setDataType(DataType.ARRAY);
    column2.setDimension(new CarbonDimension(column2Schema, 1, 1, 1, 1));

    QueryDimension[] queryDimensions = { column1, column2 };

    boolean[] dictionaryEncoding = CarbonUtil.getComplexDataTypeArray(queryDimensions);
    boolean[] expectedDictionaryEncoding = { false, true };
    for (int i = 0; i < dictionaryEncoding.length; i++) {
      assertEquals(dictionaryEncoding[i], expectedDictionaryEncoding[i]);
    }
  }

  @Test public void testToReadMetadatFile() throws CarbonUtilException {
    new MockUp<DataFileFooterConverter>() {
      @SuppressWarnings("unused") @Mock
      public DataFileFooter readDataFileFooter(TableBlockInfo info) {
        DataFileFooter fileFooter = new DataFileFooter();
        fileFooter.setVersionId((short)1);
        return fileFooter;
      }
    };
    TableBlockInfo info = new TableBlockInfo("file:/", 1, "0", new String[0], 1, (short)1);
    
    assertEquals(CarbonUtil.readMetadatFile(info).getVersionId(), 1);
  }

  @Test(expected = CarbonUtilException.class) public void testToReadMetadatFileWithException()
      throws Exception {
	TableBlockInfo info = new TableBlockInfo("file:/", 1, "0", new String[0], 1, (short)1);
    CarbonUtil.readMetadatFile(info);
  }

  @Test public void testToFindDimension() {
    ColumnSchema column1Schema = new ColumnSchema();
    ColumnSchema column2Schema = new ColumnSchema();
    column1Schema.setColumnName("Column1");
    column2Schema.setColumnName("Column2");
    List<CarbonDimension> carbonDimension = new ArrayList<>();
    carbonDimension.add(new CarbonDimension(column1Schema, 1, 1, 1, 1));
    carbonDimension.add(new CarbonDimension(column2Schema, 2, 1, 2, 1));
    assertEquals(CarbonUtil.findDimension(carbonDimension, "Column1"),
        new CarbonDimension(column1Schema, 1, 1, 1, 1));
  }

  @Test public void testToGetFormattedCardinality() {
    ColumnSchema column1Schema = new ColumnSchema();
    ColumnSchema column2Schema = new ColumnSchema();
    List<Encoding> encoding = new ArrayList<>();
    encoding.add(Encoding.DICTIONARY);
    List<Encoding> encoding2 = new ArrayList<>();
    encoding2.add(Encoding.DIRECT_DICTIONARY);
    column1Schema.setEncodingList(encoding);
    column2Schema.setEncodingList(encoding2);
    List<ColumnSchema> columnSchemas = new ArrayList<>();
    columnSchemas.add(column1Schema);
    columnSchemas.add(column2Schema);
    int[] columnCardinality = { 1, 5 };
    int[] result = CarbonUtil.getFormattedCardinality(columnCardinality, columnSchemas);
    int[] expectedResult = { 1, 5 };
    for (int i = 0; i < result.length; i++) {
      assertEquals(result[i], expectedResult[i]);
    }
  }

  @Test public void testToGetColumnSchemaList() {
    ColumnSchema column1Schema = new ColumnSchema();
    ColumnSchema column2Schema = new ColumnSchema();
    column1Schema.setColumnName("Column1");
    column2Schema.setColumnName("Column2");
    List<CarbonDimension> carbonDimension = new ArrayList<>();
    carbonDimension.add(new CarbonDimension(column1Schema, 1, 1, 1, 1));
    carbonDimension.add(new CarbonDimension(column2Schema, 2, 2, 2, 1));

    List<CarbonMeasure> carbonMeasure = new ArrayList<>();
    carbonMeasure.add(new CarbonMeasure(column1Schema, 1));
    carbonMeasure.add(new CarbonMeasure(column2Schema, 2));

    List<ColumnSchema> columnSchema =
        CarbonUtil.getColumnSchemaList(carbonDimension, carbonMeasure);
    for (int i = 0; i < carbonMeasure.size(); i++) {
      assertEquals(columnSchema.get(i), carbonMeasure.get(i).getColumnSchema());
    }
  }

  @Test public void testToReadHeader() throws IOException {
    File file = new File("../core/src/test/resources/sampleCSV.csv");
    FileWriter writer = new FileWriter(file);
    writer.write("id,name");
    writer.flush();
    String headers = CarbonUtil.readHeader("../core/src/test/resources/sampleCSV.csv");
    assertEquals(headers, "id,name");
    file.deleteOnExit();
  }

  @Test public void testToReadHeaderWithFileNotFoundException() throws IOException {
    new MockUp<FileFactory>() {
      @SuppressWarnings("unused") @Mock
      public DataInputStream getDataInputStream(String path, FileFactory.FileType fileType)
          throws FileNotFoundException {
        throw new FileNotFoundException();
      }
    };
    String result = CarbonUtil.readHeader("../core/src/test/resources/sampleCSV");
    assertEquals(null, result);
  }

  @Test public void testToReadHeaderWithIOException() throws IOException {
    new MockUp<FileFactory>() {
      @SuppressWarnings("unused") @Mock
      public DataInputStream getDataInputStream(String path, FileFactory.FileType fileType)
          throws IOException {
        throw new IOException();
      }
    };
    String result = CarbonUtil.readHeader("../core/src/test/resources/sampleCSV.csv");
    assertEquals(null, result);
  }

  @Test public void testToPrintLine() {
    String line = CarbonUtil.printLine("*", 2);
    assertEquals(line, "**");
  }

  @Test public void testToGetSegmentString() {
    List<String> list = new ArrayList<>();
    list.add("1");
    list.add("2");
    String segments = CarbonUtil.getSegmentString(list);
    assertEquals(segments, "1,2");
  }

  @Test public void testToGetSegmentStringWithEmptySegmentList() {
    List<String> list = new ArrayList<>();
    String segments = CarbonUtil.getSegmentString(list);
    assertEquals(segments, "");
  }

  @Test public void testToGetSurrogateKey() {
    byte[] data = { 1, 1 };
    ByteBuffer byteBuffer = ByteBuffer.allocate(8);
    int a = CarbonUtil.getSurrogateKey(data, byteBuffer);
    assertEquals(a, 257);
  }

  @Test public void testToGetValueCompressionModel() {
    List<DataChunk> dataChunkList = new ArrayList<>();
    DataChunk dataChunk = new DataChunk();

    List<Encoding> encodingList = new ArrayList<>();
    encodingList.add(Encoding.DELTA);
    dataChunk.setEncoderList(encodingList);

    List<ValueEncoderMeta> valueEncoderMetas = new ArrayList<>();
    ValueEncoderMeta valueEncoderMeta = new ValueEncoderMeta();
    valueEncoderMeta.setMaxValue(5.0);
    valueEncoderMeta.setMinValue(1.0);
    valueEncoderMeta.setUniqueValue(2.0);
    valueEncoderMeta.setType('n');
    valueEncoderMeta.setDataTypeSelected((byte) 'v');
    valueEncoderMetas.add(valueEncoderMeta);
    dataChunk.setValueEncoderMeta(valueEncoderMetas);
    dataChunkList.add(dataChunk);
    ValueCompressionModel valueCompressionModel =
        CarbonUtil.getValueCompressionModel(dataChunkList.get(0).getValueEncoderMeta());
    assertEquals(1, valueCompressionModel.getMaxValue().length);
  }

  @Test public void testToGetDictionaryChunkSize() {
    new MockUp<CarbonProperties>() {
      @SuppressWarnings("unused") @Mock public CarbonProperties getInstance()
          throws NumberFormatException {
        throw new NumberFormatException();
      }
    };
    int expectedResult = CarbonUtil.getDictionaryChunkSize();
    assertEquals(expectedResult, 10000);
  }

  @Test public void testToPackByteBufferIntoSingleByteArrayWithNull() {
    byte[] byteArray = CarbonUtil.packByteBufferIntoSingleByteArray(null);
    assertEquals(null, byteArray);
  }

  @Test public void testToPackByteBufferIntoSingleByteArray() {
    ByteBuffer[] byteBuffers = { ByteBuffer.allocate(1), ByteBuffer.allocate(2) };
    byte[] byteArray = CarbonUtil.packByteBufferIntoSingleByteArray(byteBuffers);
    byte[] expectedResult = { 0, 4, 0, 5, 0, 0, 0, 0 };
    for (int i = 0; i < byteArray.length; i++) {
      assertEquals(expectedResult[i], byteArray[i]);
    }
  }

  @Test public void testToIdentifyDimensionType() {
    ColumnSchema column1Schema = new ColumnSchema();
    ColumnSchema column2Schema = new ColumnSchema();
    ColumnSchema column3Schema = new ColumnSchema();
    column1Schema.setColumnName("Column1");
    column1Schema.setColumnar(true);
    column1Schema.setEncodingList(Arrays.asList(Encoding.DELTA, Encoding.DICTIONARY));
    column2Schema.setColumnName("Column2");
    column2Schema.setColumnar(false);
    column2Schema.setEncodingList(Arrays.asList(Encoding.DELTA, Encoding.DICTIONARY));
    column3Schema.setColumnName("Column3");
    column3Schema.setColumnar(true);
    column3Schema.setEncodingList(Arrays.asList(Encoding.DELTA, Encoding.INVERTED_INDEX));
    CarbonDimension carbonDimension = new CarbonDimension(column1Schema,1,1,1,1);
    CarbonDimension carbonDimension2 = new CarbonDimension(column2Schema,2,2,2,2);
    CarbonDimension carbonDimension3 = new CarbonDimension(column3Schema,3,3,3,3);
    List<CarbonDimension> carbonDimensions = Arrays.asList(carbonDimension, carbonDimension2, carbonDimension3);
    boolean[] result = CarbonUtil.identifyDimensionType(carbonDimensions);
    assertThat(result, is(equalTo(new boolean[]{true, true, false})));
  }

  @Test public void testToGetFirstIndexUsingBinarySearchWithCompareTo1() {
    DimensionChunkAttributes dimensionChunkAttributes = new DimensionChunkAttributes();
    new MockUp<ByteUtil.UnsafeComparer>() {
      @SuppressWarnings("unused") @Mock public int compareTo(byte[] buffer1, int offset1, int length1, byte[] buffer2, int offset2,
          int length2) {
        return 1;
      }
    };
    byte[] dataChunks = {10,20,30};
    byte[] compareValue = {1,20,30};
    FixedLengthDimensionDataChunk fixedLengthDimensionDataChunk = new FixedLengthDimensionDataChunk(dataChunks, dimensionChunkAttributes);
    int result = CarbonUtil.getFirstIndexUsingBinarySearch(fixedLengthDimensionDataChunk,1, 3, compareValue, false);
    assertEquals(-2, result);
  }

  @Test public void testToGetFirstIndexUsingBinarySearchWithCompareToLessThan0() {
    DimensionChunkAttributes dimensionChunkAttributes = new DimensionChunkAttributes();
    new MockUp<ByteUtil.UnsafeComparer>() {
      @SuppressWarnings("unused") @Mock public int compareTo(byte[] buffer1, int offset1, int length1, byte[] buffer2, int offset2,
          int length2) {
        return -1;
      }
    };
    byte[] dataChunks = {10,20,30};
    byte[] compareValue = {1,20,30};
    FixedLengthDimensionDataChunk fixedLengthDimensionDataChunk = new FixedLengthDimensionDataChunk(dataChunks, dimensionChunkAttributes);
    int result = CarbonUtil.getFirstIndexUsingBinarySearch(fixedLengthDimensionDataChunk,1, 3, compareValue, false);
    assertEquals(-5, result);
  }

  @Test public void testToGetFirstIndexUsingBinarySearchWithCompareTo0() {
    DimensionChunkAttributes dimensionChunkAttributes = new DimensionChunkAttributes();
    new MockUp<ByteUtil.UnsafeComparer>() {
      @SuppressWarnings("unused") @Mock public int compareTo(byte[] buffer1, int offset1, int length1, byte[] buffer2, int offset2,
          int length2) {
        return 0;
      }
    };
    byte[] dataChunks = {10,20,30};
    byte[] compareValue = {1,20,30};
    FixedLengthDimensionDataChunk fixedLengthDimensionDataChunk = new FixedLengthDimensionDataChunk(dataChunks, dimensionChunkAttributes);
    int result = CarbonUtil.getFirstIndexUsingBinarySearch(fixedLengthDimensionDataChunk,1, 3, compareValue, false);
    assertEquals(0, result);
  }

  @Test public void testToGetFirstIndexUsingBinarySearchWithMatchUpLimitTrue() {
    DimensionChunkAttributes dimensionChunkAttributes = new DimensionChunkAttributes();
    new MockUp<ByteUtil.UnsafeComparer>() {
      @SuppressWarnings("unused") @Mock public int compareTo(byte[] buffer1, int offset1, int length1, byte[] buffer2, int offset2,
          int length2) {
        return 0;
      }
    };
    byte[] dataChunks = {10,20,30};
    byte[] compareValue = {1,20,30};
    FixedLengthDimensionDataChunk fixedLengthDimensionDataChunk = new FixedLengthDimensionDataChunk(dataChunks, dimensionChunkAttributes);
    int result = CarbonUtil.getFirstIndexUsingBinarySearch(fixedLengthDimensionDataChunk,1, 3, compareValue, true);
    assertEquals(3, result);
  }

  @AfterClass public static void testcleanUp() {
    new File("../core/src/test/resources/testFile.txt").deleteOnExit();
    new File("../core/src/test/resources/testDatabase/levelmetadata_testTable.metadata")
        .deleteOnExit();
    new File("../core/src/test/resources/testDatabase").delete();
    new File("../core/src/test/resources/newDatabase").deleteOnExit();
  }

}
