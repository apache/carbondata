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
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import mockit.Mock;
import mockit.MockUp;
import org.apache.thrift.TBase;
import org.carbondata.core.carbon.CarbonTableIdentifier;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.reader.ThriftReader;
import org.carbondata.core.util.CarbonDictionaryUtil;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.format.ColumnDictionaryChunk;
import org.carbondata.format.ColumnDictionaryChunkMeta;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * This class will test the functionality writing and
 * reading a dictionary and its corresponding metadata file
 */
public class CarbonDictionaryWriterImplTest {

    /**
     * one segment entry length
     * 4 byte each for segmentId, min, max value and 8 byte for endOffset which is of long type
     */
    private static final int ONE_SEGMENT_DETAIL_LENGTH = 20;

    private static final String PROPERTY_FILE_NAME = "carbonTest.properties";

    private CarbonTableIdentifier identifier;

    private String databaseName;

    private String tableName;

    private String storePath;

    private String columnName;

    private Properties props;

    /**
     * directory path for dictionary file
     */
    private String directoryPath;

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
        this.storePath = props.getProperty("storePath", "carbonStore");
        this.columnName = "Name";
        identifier = new CarbonTableIdentifier(databaseName, tableName);
        deleteStorePath();
        prepareDataSet();
    }

    @After public void tearDown() throws Exception {
        identifier = null;
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
        boolean isSharedDimension = false;
        // second parameter is chunk count which is for the number of
        // thrift objects written for a segment
        processColumnValuesForOneChunk(isSharedDimension, 1);
    }

    /**
     * test writers write functionality for a column shared across tables
     * in a database
     */
    @Test public void testWriteForSharedColumn() throws IOException {
        boolean isSharedDimension = true;
        // second parameter is chunk count which is for the number of
        // thrift objects written for a segment
        processColumnValuesForOneChunk(isSharedDimension, 1);
    }

    /**
     * test writing multiple dictionary chunks for a single segment
     */
    @Test public void testWriteMultipleChunksForOneSegment() throws IOException {
        deleteStorePath();
        CarbonProperties.getInstance()
                .addProperty(CarbonCommonConstants.DICTIONARY_ONE_CHUNK_SIZE, "1");
        // prepare dictionary writer object
        CarbonDictionaryWriterImpl writer = prepareWriter(false);
        writeDictionaryFile(writer, dataSet1);
        // record file size from where data has to be read
        long end_offset = CarbonUtil.getFileSize(this.dictionaryFilePath);
        // read metadata chunks from file
        List<TBase> metadataChunks = readDictionary(this.dictionaryMetaFilePath, 0, true);
        assertTrue(1 == metadataChunks.size());
        // prepare retrieved chunk metadata
        List<ColumnDictionaryChunkMeta> chunks = getMetadataChunksExpectedList(metadataChunks);
        long start_offset = 0L;
        ColumnDictionaryChunkMeta actual =
                new ColumnDictionaryChunkMeta(1, dataSet1.size(), start_offset, end_offset,
                        dataSet1.size());
        // validate chunk metadata - actual and expected
        for (ColumnDictionaryChunkMeta chunk : chunks) {
            validateDictionaryMetadata(chunk, actual);
        }
        //assert for chunk count
        List<TBase> dictionaryChunks = readDictionary(this.dictionaryFilePath, 0, false);
        assertTrue(dataSet1.size() == dictionaryChunks.size());
        // prepare expected dictionary chunk list
        List<String> expected = getDictionaryChunksExpectedList(dictionaryChunks);
        // validate the dictionary data
        compareDictionaryData(expected, dataSet1);
        CarbonProperties.getInstance().addProperty(CarbonCommonConstants.DICTIONARY_ONE_CHUNK_SIZE,
                CarbonCommonConstants.DICTIONARY_ONE_CHUNK_SIZE_DEFAULT);
    }

    /**
     * this method will prepare the list which contains the meta
     * chunk data expected read form file
     */
    private List<ColumnDictionaryChunkMeta> getMetadataChunksExpectedList(
            List<TBase> metadataChunks) {
        List<ColumnDictionaryChunkMeta> chunks =
                new ArrayList<ColumnDictionaryChunkMeta>(metadataChunks.size());
        for (TBase thriftObject : metadataChunks) {
            ColumnDictionaryChunkMeta chunkMeta = (ColumnDictionaryChunkMeta) thriftObject;
            chunks.add(chunkMeta);
        }
        return chunks;
    }

    /**
     * this method will prepare the list which contains the dictionary
     * chunk data expected read form file
     */
    private List<String> getDictionaryChunksExpectedList(List<TBase> tBases) {
        List<String> expected = new ArrayList<String>(tBases.size());
        for (TBase thriftObject : tBases) {
            ColumnDictionaryChunk chunk = (ColumnDictionaryChunk) thriftObject;
            expected.addAll(convertByteArrayListToStringValueList(chunk.getValues()));
        }
        return expected;
    }

    /**
     * prepare the dictionary writer object
     */
    private CarbonDictionaryWriterImpl prepareWriter(boolean isSharedDimension) {
        initDictionaryDirPaths(isSharedDimension);
        return new CarbonDictionaryWriterImpl(this.storePath, identifier, columnName,
                isSharedDimension);
    }

    /**
     * this mehtod will test the directory creation failure
     */
    @Test public void testDirectoryCreationFailure() throws Exception {
        try {
            // delete the store
            deleteStorePath();
            // mock folder creation method
            new MockUp<CarbonUtil>() {
                @Mock public boolean checkAndCreateFolder(String path) {
                    return false;
                }
            };
            // prepare the writer
            CarbonDictionaryWriterImpl writer = prepareWriter(false);
            try {
                for (String value : dataSet1) {
                    // exception should be thrown while writing the column values
                    writer.write(value);
                }
            } catch (IOException e) {
                assertTrue(true);
            } finally {
                writer.close();
            }
            FileFactory.FileType fileType = FileFactory.getFileType(this.storePath);
            // check for file existence and assert
            boolean fileExist = FileFactory.isFileExist(this.storePath, fileType);
            assertFalse(fileExist);
        } catch (IOException e) {
            e.printStackTrace();
        }
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
        CarbonDictionaryWriterImpl writer = prepareWriter(false);
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
        ColumnDictionaryChunk chunk = new ColumnDictionaryChunk();
        chunk.setValues(convertStringListToByteArrayList(dataSet1));
        // create folder for dictionary file to be created
        String dictionaryFilePath = createDictionaryDirectory(false);
        // create writer
        ThriftWriter writer = new ThriftWriter(dictionaryFilePath, true);
        writer.open();
        // write first chunk
        writer.write(chunk);
        // maintain the offset till end offset of first chunk
        long end_offsetToRead = CarbonUtil.getFileSize(dictionaryFilePath);
        chunk = new ColumnDictionaryChunk();
        chunk.setValues(convertStringListToByteArrayList(dataSet2));
        // write second dictionary chunk
        writer.write(chunk);
        // close the writer
        writer.close();
        FileFactory.FileType fileType = FileFactory.getFileType(dictionaryFilePath);
        CarbonFile carbonFile = FileFactory.getCarbonFile(dictionaryFilePath, fileType);
        // call truncate to get retain only first dictionary chunk in the file
        carbonFile.truncate(dictionaryFilePath, end_offsetToRead);
        // assert, validate thrift object size should only be one
        List<TBase> tBases = readDictionary(dictionaryFilePath, 0, false);
        assertTrue(tBases.size() == 1);
        // validate the data retrieved and it should match dataset1
        for (TBase thriftObject : tBases) {
            ColumnDictionaryChunk expected = (ColumnDictionaryChunk) thriftObject;
            compareDictionaryData(dataSet1,
                    convertByteArrayListToStringValueList(expected.getValues()));
        }
    }

    /**
     * this method will create the directory path for dictionary file
     */
    private String createDictionaryDirectory(boolean isSharedDimension) {
        String directoryPath =
                CarbonDictionaryUtil.getDirectoryPath(identifier, storePath, isSharedDimension);
        String dictionaryFilePath = CarbonDictionaryUtil
                .getDictionaryFilePath(identifier, directoryPath, columnName, isSharedDimension);
        String folderToCreate =
                dictionaryFilePath.substring(0, dictionaryFilePath.lastIndexOf('/'));
        CarbonUtil.checkAndCreateFolder(folderToCreate);
        return dictionaryFilePath;
    }

    /**
     * this method will test the reading of dictionary file from a given offset
     */
    @Test public void testReadingOfChunkMetadataFromAnOffset() throws Exception {
        // delete store path
        deleteStorePath();
        // prepare the writer to write dataset1
        CarbonDictionaryWriterImpl writer = prepareWriter(false);
        // write dataset1 data
        writeDictionaryFile(writer, dataSet1);
        // prepare the writer to write dataset2
        writer = prepareWriter(false);
        // write dataset2
        writeDictionaryFile(writer, dataSet2);
        // record the offset from where data has to be read
        long fileSize = CarbonUtil.getFileSize(this.dictionaryMetaFilePath);
        // prepare writer to write dataset3
        writer = prepareWriter(false);
        // write dataset 3
        writeDictionaryFile(writer, dataSet3);
        // read dictionary metadata from a given offset
        List<TBase> columnDictionaryChunkMeta =
                readDictionary(this.dictionaryMetaFilePath, fileSize, true);
        // assert for metadata list
        assertTrue(1 == columnDictionaryChunkMeta.size());
        for (TBase thriftObject : columnDictionaryChunkMeta) {
            ColumnDictionaryChunkMeta chunk = (ColumnDictionaryChunkMeta) thriftObject;
            // read dictionary using the metadata offset read above
            List<TBase> dictionaryDataChunks =
                    readDictionary(this.dictionaryFilePath, chunk.getStart_offset(), false);
            // assert for dictionary list size
            assertTrue(1 == dictionaryDataChunks.size());
            for (TBase dictionaryThrift : dictionaryDataChunks) {
                // validate the dictionary data and it should be equivalent to dataset3
                ColumnDictionaryChunk data = (ColumnDictionaryChunk) dictionaryThrift;
                List<String> expected = convertByteArrayListToStringValueList(data.getValues());
                compareDictionaryData(expected, dataSet3);
            }
        }
    }

    /**
     * This method will convert list of string value to list of byte buffer
     */
    private List<ByteBuffer> convertStringListToByteArrayList(List<String> valueList) {
        List<ByteBuffer> byteArrayValueList = new ArrayList<>(valueList.size());
        for (String columnValue : valueList) {
            // +4 bytes have been added to write byte length to buffer
            byte[] value = columnValue.getBytes(Charset.defaultCharset());
            ByteBuffer buffer =
                    ByteBuffer.allocate(CarbonCommonConstants.INT_SIZE_IN_BYTE + value.length);
            buffer.putInt(value.length);
            buffer.put(value, 0, value.length);
            buffer.rewind();
            byteArrayValueList.add(buffer);
        }
        return byteArrayValueList;
    }

    /**
     * This method will convert list of byte buffer to list of string
     */
    private List<String> convertByteArrayListToStringValueList(List<ByteBuffer> valueBufferList) {
        List<String> valueList = new ArrayList<>(valueBufferList.size());
        for (ByteBuffer buffer : valueBufferList) {
            int length = buffer.getInt();
            byte[] value = new byte[length];
            buffer.get(value, 0, value.length);
            valueList.add(new String(value, Charset.defaultCharset()));
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
    private void processColumnValuesForOneChunk(boolean isSharedDimension, int chunkCountForSegment)
            throws IOException {
        // delete store path
        deleteStorePath();
        // prepare writer
        CarbonDictionaryWriterImpl writer = prepareWriter(false);
        // write the data into file
        // test write api for passing list of byte array
        writer.write(convertStringListToByteArray(dataSet1));
        // close the writer
        writer.close();
        // record end offset of file
        long end_offset = CarbonUtil.getFileSize(this.dictionaryFilePath);
        // read dictionary chunk from dictionary file
        List<TBase> tBases = readDictionary(this.dictionaryFilePath, 0, false);
        // assert for size of list retrieved which should be equivalent to
        // chunk count for a segment
        assertTrue(chunkCountForSegment == tBases.size());
        // prepare the retrieved data
        List<String> expected = getDictionaryChunksExpectedList(tBases);
        List<String> columnList = convertChunkToValue(expected);
        // compare the expected and actual data
        compareDictionaryData(columnList, dataSet1);
        // read dictionary metadata chunks
        tBases = readDictionary(this.dictionaryMetaFilePath, 0, true);
        // assert
        assertTrue(1 == tBases.size());
        // prepare the metadata chunk retrieved data
        List<ColumnDictionaryChunkMeta> chunks = getMetadataChunksExpectedList(tBases);
        long start_offset = 0L;
        // validate actial chunk metadata with expected
        ColumnDictionaryChunkMeta actual =
                new ColumnDictionaryChunkMeta(1, 2, start_offset, end_offset, 1);
        for (ColumnDictionaryChunkMeta chunk : chunks) {
            validateDictionaryMetadata(chunk, actual);
        }
    }

    /**
     * this method will convert list of string to list of byte array
     */
    private List<byte[]> convertStringListToByteArray(List<String> valueList) {
        List<byte[]> byteArrayList = new ArrayList<>(valueList.size());
        for (String value : valueList) {
            byteArrayList.add(value.getBytes(Charset.defaultCharset()));
        }
        return byteArrayList;
    }

    /**
     * this method will validate the dictionary chunk metadata
     */
    private void validateDictionaryMetadata(ColumnDictionaryChunkMeta expected,
            ColumnDictionaryChunkMeta actual) {
        assertTrue(expected.getMin_surrogate_key() == actual.getMin_surrogate_key());
        assertTrue(expected.getMax_surrogate_key() == actual.getMax_surrogate_key());
        assertTrue(expected.getStart_offset() == actual.getStart_offset());
        assertTrue(expected.getEnd_offset() == actual.getEnd_offset());
        assertTrue(expected.getChunk_count() == actual.getChunk_count());
    }

    /**
     * this method will validate the dictionary data
     */
    private void compareDictionaryData(List<String> expected, List<String> actual) {
        assertTrue(expected.size() == actual.size());
        for (int i = 0; i < actual.size(); i++) {
            assertTrue(actual.get(i).equals(expected.get(i)));
        }
    }

    /**
     * this method will read the thrift object and add all the values to a list
     */
    private List<String> convertChunkToValue(List<String> chunks) {
        List<String> columnList =
                new ArrayList<String>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        for (String value : chunks) {
            columnList.add(value);
        }
        return columnList;
    }

    /**
     * this method will read the dictionary data and
     * metadata file and return the thrift object list
     */
    private List<TBase> readDictionary(String dictionaryPath, long offset,
            final boolean dictionaryMeta) throws IOException {
        List<TBase> chunkList = new ArrayList<TBase>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        ThriftReader thriftIn = null;
        try {
            thriftIn = new ThriftReader(dictionaryPath, new ThriftReader.TBaseCreator() {
                @Override public TBase create() {
                    if (dictionaryMeta) {
                        return new ColumnDictionaryChunkMeta();
                    } else {
                        return new ColumnDictionaryChunk();
                    }
                }
            });
            // Open it
            thriftIn.open();
            thriftIn.setReadOffset(offset);
            // Read objects
            while (thriftIn.hasNext()) {
                chunkList.add(thriftIn.read());
            }
        } finally {
            // Close reader
            if (null != thriftIn) {
                thriftIn.close();
            }
        }
        return chunkList;
    }

    /**
     * this method will delete the store path
     */
    private void deleteStorePath() {
        FileFactory.FileType fileType = FileFactory.getFileType(this.storePath);
        CarbonFile carbonFile = FileFactory.getCarbonFile(this.storePath, fileType);
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
    private void initDictionaryDirPaths(boolean isSharedDimension) {
        this.directoryPath =
                CarbonDictionaryUtil.getDirectoryPath(identifier, storePath, isSharedDimension);
        this.dictionaryFilePath = CarbonDictionaryUtil
                .getDictionaryFilePath(identifier, this.directoryPath, columnName,
                        isSharedDimension);
        this.dictionaryMetaFilePath = CarbonDictionaryUtil
                .getDictionaryMetadataFilePath(identifier, this.directoryPath, columnName,
                        isSharedDimension);
    }
}