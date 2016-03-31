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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import mockit.Mock;
import mockit.MockUp;
import org.apache.thrift.TBase;
import org.carbondata.core.carbon.CarbonDictionaryMetadata;
import org.carbondata.core.carbon.CarbonTypeIdentifier;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.reader.CarbonDictionaryMetadataReader;
import org.carbondata.core.reader.ThriftReader;
import org.carbondata.core.util.CarbonDictionaryUtil;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.format.ColumnDictionaryChunk;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * This class will test the functionality writing and
 * reading a dictionary and its corresponding metadata file
 */
public class CarbonDictionaryWriterTest {

    /**
     * one segment entry length
     * 4 byte each for segmentId, min, max value and 8 byte for endOffset which is of long type
     */
    private static final int ONE_SEGMENT_DETAIL_LENGTH = 20;

    private static final String PROPERTY_FILE_NAME = "carbonTest.properties";

    private CarbonTypeIdentifier identifier;

    private String databaseName;

    private String tableName;

    private String storePath;

    private String columnName;

    private Properties props;

    @Before public void setUp() throws Exception {
        init();
        this.databaseName = props.getProperty("database", "testSchema");
        this.tableName = props.getProperty("tableName", "carbon");
        this.storePath = props.getProperty("storePath", "carbonStore");
        this.columnName = "Name";
        identifier = new CarbonTypeIdentifier(databaseName, tableName);
    }

    @After public void tearDown() throws Exception {
        identifier = null;
        deleteStorePath();
    }

    @Test public void processColumnUniqueValueList() throws Exception {
        List<String> vals = new ArrayList<String>(10);
        vals.add("manish");
        vals.add("shahid");
        String segmentName = "segment_0";
        boolean isSharedDimension = false;
        String directoryPath =
                CarbonDictionaryUtil.getDirectoryPath(identifier, storePath, isSharedDimension);
        String metadataFilePath = CarbonDictionaryUtil
                .getDictionaryMetadataFilePath(identifier, directoryPath, columnName,
                        isSharedDimension);
        String dictionaryFilePath = CarbonDictionaryUtil
                .getDictionaryFilePath(identifier, directoryPath, columnName, isSharedDimension);
        deleteFileIfExists(metadataFilePath);
        deleteFileIfExists(dictionaryFilePath);
        CarbonDictionaryWriter writer =
                new CarbonDictionaryWriter(identifier, vals.iterator(), columnName, segmentName,
                        this.storePath, isSharedDimension);
        writer.processColumnUniqueValueList();
        List<ColumnDictionaryChunk> columnDictionaryChunks = readDictionary(dictionaryFilePath);
        assertTrue(1 == columnDictionaryChunks.size());
        List<ByteBuffer> values =
                new ArrayList<ByteBuffer>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        for (ColumnDictionaryChunk chunk : columnDictionaryChunks) {
            values = chunk.getValues();
        }
        assertTrue(3 == values.size());
        List<String> columnList = convertChunkToValue(values);
        compareDictionaryData(columnList, vals);
        CarbonDictionaryMetadata expected = new CarbonDictionaryMetadata(0, 1, 3, 32);
        validateDictionaryMetadata(expected, metadataFilePath, ONE_SEGMENT_DETAIL_LENGTH);
    }

    @Test public void testMultipleChunksOfData() throws Exception {
        List<String> list1 = new ArrayList<String>(10);
        list1.add("manish");
        list1.add("shahid");
        String segmentName = "segment_0";
        boolean isSharedDimension = false;
        String directoryPath =
                CarbonDictionaryUtil.getDirectoryPath(identifier, storePath, isSharedDimension);
        String metadataFilePath = CarbonDictionaryUtil
                .getDictionaryMetadataFilePath(identifier, directoryPath, columnName,
                        isSharedDimension);
        String dictionaryFilePath = CarbonDictionaryUtil
                .getDictionaryFilePath(identifier, directoryPath, columnName, isSharedDimension);
        deleteFileIfExists(metadataFilePath);
        deleteFileIfExists(dictionaryFilePath);
        CarbonDictionaryWriter writer =
                new CarbonDictionaryWriter(identifier, list1.iterator(), columnName, segmentName,
                        this.storePath, isSharedDimension);
        writer.processColumnUniqueValueList();
        List<String> list2 = new ArrayList<String>(10);
        list2.add("a");
        list2.add("b");
        list2.add("c");
        writer = new CarbonDictionaryWriter(identifier, list2.iterator(), columnName, segmentName,
                this.storePath, isSharedDimension);
        writer.processColumnUniqueValueList();
        List<List<String>> mergedList =
                new ArrayList<List<String>>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        mergedList.add(list1);
        mergedList.add(list2);
        List<ColumnDictionaryChunk> columnDictionaryChunks = readDictionary(dictionaryFilePath);
        assertTrue(columnDictionaryChunks.size() == 2);
        for (int i = 0; i < columnDictionaryChunks.size(); i++) {
            List<String> expected = convertChunkToValue(columnDictionaryChunks.get(i).getValues());
            compareDictionaryData(expected, mergedList.get(i));
        }
        CarbonDictionaryMetadata expected = new CarbonDictionaryMetadata(0, 4, 6, 47);
        validateDictionaryMetadata(expected, metadataFilePath, ONE_SEGMENT_DETAIL_LENGTH);
    }

    @Test public void processColumnUniqueValueListAsSharedDimension() throws Exception {
        List<String> vals = new ArrayList<String>(10);
        vals.add("manish");
        vals.add("shahid");
        String segmentName = "segment_0";
        boolean isSharedDimension = true;
        String directoryPath =
                CarbonDictionaryUtil.getDirectoryPath(identifier, storePath, isSharedDimension);
        String metadataFilePath = CarbonDictionaryUtil
                .getDictionaryMetadataFilePath(identifier, directoryPath, columnName,
                        isSharedDimension);
        String dictionaryFilePath = CarbonDictionaryUtil
                .getDictionaryFilePath(identifier, directoryPath, columnName, isSharedDimension);
        deleteFileIfExists(metadataFilePath);
        deleteFileIfExists(dictionaryFilePath);
        CarbonDictionaryWriter writer =
                new CarbonDictionaryWriter(identifier, vals.iterator(), columnName, segmentName,
                        this.storePath, isSharedDimension);
        writer.processColumnUniqueValueList();
        List<ColumnDictionaryChunk> columnDictionaryChunks = readDictionary(dictionaryFilePath);
        assertTrue(1 == columnDictionaryChunks.size());
        List<ByteBuffer> values =
                new ArrayList<ByteBuffer>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        for (ColumnDictionaryChunk chunk : columnDictionaryChunks) {
            values = chunk.getValues();
        }
        assertTrue(3 == values.size());
        List<String> columnList = convertChunkToValue(values);
        compareDictionaryData(columnList, vals);
        CarbonDictionaryMetadata expected = new CarbonDictionaryMetadata(0, 1, 3, 32);
        validateDictionaryMetadata(expected, metadataFilePath, ONE_SEGMENT_DETAIL_LENGTH);
    }

    @Test public void testMultipleChunksOfDataAsSharedDimension() throws Exception {
        List<String> list1 = new ArrayList<String>(10);
        list1.add("manish");
        list1.add("shahid");
        String segmentName = "segment_0";
        boolean isSharedDimension = true;
        String directoryPath =
                CarbonDictionaryUtil.getDirectoryPath(identifier, storePath, isSharedDimension);
        String metadataFilePath = CarbonDictionaryUtil
                .getDictionaryMetadataFilePath(identifier, directoryPath, columnName,
                        isSharedDimension);
        String dictionaryFilePath = CarbonDictionaryUtil
                .getDictionaryFilePath(identifier, directoryPath, columnName, isSharedDimension);
        deleteFileIfExists(metadataFilePath);
        deleteFileIfExists(dictionaryFilePath);
        CarbonDictionaryWriter writer =
                new CarbonDictionaryWriter(identifier, list1.iterator(), columnName, segmentName,
                        this.storePath, isSharedDimension);
        writer.processColumnUniqueValueList();
        List<String> list2 = new ArrayList<String>(10);
        list2.add("a");
        list2.add("b");
        list2.add("c");
        writer = new CarbonDictionaryWriter(identifier, list2.iterator(), columnName, segmentName,
                this.storePath, isSharedDimension);
        writer.processColumnUniqueValueList();
        List<List<String>> mergedList =
                new ArrayList<List<String>>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        mergedList.add(list1);
        mergedList.add(list2);
        List<ColumnDictionaryChunk> columnDictionaryChunks = readDictionary(dictionaryFilePath);
        assertTrue(columnDictionaryChunks.size() == 2);
        for (int i = 0; i < columnDictionaryChunks.size(); i++) {
            List<String> expected = convertChunkToValue(columnDictionaryChunks.get(i).getValues());
            compareDictionaryData(expected, mergedList.get(i));
        }
        CarbonDictionaryMetadata expected = new CarbonDictionaryMetadata(0, 4, 6, 47);
        validateDictionaryMetadata(expected, metadataFilePath, ONE_SEGMENT_DETAIL_LENGTH);
    }

    @Test public void testDirectoryCreationFailure() {
        try {
            deleteStorePath();
            new MockUp<CarbonDictionaryUtil>() {
                @Mock public boolean checkAndCreateFolder(String path) {
                    return false;
                }
            };
            List<String> vals = new ArrayList<String>(10);
            vals.add("manish");
            vals.add("shahid");
            String segmentName = "segment_0";
            boolean isSharedDimension = false;
            String directoryPath =
                    CarbonDictionaryUtil.getDirectoryPath(identifier, storePath, isSharedDimension);
            String metadataFilePath = CarbonDictionaryUtil
                    .getDictionaryMetadataFilePath(identifier, directoryPath, columnName,
                            isSharedDimension);
            String dictionaryFilePath = CarbonDictionaryUtil
                    .getDictionaryFilePath(identifier, directoryPath, columnName,
                            isSharedDimension);
            deleteFileIfExists(metadataFilePath);
            deleteFileIfExists(dictionaryFilePath);
            CarbonDictionaryWriter writer =
                    new CarbonDictionaryWriter(identifier, vals.iterator(), columnName, segmentName,
                            this.storePath, isSharedDimension);
            writer.processColumnUniqueValueList();
            FileFactory.FileType fileType = FileFactory.getFileType(this.storePath);
            boolean fileExist = FileFactory.isFileExist(directoryPath, fileType);
            assertFalse(fileExist);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void validateDictionaryMetadata(CarbonDictionaryMetadata expected,
            String metadataFilePath, int skipByte) {
        CarbonDictionaryMetadata dictionaryMetadata = CarbonDictionaryMetadataReader
                .readAndGetDictionaryMetadataForLastSegment(metadataFilePath, skipByte);
        assertTrue(expected.getSegmentId() == dictionaryMetadata.getSegmentId());
        assertTrue(expected.getMin() == dictionaryMetadata.getMin());
        assertTrue(expected.getMax() == dictionaryMetadata.getMax());
        assertTrue(expected.getOffset() == dictionaryMetadata.getOffset());
    }

    private void compareDictionaryData(List<String> expected, List<String> actual) {
        assertTrue(expected.size() == actual.size());
        for (int i = 0; i < actual.size(); i++) {
            assertTrue(actual.get(i).equals(expected.get(i)));
        }
    }

    private List<String> convertChunkToValue(List<ByteBuffer> chunks) {
        List<String> columnList =
                new ArrayList<String>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        for (ByteBuffer buffer : chunks) {
            int length = buffer.getInt();
            byte[] buff = new byte[length];
            buffer.get(buff, 0, buff.length);
            String value = new String(buff);
            if (CarbonCommonConstants.MEMBER_DEFAULT_VAL.equals(value)) {
                continue;
            }
            columnList.add(value);
        }
        return columnList;
    }

    private List<ColumnDictionaryChunk> readDictionary(String dictionaryPath) throws IOException {
        List<ColumnDictionaryChunk> chunkList = new ArrayList<ColumnDictionaryChunk>();
        ThriftReader thriftIn = new ThriftReader(dictionaryPath, new ThriftReader.TBaseCreator() {
            @Override public TBase create() {
                return new ColumnDictionaryChunk();
            }
        });

        // Open it
        thriftIn.open();

        // Read objects
        while (thriftIn.hasNext()) {
            chunkList.add((ColumnDictionaryChunk) thriftIn.read());
        }

        // Close reader
        thriftIn.close();
        return chunkList;
    }

    private void deleteStorePath() {
        FileFactory.FileType fileType = FileFactory.getFileType(this.storePath);
        CarbonFile carbonFile = FileFactory.getCarbonFile(this.storePath, fileType);
        deleteRecursiveSilent(carbonFile);
    }

    private void deleteFileIfExists(String fileName) {
        FileFactory.FileType fileType = FileFactory.getFileType(fileName);
        try {
            if (FileFactory.isFileExist(fileName, fileType)) {
                CarbonFile carbonFile = FileFactory.getCarbonFile(fileName, fileType);
                carbonFile.delete();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

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
}