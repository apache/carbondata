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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.carbondata.core.datastorage.store.compression.ValueCompressionModel;
import org.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.metadata.LeafNodeInfoColumnar;
import org.carbondata.core.reader.CarbonMetaDataReader;
import org.carbondata.core.util.CarbonMetadataUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * This class will test the functionality writing and
 * reading a dictionary and its corresponding metadata file
 */
public class CarbonMetaDataWriterTest {

    private String filePath;

    @Before
    public void setUp() throws Exception {
        filePath = "testMeta.fact";
        deleteFile();
        createFile();
    }

    @After
    public void tearDown() throws Exception {
        deleteFile();
    }

    /**
     * test writing fact metadata.
     */
    @Test
    public void testWriteFactMetadata() throws IOException {
        deleteFile();
        createFile();
        CarbonMetaDataWriter writer = new CarbonMetaDataWriter(filePath);

        List<LeafNodeInfoColumnar> infoColumnars = getLeafNodeInfoColumnars();

        writer.writeMetaData(
                CarbonMetadataUtil.convertFileMeta(infoColumnars, 6, new int[] { 2, 4, 5, 7 }), 0);

        CarbonMetaDataReader metaDataReader = new CarbonMetaDataReader(filePath, 0);
        assertTrue(metaDataReader.readMetaData() != null);
    }

    /**
     * test writing fact metadata.
     */
    @Test
    public void testReadFactMetadata() throws IOException {
        deleteFile();
        createFile();
        CarbonMetaDataWriter writer = new CarbonMetaDataWriter(filePath);

        List<LeafNodeInfoColumnar> infoColumnars = getLeafNodeInfoColumnars();

        writer.writeMetaData(CarbonMetadataUtil.convertFileMeta(infoColumnars, 6,
                new int[] { 2, 4, 5, 7 }), 0);

        CarbonMetaDataReader metaDataReader = new CarbonMetaDataReader(filePath, 0);
        List<LeafNodeInfoColumnar> nodeInfoColumnars =
                CarbonMetadataUtil.convertLeafNodeInfo(metaDataReader.readMetaData());

        assertTrue(nodeInfoColumnars.size() == infoColumnars.size());
    }

    private List<LeafNodeInfoColumnar> getLeafNodeInfoColumnars() {
        LeafNodeInfoColumnar infoColumnar = new LeafNodeInfoColumnar();
        infoColumnar.setStartKey(new byte[] { 1, 2, 3 });
        infoColumnar.setEndKey(new byte[] { 8, 9, 10 });
        infoColumnar.setKeyLengths(new int[] { 1, 2, 3, 4 });
        infoColumnar.setKeyOffSets(new long[] { 22, 44, 55, 77 });
        infoColumnar.setIsSortedKeyColumn(new boolean[] { false, true, false, true });
        infoColumnar.setColumnMinMaxData(
                new byte[][] { new byte[] { 1, 2 }, new byte[] { 3, 4 }, new byte[] { 4, 5 },
                        new byte[] { 5, 6 } });
        infoColumnar.setKeyBlockIndexLength(new int[] { 4, 7 });
        infoColumnar.setKeyBlockIndexOffSets(new long[] { 55, 88 });
        infoColumnar.setDataIndexMapLength(new int[] { 2, 6, 7, 8 });
        infoColumnar.setDataIndexMapOffsets(new long[] { 77, 88, 99, 111 });
        infoColumnar.setMeasureLength(new int[] { 6, 7 });
        infoColumnar.setMeasureOffset(new long[] { 33, 99 });
        infoColumnar.setAggKeyBlock(new boolean[]{true, true, true, true});
        ValueCompressionModel compressionModel = new ValueCompressionModel();
        compressionModel.setMaxValue(new Object[]{44d,55d});
        compressionModel.setMinValue(new Object[]{0d,0d});
        compressionModel.setDecimal(new int[]{0,0});
        compressionModel.setType(new char[]{'n', 'n'});
        compressionModel.setUniqueValue(new Object[]{0d,0d});
        compressionModel.setDataTypeSelected(new byte[2]);
        infoColumnar.setCompressionModel(compressionModel);
        List<LeafNodeInfoColumnar> infoColumnars = new ArrayList<LeafNodeInfoColumnar>();
        infoColumnars.add(infoColumnar);
        return infoColumnars;
    }

    /**
     * this method will delete file
     */
    private void deleteFile() {
        FileFactory.FileType fileType = FileFactory.getFileType(this.filePath);
        CarbonFile carbonFile = FileFactory.getCarbonFile(this.filePath, fileType);
        carbonFile.delete();
    }

    private void createFile() {
        FileFactory.FileType fileType = FileFactory.getFileType(this.filePath);
        CarbonFile carbonFile = FileFactory.getCarbonFile(this.filePath, fileType);
        carbonFile.createNewFile();
    }

}
