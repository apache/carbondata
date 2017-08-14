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

package org.apache.carbondata.hive;

import mockit.Mock;
import mockit.MockUp;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CarbonHiveInputSplitTest {

    private static int count = 0;
    private final int DEFAULT_COLLECTION_SIZE = 32;
    private String defaultSegmentId = "789";
    private long start = 123456789L;
    private long length = 5000L;
    private String[] locations = {"Location_1", "Location_2", "Location_3"};
    private CarbonHiveInputSplit split = null;
    private int numberOfBlocklet = 12;
    private Map<String, String> blockStorageIdMap = new HashMap<>(DEFAULT_COLLECTION_SIZE);

    private CarbonHiveInputSplit getCarbonHiveInputSplit(String segmentId) {
        return new CarbonHiveInputSplit(segmentId, new Path("path-to-test-bucket_id-for-hdfs"), start,
                length, locations, ColumnarFormatVersion.V3);
    }

    @Before
    public void setUp() throws Exception {

        new MockUp<CarbonTablePath.DataFileUtil>() {
            @Mock
            public String getTaskNo(String carbonDataFileName) {
                return "120";
            }

            @Mock
            public String getBucketNo(String carbonFilePath) {
                return "5";
            }

            @Mock
            public String getPartNo(String carbonDataFileName) {
                return "17";
            }

        };

        new MockUp<FileSplit>() {
            @Mock
            public Path getPath() {
                return new Path("1234");
            }

            @Mock
            public long getStart() {
                return 123456987L;
            }

            @Mock
            public long getLength() {
                return 123456789L;
            }

            @Mock
            public java.lang.String[] getLocations() throws java.io.IOException {
                return new String[]{"Location_1", "Loaction_2", "Location_3"};
            }

            @Mock
            public void readFields(DataInput in) throws IOException {
            }

            @Mock
            public void write(DataOutput out) throws IOException {
            }
        };
        count = 0;
        split = getCarbonHiveInputSplit(defaultSegmentId);
    }

    @After
    public void tearDown() {
        split = null;
        count = 0;
    }

    @Test
    public void testDefaultConstructor() {
        new MockUp<CarbonProperties>() {
            @Mock
            private void validateAndLoadDefaultProperties() {
            }

            @Mock
            private void loadProperties() {
            }

            @Mock
            public ColumnarFormatVersion getFormatVersion() {
                return ColumnarFormatVersion.V3;
            }
        };

        new MockUp<LogServiceFactory>() {
            @Mock
            public LogService getLogService(final String className) {
                return null;
            }
        };
        split = null;
        split = new CarbonHiveInputSplit();

        Assert.assertNull(split.getSegmentId());
        Assert.assertEquals("0", split.getBucketId());
        Assert.assertEquals(0, split.getNumberOfBlocklets());
        Assert.assertEquals(new ArrayList<String>(), split.getInvalidSegments());
        Assert.assertEquals(ColumnarFormatVersion.V3, split.getVersion());
    }

    @Test
    public void testConstructorWithoutSpecifyingNoOfBlocklets() {
        split = null;
        split = getCarbonHiveInputSplit(defaultSegmentId);

        Assert.assertEquals(defaultSegmentId, split.getSegmentId());
        Assert.assertEquals("5", split.getBucketId());
        Assert.assertEquals(new ArrayList<String>(), split.getInvalidSegments());
        Assert.assertEquals(ColumnarFormatVersion.V3, split.getVersion());
    }

    @Test
    public void testConstructorSpecifyingNoOfBlocklets() {
        split = null;
        split = new CarbonHiveInputSplit(defaultSegmentId, new Path("path-to-test-bucket_id-for-hdfs"),
                start, length, locations, numberOfBlocklet, ColumnarFormatVersion.V3);

        Assert.assertEquals(defaultSegmentId, split.getSegmentId());
        Assert.assertEquals("5", split.getBucketId());
        Assert.assertEquals(numberOfBlocklet, split.getNumberOfBlocklets());
        Assert.assertEquals(new ArrayList<String>(), split.getInvalidSegments());
        Assert.assertEquals(ColumnarFormatVersion.V3, split.getVersion());
    }

    @Test
    public void testConstructorInitializeBlockStorageIdMap() {
        split = null;
        split = new CarbonHiveInputSplit(defaultSegmentId, new Path("path-to-test-bucket_id-for-hdfs"),
                start, length, locations, numberOfBlocklet, ColumnarFormatVersion.V3, blockStorageIdMap);

        Assert.assertEquals(defaultSegmentId, split.getSegmentId());
        Assert.assertEquals(numberOfBlocklet, split.getNumberOfBlocklets());
        Assert.assertEquals(new ArrayList<String>(), split.getInvalidSegments());
        Assert.assertEquals(ColumnarFormatVersion.V3, split.getVersion());
        Assert.assertEquals(new HashMap<>(DEFAULT_COLLECTION_SIZE), split.getBlockStorageIdMap());
    }

    @Test
    public void testConstructorWithFileSplit() throws IOException {
        FileSplit fileSplit =
                new FileSplit(new Path("path-to-test-bucket_id-for-hdfs"), start, length, locations);
        CarbonHiveInputSplit hiveSplit =
                CarbonHiveInputSplit.from("Dummy_SegmentID", fileSplit, ColumnarFormatVersion.V2);

        Assert.assertNotNull(hiveSplit);
        Assert.assertEquals("Dummy_SegmentID", hiveSplit.getSegmentId());
        Assert.assertEquals("5", hiveSplit.getBucketId());
    }

    @Test
    public void testCreateBlocks() {
        List<CarbonHiveInputSplit> splitList = new ArrayList<>();
        splitList.add(getCarbonHiveInputSplit(defaultSegmentId));
        splitList.add(getCarbonHiveInputSplit("199"));
        splitList.add(getCarbonHiveInputSplit("299"));
        splitList.add(getCarbonHiveInputSplit("193"));

        List<TableBlockInfo> list = CarbonHiveInputSplit.createBlocks(splitList);

        Assert.assertEquals(start, list.get(2).getBlockLength());
        Assert.assertEquals(4, list.size());
    }

    @Test
    public void testReadFields() throws IOException {

        new MockUp<DataInputStream>() {
            @Mock
            String readUTF() throws IOException {
                return "Segment_Id_213";
            }

            @Mock
            short readShort() throws IOException {
                return 3;
            }

            @Mock
            int readInt() throws IOException {
                return 4;
            }
        };

        DataInput in = new DataInputStream(null);

        split.readFields(in);

        Assert.assertEquals("Segment_Id_213", split.getSegmentId());
        Assert.assertEquals("Segment_Id_213", split.getBucketId());
        Assert.assertEquals(4, split.getNumberOfBlocklets());
        Assert.assertEquals(4, split.getInvalidSegments().size());
        Assert.assertEquals(ColumnarFormatVersion.V3, split.getVersion());
    }

    @Test
    public void testCompareFirstGreaterSegmentId() {
        int result = split.compareTo(getCarbonHiveInputSplit("127"));

        Assert.assertEquals(1, result);
    }

    @Test
    public void testCompareSecondGreaterSegmentId() {
        int result = split.compareTo(
                new CarbonHiveInputSplit("1923", new Path("path-to-test-bucket_id-for-hdfs"), start, length,
                        locations, numberOfBlocklet, ColumnarFormatVersion.V3, blockStorageIdMap));

        Assert.assertEquals(-1, result);
    }

    @Test
    public void testCompareNull() {
        int result = split.compareTo(null);

        Assert.assertEquals(-1, result);
    }

    @Test
    public void testCompareValidCarbonFile() {
        new MockUp<CarbonTablePath>() {
            @Mock
            public boolean isCarbonDataFile(String fileNameWithPath) {
                return true;
            }
        };

        new MockUp<CarbonTablePath.DataFileUtil>() {
            @Mock
            public String getTaskNo(String carbonDataFileName) {
                if (count % 2 == 0) {
                    count++;
                    return "128";
                } else {
                    count++;
                    return "123";
                }
            }
        };

        int result = split.compareTo(split);

        Assert.assertEquals(5, result);
    }

    @Test
    public void testCompareInvalidCarbonFile() {
        new MockUp<CarbonTablePath>() {
            @Mock
            public boolean isCarbonDataFile(String fileNameWithPath) {
                return false;
            }
        };

        new MockUp<FileSplit>() {
            @Mock
            public Path getPath() {
                if (count % 2 == 0) {
                    count++;
                    return new Path("1219");
                } else {
                    count++;
                    return new Path("1221");
                }
            }
        };

        int result = split.compareTo(split);

        Assert.assertEquals(-1, result);
    }

    @Test
    public void testCompareDifferentTaskId() {
        new MockUp<CarbonTablePath.DataFileUtil>() {
            @Mock
            public String getTaskNo(String carbonDataFileName) {
                if (count % 2 == 0) {
                    count++;
                    return "127";
                } else {
                    count++;
                    return "123";
                }
            }
        };

        new MockUp<FileSplit>() {
            @Mock
            public Path getPath() {
                if (count % 2 == 0) {
                    count++;
                    return new Path("1232");
                } else {
                    count++;
                    return new Path("1239");
                }
            }
        };

        int result = split.compareTo(getCarbonHiveInputSplit(defaultSegmentId));

        Assert.assertEquals(7, result);
    }

    @Test
    public void testCompareDifferentBucketNo() {
        new MockUp<CarbonTablePath>() {
            @Mock
            public boolean isCarbonDataFile(String fileNameWithPath) {
                return true;
            }
        };

        new MockUp<CarbonTablePath.DataFileUtil>() {
            @Mock
            public String getTaskNo(String carbonDataFileName) {
                return "128";
            }

            @Mock
            public String getBucketNo(String carbonDataFileName) {
                if (count % 2 == 0) {
                    count++;
                    return "5";
                } else {
                    count++;
                    return "6";
                }
            }
        };

        new MockUp<FileSplit>() {
            @Mock
            public Path getPath() {
                if (count % 2 == 0) {
                    count++;
                    return new Path("1236");
                } else {
                    count++;
                    return new Path("1237");
                }
            }
        };

        int result = split.compareTo(split);

        Assert.assertEquals(-1, result);
    }

    @Test
    public void testCompareDifferentPartNo() {

        new MockUp<CarbonTablePath>() {
            @Mock
            public boolean isCarbonDataFile(String fileNameWithPath) {
                return true;
            }
        };

        new MockUp<CarbonTablePath.DataFileUtil>() {
            @Mock
            public String getTaskNo(String carbonDataFileName) {
                return "128";
            }

            @Mock
            public String getBucketNo(String carbonDataFileName) {
                return "6";
            }

            @Mock
            public String getPartNo(String carbonDataFileName) {
                if (count % 2 == 0) {
                    count++;
                    return "18";
                } else {
                    count++;
                    return "17";
                }
            }
        };

        int result = split.compareTo(split);

        Assert.assertEquals(1, result);
    }

    @Test
    public void testCompareSameSplit() {
        new MockUp<CarbonTablePath>() {
            @Mock
            public boolean isCarbonDataFile(String fileNameWithPath) {
                return false;
            }
        };

        new MockUp<CarbonTablePath.DataFileUtil>() {
            @Mock
            public String getTaskNo(String carbonDataFileName) {
                return "120";
            }

            @Mock
            public String getBucketNo(String carbonFilePath) {
                return "5";
            }

            @Mock
            public String getPartNo(String carbonDataFileName) {
                return "17";
            }

        };

        new MockUp<FileSplit>() {
            @Mock
            public Path getPath() {
                return new Path("1234");
            }
        };

        int result = split.compareTo(split);

        Assert.assertEquals(0, result);
    }

    @Test
    public void testGetTableBlockInfo() {
        TableBlockInfo tableBlockInfo = CarbonHiveInputSplit.getTableBlockInfo(split);

        Assert.assertEquals(defaultSegmentId, tableBlockInfo.getSegmentId());
        Assert.assertEquals(blockStorageIdMap, tableBlockInfo.getBlockStorageIdMap());
        Assert.assertTrue(tableBlockInfo instanceof TableBlockInfo);
    }

    @Test(expected = RuntimeException.class)
    public void testIOExceptionInGetTableBlockInfo() {
        new MockUp<FileSplit>() {
            @Mock
            public Path getPath() throws IOException {
                throw new IOException();
            }
        };
        TableBlockInfo tableBlockInfo = CarbonHiveInputSplit.getTableBlockInfo(split);
    }

    @Test(expected = RuntimeException.class)
    public void testIOExceptionCreateBlocks() {
        new MockUp<FileSplit>() {
            @Mock
            public Path getPath() throws IOException {
                throw new IOException();
            }
        };
        List<CarbonHiveInputSplit> splitList = new ArrayList<>();
        splitList.add(getCarbonHiveInputSplit(defaultSegmentId));
        splitList.add(getCarbonHiveInputSplit("199"));
        List<TableBlockInfo> list = CarbonHiveInputSplit.createBlocks(splitList);
    }

    @Test
    public void testEqual() {
        String testInstance = "test_instance";

        Assert.assertTrue(split.equals(split));
        Assert.assertFalse(split.equals(testInstance));
        Assert.assertFalse(split.equals(getCarbonHiveInputSplit("12365")));
    }

    @Test
    public void testHashCode() {
        int actualResult = split.hashCode();

        Assert.assertEquals(-653103797, actualResult);
    }

}
