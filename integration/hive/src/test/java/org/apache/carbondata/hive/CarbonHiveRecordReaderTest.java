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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.scan.executor.exception.QueryExecutionException;
import org.apache.carbondata.core.scan.executor.impl.DetailQueryExecutor;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.result.BatchResult;
import org.apache.carbondata.core.scan.result.iterator.AbstractDetailQueryResultIterator;
import org.apache.carbondata.core.scan.result.iterator.ChunkRowIterator;
import org.apache.carbondata.core.scan.result.iterator.DetailQueryResultIterator;
import org.apache.carbondata.hadoop.readsupport.CarbonReadSupport;

import mockit.Mock;
import mockit.MockUp;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class CarbonHiveRecordReaderTest {
    private static CarbonHiveRecordReader carbonHiveRecordReaderObj;
    private static AbsoluteTableIdentifier absoluteTableIdentifier;
    private static QueryModel queryModel = new QueryModel();
    private static CarbonReadSupport<ArrayWritable> readSupport = new CarbonDictionaryDecodeReadSupport<>();
    private static JobConf jobConf = new JobConf();
    private static InputSplit inputSplitNotInstanceOfHiveInputSplit, inputSplitInstanceOfHiveInputSplit;
    private static BatchResult batchResult = new BatchResult();
    private static Writable writable;
    private static CarbonIterator carbonIteratorObject;

    @BeforeClass
    public static void setUp() throws Exception {
        String array[] = {"neha", "01", "vaishali"};
        writable = new ArrayWritable(array);
        absoluteTableIdentifier = new AbsoluteTableIdentifier(
                "/home/neha/Projects/incubator-carbondata/examples/spark2/target/store",
                new CarbonTableIdentifier("DB", "TBL", "TBLID"));
        ColumnarFormatVersion columnarFormatVersion = ColumnarFormatVersion.V3;
        Path path = new Path(
                "/home/store/database/Fact/Part0/Segment_0/part-0-0_batchno0-0-1502197243476.carbondata");
        inputSplitInstanceOfHiveInputSplit =
                new CarbonHiveInputSplit("20", path, 1235L, 235L, array, columnarFormatVersion);
        List<Object[]> rowsList = new ArrayList(2);
        ExecutorService executorService = new ForkJoinPool();
        List<BlockExecutionInfo> blockExecutionInfoList = new ArrayList<>();

        blockExecutionInfoList.add(new BlockExecutionInfo());
        blockExecutionInfoList.add(new BlockExecutionInfo());

        jobConf.set("hive.io.file.readcolumn.ids", "01,02");
        jobConf.set(serdeConstants.LIST_COLUMN_TYPES, "int,string");

        rowsList.add(0, new Object[]{1, "neha"});
        rowsList.add(1, new Object[]{3, "divya"});
        batchResult.setRows(rowsList);

        new MockUp<QueryModel>() {
            @Mock
            public AbsoluteTableIdentifier getAbsoluteTableIdentifier() {
                return absoluteTableIdentifier;
            }
        };

        new MockUp<AbstractDetailQueryResultIterator>() {
            @Mock
            private void intialiseInfos() {
            }

            @Mock
            protected void initQueryStatiticsModel() {
            }
        };

        new MockUp<DetailQueryExecutor>() {
            @Mock
            public CarbonIterator<BatchResult> execute(QueryModel queryModel)
                    throws QueryExecutionException, IOException {
                return carbonIteratorObject;
            }
        };
        new MockUp<DetailQueryResultIterator>() {
            @Mock
            private BatchResult getBatchResult() {
                return batchResult;
            }
        };

        new MockUp<CarbonDictionaryDecodeReadSupport>() {
            @Mock
            public Object readRow(Object[] data) {
                Writable[] writables = new Writable[data.length];
                for (int i = 0; i < data.length; i++) {
                    switch (i) {
                        case 0:
                            new IntWritable((int) data[i]);
                            break;
                        case 1:
                            new Text(data[i].toString());
                            break;
                    }
                }
                return writables;
            }
        };
        carbonIteratorObject =
                new DetailQueryResultIterator(blockExecutionInfoList, queryModel, executorService);
    }

    public void testUnsupportedInputSplitTypeException() throws IOException {
        try {
            carbonHiveRecordReaderObj =
                    new CarbonHiveRecordReader(queryModel, readSupport, inputSplitNotInstanceOfHiveInputSplit,
                            jobConf);
            Assert.assertTrue(false);
        } catch (IOException | RuntimeException ex) {
            Assert.assertTrue(true);
        }
    }

    public void testQueryExecutionException() {

        new MockUp<DetailQueryExecutor>() {
            @Mock
            public CarbonIterator<BatchResult> execute(QueryModel queryModel)
                    throws QueryExecutionException, IOException {
                throw new QueryExecutionException("QueryExecutionException");
            }
        };
        try {
            carbonHiveRecordReaderObj =
                    new CarbonHiveRecordReader(queryModel, readSupport, inputSplitInstanceOfHiveInputSplit,
                            jobConf);
            Assert.assertTrue(false);
        } catch (IOException | RuntimeException ex) {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void testNullColumnProperty() throws IOException {
        jobConf.set(serdeConstants.LIST_COLUMN_TYPES, "");
        carbonHiveRecordReaderObj =
                new CarbonHiveRecordReader(queryModel, readSupport, inputSplitInstanceOfHiveInputSplit,
                        jobConf);
        Assert.assertEquals(carbonHiveRecordReaderObj.createValue().get().length, 0);
    }

    @Test
    public void testNext() throws IOException {
        carbonHiveRecordReaderObj =
                new CarbonHiveRecordReader(queryModel, readSupport, inputSplitInstanceOfHiveInputSplit,
                        jobConf);
        ArrayWritable arrayWritable = (ArrayWritable) writable;
        Void avoid = carbonHiveRecordReaderObj.createKey();
        Assert.assertEquals(true, carbonHiveRecordReaderObj.next(avoid, arrayWritable));
    }

    @Test
    public void testNextNullColumnId() throws IOException {
        jobConf.set("hive.io.file.readcolumn.ids", "");
        carbonHiveRecordReaderObj =
                new CarbonHiveRecordReader(queryModel, readSupport, inputSplitInstanceOfHiveInputSplit,
                        jobConf);
        ArrayWritable arrayWritable = (ArrayWritable) writable;
        Void avoid = carbonHiveRecordReaderObj.createKey();
        Assert.assertEquals(true, carbonHiveRecordReaderObj.next(avoid, arrayWritable));
    }

    @Test
    public void testNextFalseCase() throws IOException {
        new MockUp<ChunkRowIterator>() {
            @Mock
            public boolean hasNext() {
                return false;
            }
        };
        ArrayWritable arrayWritable = (ArrayWritable) writable;
        Void avoid = null;
        carbonHiveRecordReaderObj =
                new CarbonHiveRecordReader(queryModel, readSupport, inputSplitInstanceOfHiveInputSplit,
                        jobConf);
        Assert.assertEquals(false, carbonHiveRecordReaderObj.next(avoid, arrayWritable));
    }

    @Test
    public void testCreateKey() throws IOException {
        carbonHiveRecordReaderObj =
                new CarbonHiveRecordReader(queryModel, readSupport, inputSplitInstanceOfHiveInputSplit,
                        jobConf);
        Assert.assertEquals(carbonHiveRecordReaderObj.createKey(), null);
    }

    @Test
    public void testCreateValue() throws IOException {
        carbonHiveRecordReaderObj =
                new CarbonHiveRecordReader(queryModel, readSupport, inputSplitInstanceOfHiveInputSplit,
                        jobConf);
        Assert.assertEquals(carbonHiveRecordReaderObj.createValue().getClass(), ArrayWritable.class);
    }

    @Test
    public void testGetPos() throws IOException {
        carbonHiveRecordReaderObj =
                new CarbonHiveRecordReader(queryModel, readSupport, inputSplitInstanceOfHiveInputSplit,
                        jobConf);
        Assert.assertEquals(carbonHiveRecordReaderObj.getPos(), 0L);
    }

    @Test
    public void testGetProgress() throws IOException {
        carbonHiveRecordReaderObj =
                new CarbonHiveRecordReader(queryModel, readSupport, inputSplitInstanceOfHiveInputSplit,
                        jobConf);
        Assert.assertEquals(0F, carbonHiveRecordReaderObj.getProgress(),0);
    }
}