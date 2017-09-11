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
package org.apache.carbondata.presto;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.scan.executor.exception.QueryExecutionException;
import org.apache.carbondata.core.scan.executor.impl.AbstractQueryExecutor;
import org.apache.carbondata.core.scan.executor.impl.VectorDetailQueryExecutor;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.result.iterator.AbstractDetailQueryResultIterator;
import org.apache.carbondata.core.scan.result.iterator.VectorDetailQueryResultIterator;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnarBatch;
import org.apache.carbondata.core.stats.QueryStatisticsRecorder;
import org.apache.carbondata.core.stats.QueryStatisticsRecorderImpl;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.hadoop.CarbonInputSplit;

import com.facebook.presto.hadoop.$internal.io.netty.util.concurrent.DefaultEventExecutorGroup;
import mockit.Mock;
import mockit.MockUp;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CarbonVectorizedRecordReaderTest {
  private static VectorDetailQueryResultIterator vectorDetailQueryResultIterator;
  private static CarbonVectorizedRecordReader carbonVectorizedRecordReader;
  private static VectorDetailQueryExecutor vectorDetailQueryExecutor;
  private static QueryModel queryModel;
  private static DefaultEventExecutorGroup defaultEventExecutorGroup;
  private static BlockExecutionInfo blockExecutionInfo;
  private static List<BlockExecutionInfo> blockExecutionInfoList;
  private static CarbonInputSplit carbonInputSplit;
  private static TaskAttemptContextImpl taskAttemptContext;
  private static TaskAttemptID taskAttemptID;
  private static QueryStatisticsRecorder queryStatisticsRecorder;

  @BeforeClass public static void setUp() {
    queryStatisticsRecorder = new QueryStatisticsRecorderImpl("1");
    blockExecutionInfo = new BlockExecutionInfo();
    blockExecutionInfo.setAbsoluteTableIdentifier(
        new AbsoluteTableIdentifier("default", new CarbonTableIdentifier("db", "tablename", "id")));
    blockExecutionInfoList = new ArrayList<>(Collections.singletonList(blockExecutionInfo));
    blockExecutionInfoList.add(blockExecutionInfo);
    defaultEventExecutorGroup = new DefaultEventExecutorGroup(1);
    queryModel = new QueryModel();
    queryModel.setAbsoluteTableIdentifier(
        new AbsoluteTableIdentifier("default", new CarbonTableIdentifier("db", "tablename", "id")));
    queryModel.setStatisticsRecorder(queryStatisticsRecorder);
    new MockUp<AbstractDetailQueryResultIterator>() {
      @Mock private void intialiseInfos() {
      }

      @Mock protected void initQueryStatiticsModel() {

      }
    };
    new MockUp<CarbonInputSplit>() {
      @Mock public List<TableBlockInfo> createBlocks(List<CarbonInputSplit> splitList) {
        TableBlockInfo tableBlockInfo = new TableBlockInfo();
        return new ArrayList<>(Arrays.asList(tableBlockInfo));
      }
    };
    new MockUp<AbstractQueryExecutor>() {
      @Mock protected List<BlockExecutionInfo> getBlockExecutionInfos(QueryModel queryModel)
          throws IOException, QueryExecutionException {
        return blockExecutionInfoList;
      }
    };
    new MockUp<VectorDetailQueryResultIterator>() {
      @Mock public void processNextBatch(CarbonColumnarBatch columnarBatch) {

      }
    };
    vectorDetailQueryResultIterator =
        new VectorDetailQueryResultIterator(blockExecutionInfoList, queryModel,
            defaultEventExecutorGroup);

    vectorDetailQueryExecutor = new VectorDetailQueryExecutor();
    carbonVectorizedRecordReader =
        new CarbonVectorizedRecordReader(vectorDetailQueryExecutor, queryModel,
            vectorDetailQueryResultIterator);
    taskAttemptID = new TaskAttemptID();
    Configuration configuration = new Configuration();
    taskAttemptContext = new TaskAttemptContextImpl(configuration, taskAttemptID);
  }

  @AfterClass public static void tearDown() throws IOException {
    carbonVectorizedRecordReader.close();
  }

  @Test public void testInitializeForEmptySplit()
      throws IOException, InterruptedException, NoSuchFieldException, IllegalAccessException {
    carbonInputSplit = new CarbonInputSplit();
    carbonVectorizedRecordReader.initialize(carbonInputSplit, taskAttemptContext);
    assertNotNull(FieldUtils.readField(carbonVectorizedRecordReader, "iterator", true));
  }

  @Test public void testInitializeForValidSplit()
      throws IOException, InterruptedException, NoSuchFieldException, IllegalAccessException {
    carbonInputSplit = new CarbonInputSplit();

    org.apache.hadoop.mapreduce.lib.input.FileSplit fileSplit =
        new org.apache.hadoop.mapreduce.lib.input.FileSplit(new Path("default"), 1L, 1L,
            new String[] { "location1", "location2" });
    new MockUp<org.apache.hadoop.mapreduce.lib.input.FileSplit>() {
      @Mock public String[] getLocations() throws IOException {
        return new String[] { "location1", "location2" };
      }
    };
    new MockUp<CarbonTablePath.DataFileUtil>() {
      @Mock public String getTaskNo(String carbonDataFileName) {
        return "task1";
      }
    };
    carbonVectorizedRecordReader
        .initialize(CarbonInputSplit.from("segementId", fileSplit, ColumnarFormatVersion.V3),
            taskAttemptContext);
    assertNotNull(FieldUtils.readField(carbonVectorizedRecordReader, "iterator", true));
  }

  @Test public void testNextKeyValue() throws IOException, InterruptedException {
    assertTrue(carbonVectorizedRecordReader.nextKeyValue());
  }

  @Test public void testNextKeyValueFalseCase() throws IOException, InterruptedException {
    new MockUp<AbstractDetailQueryResultIterator>() {
      @Mock public boolean hasNext() {
        return false;
      }
    };
    assertFalse(carbonVectorizedRecordReader.nextKeyValue());
  }
}

