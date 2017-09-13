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

import com.facebook.presto.hadoop.$internal.io.netty.util.concurrent.DefaultEventExecutorGroup;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.testing.TestingConnectorSession;
import mockit.Mock;
import mockit.MockUp;
import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.PartitionInfo;
import org.apache.carbondata.core.metadata.schema.partition.PartitionType;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableSchema;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.scan.executor.exception.QueryExecutionException;
import org.apache.carbondata.core.scan.executor.impl.DetailQueryExecutor;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.result.BatchResult;
import org.apache.carbondata.core.scan.result.iterator.AbstractDetailQueryResultIterator;
import org.apache.carbondata.core.scan.result.iterator.DetailQueryResultIterator;
import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.presto.impl.CarbonLocalInputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapred.TaskAttemptContextImpl;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

public class CarbondataRecordSetTest {

    CarbonTable carbonTable = CarbonTable.buildFromTableInfo(getTableInfo(1000L));
    TupleDomain<ColumnHandle> domain = TupleDomain.all();
    Optional<Domain> domainAll = Optional.of(Domain.all(IntegerType.INTEGER));

    CarbonLocalInputSplit localSplits = new CarbonLocalInputSplit("segmentId", "path", 0, 5
            , new ArrayList<String>(), 5, Short.MAX_VALUE, new String[]{"d1", "d2"}, "detailInfo");
    CarbondataColumnConstraint constraints = new CarbondataColumnConstraint("", domainAll, false);
    List constraintsList = new ArrayList<CarbondataColumnConstraint>();
    CarbondataRecordSet carbondataRecordSet;
    QueryModel queryModel;

    @Before
    public void setup() {
        constraintsList.add(constraints);
        CarbondataSplit carbondataSplit = new CarbondataSplit("connectorId", new SchemaTableName("default", "tableName"), domain, localSplits, constraintsList);

        CarbondataColumnHandle carbondataColumnHandle = new CarbondataColumnHandle("connectorId", "id", IntegerType.INTEGER, 0, 0, 0, true, 0, "1", false, 0, 0);
        List<CarbondataColumnHandle> carbondataColumnHandles = new ArrayList<>();
        carbondataColumnHandles.add(carbondataColumnHandle);
        queryModel = new QueryModel();
        queryModel.setAbsoluteTableIdentifier(
                new AbsoluteTableIdentifier("default", new CarbonTableIdentifier("db", "tablename", "id")));
        TaskAttemptContext taskAttemptContext = new TaskAttemptContextImpl(new JobConf(), new TaskAttemptID("", 1, TaskType.MAP, 0, 0));
        carbondataRecordSet = new CarbondataRecordSet(carbonTable, TestingConnectorSession.SESSION, carbondataSplit, carbondataColumnHandles, queryModel, taskAttemptContext);

        new MockUp<CarbonLocalInputSplit>() {
            @Mock
            public CarbonInputSplit convertSplit(CarbonLocalInputSplit carbonLocalInputSplit) {
                return new CarbonInputSplit();
            }
        };

        new MockUp<CarbonInputSplit>() {
            @Mock
            public List<TableBlockInfo> createBlocks(List<CarbonInputSplit> splitList) {
                TableBlockInfo tableBlockInfo = new TableBlockInfo();
                List<TableBlockInfo> tableBlockInfos = new ArrayList<>();
                tableBlockInfos.add(tableBlockInfo);
                return tableBlockInfos;
            }
        };

    }


    private org.apache.carbondata.core.metadata.schema.table.TableInfo getTableInfo(long timeStamp) {
        org.apache.carbondata.core.metadata.schema.table.TableInfo info = new org.apache.carbondata.core.metadata.schema.table.TableInfo();
        info.setDatabaseName("schemaName");
        info.setLastUpdatedTime(timeStamp);
        info.setTableUniqueName("schemaName_tableName");
        info.setFactTable(getTableSchema());
        info.setStorePath("storePath");
        PartitionInfo partitionInfo = new PartitionInfo(info.getFactTable().getListOfColumns(), PartitionType.LIST);
        List partitionList = new ArrayList();
        partitionList.add(1);
        partitionInfo.setPartitionIds(partitionList);
        info.getFactTable().setPartitionInfo(partitionInfo);
        return info;
    }

    private TableSchema getTableSchema() {
        TableSchema tableSchema = new TableSchema();
        List<ColumnSchema> columnSchemaList = new ArrayList<ColumnSchema>();
        columnSchemaList.add(getColumnarMeasureColumn());
        columnSchemaList.add(getColumnarDimensionColumn());
        tableSchema.setListOfColumns(columnSchemaList);
        tableSchema.setTableName("tableName");
        return tableSchema;
    }

    private ColumnSchema getColumnarDimensionColumn() {
        ColumnSchema dimColumn = new ColumnSchema();
        dimColumn.setColumnar(true);
        dimColumn.setColumnName("imei");
        dimColumn.setColumnUniqueId(UUID.randomUUID().toString());
        dimColumn.setDataType(DataType.STRING);
        dimColumn.setDimensionColumn(true);
        List<Encoding> encodeList =
                new ArrayList<Encoding>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        encodeList.add(Encoding.DICTIONARY);
        dimColumn.setEncodingList(encodeList);
        dimColumn.setNumberOfChild(0);
        return dimColumn;
    }

    private ColumnSchema getColumnarMeasureColumn() {
        ColumnSchema dimColumn = new ColumnSchema();
        dimColumn.setColumnName("id");
        dimColumn.setColumnUniqueId(UUID.randomUUID().toString());
        dimColumn.setDataType(DataType.INT);
        return dimColumn;
    }

    @Test
    public void getColumnTypesTest() {
        List<Type> columnTypes = carbondataRecordSet.getColumnTypes();
        assertTrue(columnTypes.get(0).getDisplayName().equalsIgnoreCase("integer"));
    }

    @Test
    public void cursorTest() {

        new MockUp<DetailQueryExecutor>() {
            @Mock
            public CarbonIterator<BatchResult> execute(QueryModel queryModel)
                    throws QueryExecutionException, IOException {
                List<BlockExecutionInfo> blockExecutionInfoList;
                BlockExecutionInfo blockExecutionInfo = new BlockExecutionInfo();
                DefaultEventExecutorGroup defaultEventExecutorGroup;
                defaultEventExecutorGroup = new DefaultEventExecutorGroup(1);
                blockExecutionInfoList = new ArrayList<>(Collections.singletonList(blockExecutionInfo));
                blockExecutionInfoList.add(blockExecutionInfo);
                new MockUp<AbstractDetailQueryResultIterator>() {
                    @Mock
                    private void intialiseInfos() {
                    }

                    @Mock
                    protected void initQueryStatiticsModel() {

                    }
                };
                DetailQueryResultIterator detailQueryResultIterator =
                        new DetailQueryResultIterator(blockExecutionInfoList, queryModel,
                                defaultEventExecutorGroup);
                return detailQueryResultIterator;
            }
        };
        RecordCursor carbondataRecordCursor = carbondataRecordSet.cursor();
        assertEquals(((CarbondataRecordCursor) carbondataRecordCursor).getType(0), IntegerType.INTEGER);
    }

    @Test(expected = RuntimeException.class)
    public void cursorTestExceptionCase() {
        carbondataRecordSet.cursor();
    }

    @Test(expected = RuntimeException.class)
    public void cursorTestQueryExecutionExceptionCase() {

        new MockUp<DetailQueryExecutor>() {
            @Mock
            public CarbonIterator<BatchResult> execute(QueryModel queryModel)
                    throws QueryExecutionException, IOException {
                throw new QueryExecutionException("");
            }
        };
        carbondataRecordSet.cursor();
    }
}
