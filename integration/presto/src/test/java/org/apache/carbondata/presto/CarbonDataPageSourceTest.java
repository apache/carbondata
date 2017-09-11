package org.apache.carbondata.presto;

import com.facebook.presto.hadoop.$internal.io.netty.util.concurrent.DefaultEventExecutorGroup;
import com.facebook.presto.spi.*;
import com.facebook.presto.spi.block.SliceArrayBlock;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import mockit.Mock;
import mockit.MockUp;
import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.metadata.schema.table.TableSchema;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.scan.executor.QueryExecutor;
import org.apache.carbondata.core.scan.executor.QueryExecutorFactory;
import org.apache.carbondata.core.scan.executor.exception.QueryExecutionException;
import org.apache.carbondata.core.scan.executor.impl.VectorDetailQueryExecutor;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.model.QueryDimension;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.result.BatchResult;
import org.apache.carbondata.core.scan.result.iterator.AbstractDetailQueryResultIterator;
import org.apache.carbondata.core.scan.result.iterator.VectorDetailQueryResultIterator;
import org.apache.carbondata.presto.impl.CarbonLocalInputSplit;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.execution.vectorized.ColumnarBatch;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class CarbonDataPageSourceTest {

    private static TableInfo tableInfo;
    private static CarbonTable carbonTable;
    private static SchemaTableName schemaTable;
    private static TupleDomain<ColumnHandle> domain;
    private static CarbonLocalInputSplit localSplits;
    private static QueryModel queryModel;
    private static CarbondataSplit split;
    private static CarbondataRecordSet carbondataRecordSet;
    private static CarbondataPageSource carbonPage;
    private static CarbonTableIdentifier carbonTableIdentifier;

    @BeforeClass
    public static void setUp() {
        ColumnSchema carbonSchema = new ColumnSchema();
        carbonSchema.setColumnName("id");
        carbonSchema.setColumnUniqueId(UUID.randomUUID().toString());
        carbonSchema.setDataType(DataType.INT);
        TableSchema tableSchema = new TableSchema();
        List<ColumnSchema> columnSchemaList = new ArrayList<ColumnSchema>();
        columnSchemaList.add(carbonSchema);
        tableSchema.setListOfColumns(columnSchemaList);
        tableSchema.setTableName("table1");
        tableInfo = new TableInfo();

        tableInfo.setDatabaseName("schema1");
        tableInfo.setLastUpdatedTime(1234L);
        tableInfo.setTableUniqueName("schema1_tableName");
        tableInfo.setFactTable(tableSchema);
        tableInfo.setStorePath("storePath");
        carbonTable = CarbonTable.buildFromTableInfo(tableInfo);
        schemaTable = new SchemaTableName("schemaName", "tableName");
        domain = TupleDomain.all();
        localSplits = new CarbonLocalInputSplit("segmentId", "path", 0, 5, new ArrayList<String>(), 5,
                Short.MAX_VALUE, new String[]{}, "detailInfo");
        Optional<Domain> domainAll = Optional.of(Domain.all(IntegerType.INTEGER));

        CarbondataColumnConstraint constraints = new CarbondataColumnConstraint("", domainAll, false);
        List constraintsList = new ArrayList<CarbondataColumnConstraint>();
        constraintsList.add(constraints);
        queryModel = new QueryModel();
        new MockUp<BlockExecutionInfo>() {
            @Mock
            public QueryDimension[] getQueryDimensions() {
                QueryDimension queryDimension = new QueryDimension("emp");
                QueryDimension[] queryDimensionList = new QueryDimension[]{queryDimension};
                return queryDimensionList;

            }
        };

        split = new CarbondataSplit("conid", schemaTable, domain, localSplits, constraintsList);
        carbonTableIdentifier = new CarbonTableIdentifier("default", "emp", "1");
        queryModel
                .setAbsoluteTableIdentifier(new AbsoluteTableIdentifier("/default", carbonTableIdentifier));

        new MockUp<AbstractDetailQueryResultIterator>() {
            @Mock
            private void intialiseInfos() {

            }

        };
        queryModel.setVectorReader(true);

        //case when type is not specified
        carbondataRecordSet =
                new CarbondataRecordSet(carbonTable, null, split, new ArrayList<CarbondataColumnHandle>(),
                        queryModel, null);
        new MockUp<AbstractDetailQueryResultIterator>() {
            @Mock
            void initQueryStatiticsModel() {

            }
        };


        new MockUp<QueryExecutorFactory>() {
            @Mock
            public QueryExecutor getQueryExecutor(QueryModel queryModel) {
                return new VectorDetailQueryExecutor();
            }
        };
        new MockUp<VectorDetailQueryExecutor>() {
            @Mock
            public CarbonIterator<Object> execute(QueryModel queryModel) {
               return new VectorDetailQueryResultIterator(null, queryModel, new DefaultEventExecutorGroup(2));
            }
        };


        new MockUp<CarbondataRecordSet>() {
            @Mock
            public RecordCursor cursor() throws IOException, QueryExecutionException {
                QueryExecutor queryExecutor = QueryExecutorFactory.getQueryExecutor(queryModel);
                CarbonIterator iterator = queryExecutor.execute(queryModel);
                CarbondataColumnHandle carbondataColumnHandle =
                        new CarbondataColumnHandle("connectorId", "id", IntegerType.INTEGER,
                                0, 3, 1, true, 1, "char", true, 5,
                                4);
                List<CarbondataColumnHandle> carbonColumnHandles = new ArrayList<>();
                carbonColumnHandles.add(carbondataColumnHandle);
                carbonColumnHandles.add(carbondataColumnHandle);

                CarbonVectorizedRecordReader carbonVectorizedRecordReader = new CarbonVectorizedRecordReader(queryExecutor,
                        queryModel, (AbstractDetailQueryResultIterator) iterator);
                return new CarbondataRecordCursor(new CarbonDictionaryDecodeReadSupport(),
                        carbonVectorizedRecordReader, carbonColumnHandles, split);
            }
        };

        new MockUp<CarbondataRecordSet>() {
            @Mock
            public List<Type> getColumnTypes() {
                List<Type> types = new ArrayList<>();
                types.add(IntegerType.INTEGER);
                types.add(IntegerType.INTEGER);
                return types;
            }
        };
        new MockUp<CarbonDictionaryDecodeReadSupport>() {
            @Mock
            public SliceArrayBlock getSliceArrayBlock(int columnNo) {
                return new SliceArrayBlock(2,
                        new Slice[]{Slices.utf8Slice("123"), Slices.utf8Slice("456")});
            }
        };
        carbonPage = new CarbondataPageSource(carbondataRecordSet);
    }


    @AfterClass
    public static void tearDown() {
        carbonPage.close();
    }

    @Test()
    public void testEmptyColumn() {

        new MockUp<CarbonVectorizedRecordReader>() {
            @Mock
            public boolean nextKeyValue() throws IOException, InterruptedException {
                return true;
            }
        };
        new MockUp<CarbonVectorizedRecordReader>() {
            @Mock
            public Object getCurrentValue() throws IOException, InterruptedException {
                StructField structField = new StructField("column1", DataTypes.IntegerType, false, null);

                ColumnarBatch columnarBatch = ColumnarBatch.allocate(new StructType(new StructField[]{structField, structField}),
                        MemoryMode.ON_HEAP, 5);
                columnarBatch.setNumRows(2);
                return columnarBatch;

            }
        };

        assertNotNull(carbonPage.getNextPage());
    }

    @Test(expected = PrestoException.class)
    public void prestoExceptionTest() {
        new MockUp<CarbonVectorizedRecordReader>() {
            @Mock
            public void close() throws IOException {
            }
        };
        new MockUp<CarbondataRecordCursor>() {
            @Mock
            public void close() throws IOException {

            }
        };
        new MockUp<CarbonVectorizedRecordReader>() {
            @Mock
            public boolean nextKeyValue() throws IOException, InterruptedException {
                throw new PrestoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, "unable to fetch row");
            }
        };
        carbonPage.getNextPage();
    }

    @Test(expected = RuntimeException.class)
    public void runTimeExceptionTest() {
        new MockUp<BatchResult>() {
            @Mock
            public int getSize() {
                return 1;
            }
        };
        carbonPage.getNextPage();
    }

    @Test
    public void testGetCompletedBytes() {
        assertEquals(0, carbonPage.getCompletedBytes());
    }

    @Test
    public void testGetTotalBytes() {
        assertEquals(5, carbonPage.getTotalBytes());
    }

    @Test
    public void testIsFinished() {
        assertEquals(true, carbonPage.isFinished());
    }
}
