package org.apache.carbondata.presto;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import mockit.Mock;
import mockit.MockUp;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.metadata.schema.table.TableSchema;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.presto.impl.CarbonLocalInputSplit;
import org.apache.carbondata.presto.impl.CarbonTableCacheModel;
import org.apache.carbondata.presto.impl.CarbonTableConfig;
import org.apache.carbondata.presto.impl.CarbonTableReader;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static junit.framework.TestCase.assertTrue;

public class CarbonDataRecordSetProviderTest {
    public static final ConnectorSession SESSION = new TestingConnectorSession(ImmutableList.of());
    CarbonTableConfig tableConfig = new CarbonTableConfig();
    CarbonTableReader carbonTableReader = new CarbonTableReader(tableConfig);
    ColumnSchema columnSchema = new ColumnSchema();
    List<CarbondataColumnHandle> carbonColumnHandles = new ArrayList<>();
    CarbondataSplit split;
    CarbondataRecordSetProvider carbonRecordSetProvider;

    private ColumnSchema getColumnarDimensionColumn() {
        ColumnSchema dimColumn = new ColumnSchema();
        dimColumn.setColumnar(true);
        dimColumn.setColumnName("imei");
        dimColumn.setColumnUniqueId(UUID.randomUUID().toString());
        dimColumn.setDataType(DataType.STRING);
        dimColumn.setDimensionColumn(true);
        List<Encoding> encodeList =
                new ArrayList<Encoding>(Integer.parseInt("3"));
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

    private TableSchema getTableSchema() {
        TableSchema tableSchema = new TableSchema();
        List<ColumnSchema> columnSchemaList = new ArrayList<ColumnSchema>();
        columnSchemaList.add(getColumnarMeasureColumn());
        columnSchemaList.add(getColumnarDimensionColumn());
        tableSchema.setListOfColumns(columnSchemaList);
        tableSchema.setTableName("table1");
        return tableSchema;
    }

    private TableInfo getTableInfo(long timeStamp) {
        TableInfo info = new TableInfo();
        info.setDatabaseName("schema1");
        info.setLastUpdatedTime(timeStamp);
        info.setTableUniqueName("schema1_tableName");
        info.setFactTable(getTableSchema());
        info.setStorePath("storePath");
        return info;
    }

    @Test
    public void testGetRecordSet() {
        tableConfig.setDbPath("DbPath");
        tableConfig.setStorePath("StorePath");
        tableConfig.setTablePath("tablePath");
        carbonRecordSetProvider = new CarbondataRecordSetProvider(new CarbondataConnectorId("fakeConnector"), carbonTableReader);
        new MockUp<CarbondataMetadata>() {
            @Mock
            Type carbonDataType2SpiMapper(ColumnSchema columnSchema) {
                return IntegerType.INTEGER;
            }
        };
        Type spiType = CarbondataMetadata.carbonDataType2SpiMapper(columnSchema);
        CarbondataColumnHandle carbondataColumnHandle =
                new CarbondataColumnHandle("connectorId", "id", spiType, 0, 3, 1, true, 1, "char", true, 5,
                        4);
        carbonColumnHandles.add(carbondataColumnHandle);
        carbonColumnHandles.add(carbondataColumnHandle);

        SchemaTableName schemaTable = new SchemaTableName("schemaName", "tableName");
        TupleDomain<ColumnHandle> domain = TupleDomain.all();
        CarbonLocalInputSplit localSplits = new CarbonLocalInputSplit("segmentId", "path", 0, 5
                , new ArrayList<String>(), 5, Short.MAX_VALUE, new String[]{"d1","d2"}, "detailInfo");
        Optional<Domain> domainAll = Optional.of(Domain.all(IntegerType.INTEGER));
        CarbondataColumnConstraint constraints = new CarbondataColumnConstraint("", domainAll, false);
        List constraintsList = new ArrayList<CarbondataColumnConstraint>();
        constraintsList.add(constraints);
        split = new CarbondataSplit("fakeConnector", schemaTable, domain, localSplits, constraintsList);
        CarbonTable carbonTable = CarbonTable.buildFromTableInfo(getTableInfo(445));
        new MockUp<CarbonTableReader>() {
            private ConcurrentHashMap<SchemaTableName, CarbonTableCacheModel> cc;

            @Mock
            private CarbonTable parseCarbonMetadata(SchemaTableName table) {
                return carbonTable;
            }

        };
        new MockUp<CarbonTableReader>() {
            @Mock
            public CarbonTableCacheModel getCarbonCache(SchemaTableName table) {
                ConcurrentHashMap<SchemaTableName, CarbonTableCacheModel> cc = new ConcurrentHashMap<>();
                CarbonTableCacheModel cache = cc.getOrDefault(schemaTable, new CarbonTableCacheModel());
                cache.carbonTable = CarbonTable.buildFromTableInfo(getTableInfo(5552));
                cache.tableInfo = new TableInfo();
                return cache;
            }
        };

        RecordSet rs = carbonRecordSetProvider.getRecordSet(CarbondataTransactionHandle.INSTANCE, SESSION, split, carbonColumnHandles);

        assertTrue(rs instanceof CarbondataRecordSet);
    }

}