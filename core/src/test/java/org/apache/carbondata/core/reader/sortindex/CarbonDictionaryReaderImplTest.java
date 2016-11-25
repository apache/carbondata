package org.apache.carbondata.core.reader.sortindex;

import mockit.Mock;
import mockit.MockUp;
import org.apache.carbondata.common.ext.PathFactory;
import org.apache.carbondata.common.factory.CarbonCommonFactory;
import org.apache.carbondata.core.carbon.CarbonTableIdentifier;
import org.apache.carbondata.core.carbon.ColumnIdentifier;
import org.apache.carbondata.core.reader.CarbonDictionaryColumnMetaChunk;
import org.apache.carbondata.core.reader.CarbonDictionaryMetadataReaderImpl;
import org.apache.carbondata.core.reader.CarbonDictionaryReaderImpl;
import org.apache.carbondata.core.reader.ThriftReader;
import org.apache.carbondata.core.service.PathService;
import org.apache.thrift.TBase;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertTrue;

public class CarbonDictionaryReaderImplTest {
    private static CarbonTableIdentifier carbonTableIdentifier = null;
    private static ColumnIdentifier columnIdentifier = null;

    private static CarbonDictionaryReaderImpl carbonDictionaryReaderImpl = null;


    @BeforeClass
    public static void setUp() throws Exception {
        columnIdentifier = new ColumnIdentifier("1", null, null);
        carbonTableIdentifier = new CarbonTableIdentifier("dbName", "tableName", UUID.randomUUID().toString());
        carbonDictionaryReaderImpl = new CarbonDictionaryReaderImpl("storePath", carbonTableIdentifier, columnIdentifier);
    }

    @Test
    public void testRead() throws Exception {
        new MockUp<CarbonCommonFactory>() {
            @Mock
            public PathService getPathService() {

                return new PathFactory();
            }
        };
        new MockUp<CarbonDictionaryMetadataReaderImpl>() {
            @Mock
            public List<CarbonDictionaryColumnMetaChunk> read() throws IOException {
                CarbonDictionaryColumnMetaChunk carbonDictionaryColumnMetaChunks = new CarbonDictionaryColumnMetaChunk(1, 8, 60L, 120L, 20);
                List<CarbonDictionaryColumnMetaChunk> carbonDictionaryColumnMetaChunksList = new ArrayList<CarbonDictionaryColumnMetaChunk>();
                carbonDictionaryColumnMetaChunksList.add(carbonDictionaryColumnMetaChunks);
                return carbonDictionaryColumnMetaChunksList;
            }

        };

        new MockUp<ThriftReader>() {
            @Mock
            public void setReadOffset(long bytesToSkip) throws IOException {

            }

        };
        new MockUp<ThriftReader>() {
            @Mock
            public void open() throws IOException {
            }

        };
        new MockUp<ThriftReader>() {
            @Mock
            public boolean hasNext() throws IOException {
                return true;
            }

        };
        new MockUp<CarbonDictionaryColumnMetaChunk>() {
            @Mock
            public long getStart_offset() {
                return 0L;
            }
        };
        new MockUp<ThriftReader>() {
            @Mock
            public TBase read() throws IOException {
                List<ByteBuffer> byteBufferList = new ArrayList<ByteBuffer>();
                ByteBuffer buf = ByteBuffer.allocate(2);
                byteBufferList.add(buf);
                return new org.apache.carbondata.format.ColumnDictionaryChunk(byteBufferList);

            }

        };

        int result = carbonDictionaryReaderImpl.read().size();
        assertTrue(result == 20);

    }


}
