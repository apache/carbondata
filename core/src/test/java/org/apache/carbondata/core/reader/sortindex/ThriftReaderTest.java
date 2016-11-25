package org.apache.carbondata.core.reader.sortindex;

import mockit.Mock;
import mockit.MockUp;
import org.apache.carbondata.core.reader.ThriftReader;
import org.apache.carbondata.format.ColumnDictionaryChunkMeta;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertTrue;

public class ThriftReaderTest {

    private static ThriftReader thriftReader = null;

    @BeforeClass
    public static void setup() {
        thriftReader = new ThriftReader("TestFile.carbon");
    }

    @Test(expected = java.io.IOException.class)
    public void testReadForException() throws IOException {
        ThriftReader.TBaseCreator tBaseCreator = new ThriftReader.TBaseCreator() {
            @Override
            public TBase create() {
                return new ColumnDictionaryChunkMeta();
            }

        };
        new MockUp<ColumnDictionaryChunkMeta>() {
            @Mock
            public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
                throw new TException("TException Occur");
            }


        };
        thriftReader = new ThriftReader("TestFile.carbon", tBaseCreator);
        thriftReader.read();
    }

    @Test
    public void testReadWithTBaseCreator() throws IOException {
        ThriftReader.TBaseCreator tBaseCreator = new ThriftReader.TBaseCreator() {
            @Override
            public TBase create() {
                return new ColumnDictionaryChunkMeta();
            }
        };
        new MockUp<ColumnDictionaryChunkMeta>() {
            @Mock
            public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {

            }


        };
        assertTrue(thriftReader.read(tBaseCreator) != null);

    }

    @Test(expected = java.io.IOException.class)
    public void testReadWithTBaseCreatorForException() throws IOException {
        ThriftReader.TBaseCreator tBaseCreator = new ThriftReader.TBaseCreator() {
            @Override
            public TBase create() {
                return new ColumnDictionaryChunkMeta();
            }
        };
        new MockUp<ColumnDictionaryChunkMeta>() {
            @Mock
            public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
                throw new TException("TException Occur");
            }


        };
        thriftReader.read(tBaseCreator);
    }

}
