package org.apache.carbondata.core.reader.sortindex;

import mockit.Mock;
import mockit.MockUp;
import org.apache.carbondata.core.reader.CarbonIndexFileReader;
import org.apache.carbondata.core.reader.ThriftReader;
import org.apache.carbondata.format.IndexHeader;
import org.apache.thrift.TBase;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class CarbonIndexFileReaderTest {
    private static CarbonIndexFileReader carbonIndexFileReader = null;

    @BeforeClass
    public static void setUp() throws IOException {
        carbonIndexFileReader = new CarbonIndexFileReader();
        new MockUp<ThriftReader>() {
            @Mock
            public void open() throws IOException {


            }

        };
        carbonIndexFileReader.openThriftReader("TestFile.Carbon");
    }

    @AfterClass
    public static void cleanUp() {
        carbonIndexFileReader.closeThriftReader();
    }

    @Test
    public void testreadIndexHeader() throws IOException {
        new MockUp<ThriftReader>() {
            @Mock
            public TBase read(ThriftReader.TBaseCreator creator) throws IOException {
                return new IndexHeader();

            }

        };

        Assert.assertTrue(carbonIndexFileReader.readIndexHeader() != null);
    }

    @Test
    public void testHasNext() throws IOException {
        new MockUp<ThriftReader>() {
            @Mock
            public boolean hasNext() throws IOException {

                return true;

            }

        };
        Assert.assertTrue(carbonIndexFileReader.hasNext());
    }

    @Test
    public void testReadBlockInfo() throws IOException {
        new MockUp<ThriftReader>() {
            @Mock
            public TBase read(ThriftReader.TBaseCreator creator) throws IOException {
                return new org.apache.carbondata.format.BlockIndex();

            }

        };
        Assert.assertTrue(carbonIndexFileReader.readBlockIndexInfo() != null);
    }
}
