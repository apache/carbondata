package org.apache.carbondata.core.carbon.datastore.chunk.reader.measure;


import mockit.Mock;
import mockit.MockUp;
import org.apache.carbondata.core.carbon.datastore.chunk.MeasureColumnDataChunk;
import org.apache.carbondata.core.carbon.metadata.blocklet.datachunk.DataChunk;
import org.apache.carbondata.core.datastorage.store.FileHolder;
import org.apache.carbondata.core.datastorage.store.compression.ValueCompressionModel;
import org.apache.carbondata.core.datastorage.store.compression.ValueCompressonHolder;
import org.apache.carbondata.core.datastorage.store.compression.type.UnCompressByteArray;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class CompressedMeasureChunkFileBasedReaderTest {

    static CompressedMeasureChunkFileBasedReader compressedMeasureChunkFileBasedReader;

    @BeforeClass
    public static void setup() {
        List<DataChunk> dataChunkList = new ArrayList<>();
        dataChunkList.add(new DataChunk());
        ValueCompressionModel valueCompressionModel = new ValueCompressionModel();

        ValueCompressonHolder.UnCompressValue unCompressValue[] = {new UnCompressByteArray(UnCompressByteArray.ByteArrayType.BYTE_ARRAY)};
        byte valueInByte[] = {1, 5, 4, 8, 7};
        unCompressValue[0].setValueInBytes(valueInByte);

        valueCompressionModel.setUnCompressValues(unCompressValue);
        compressedMeasureChunkFileBasedReader = new CompressedMeasureChunkFileBasedReader(dataChunkList,valueCompressionModel,"filePath");
    }

   /* @Test
    public void readMeasureChunksTest(){
        FileHolder fileHolder = new MockUp<FileHolder>() {
            @Mock
            public byte[] readByteArray(String filePath, long offset, int length) {
                byte mockedValue[] = {1, 5, 4, 8, 7};
                return mockedValue;
            }
        }.getMockInstance();

      *//*  new MockUp<ValueCompressonHolder.UnCompressValue>() {
            @Mock
            public byte[] readByteArray(String filePath, long offset, int length) {
                System.out.println("readByteArray");
                byte mockedValue[] = {1, 5, 4, 8, 7};
                return mockedValue;
            }
        };*//*

        int blockIndexes[] = {0};
        MeasureColumnDataChunk measureColumnDataChunks[] = compressedMeasureChunkFileBasedReader.readMeasureChunks(fileHolder,blockIndexes);
        System.out.println(measureColumnDataChunks.length);
    }*/
}
