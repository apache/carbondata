package org.apache.carbondata.core.carbon.datastore.chunk.reader.measure;

import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.carbon.datastore.chunk.MeasureColumnDataChunk;
import org.apache.carbondata.core.carbon.metadata.blocklet.datachunk.DataChunk;
import org.apache.carbondata.core.datastorage.store.FileHolder;
import org.apache.carbondata.core.datastorage.store.compression.ValueCompressionModel;
import org.apache.carbondata.core.datastorage.store.compression.ValueCompressonHolder;
import org.apache.carbondata.core.datastorage.store.compression.type.UnCompressByteArray;
import org.apache.carbondata.core.datastorage.store.dataholder.CarbonReadDataHolder;
import org.apache.carbondata.core.util.ValueCompressionUtil;

import mockit.Mock;
import mockit.MockUp;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

public class CompressedMeasureChunkFileBasedReaderTest {

  static CompressedMeasureChunkFileBasedReader compressedMeasureChunkFileBasedReader;

  @BeforeClass public static void setup() {
    List<DataChunk> dataChunkList = new ArrayList<>();
    dataChunkList.add(new DataChunk());

    ValueCompressionModel valueCompressionModel = new ValueCompressionModel();

    ValueCompressonHolder.UnCompressValue unCompressValue[] =
        { new UnCompressByteArray(UnCompressByteArray.ByteArrayType.BYTE_ARRAY) };
    byte valueInByte[] = { 1, 5, 4, 8, 7 };
    unCompressValue[0].setValueInBytes(valueInByte);
    ValueCompressionUtil.DataType dataType[] = { ValueCompressionUtil.DataType.DATA_BYTE };

    valueCompressionModel.setUnCompressValues(unCompressValue);
    valueCompressionModel.setChangedDataType(dataType);
    int decimal[] = { 5, 8, 2 };
    valueCompressionModel.setDecimal(decimal);
    Object maxValue[] = { 8 };
    valueCompressionModel.setMaxValue(maxValue);

    compressedMeasureChunkFileBasedReader =
        new CompressedMeasureChunkFileBasedReader(dataChunkList, valueCompressionModel, "filePath");
  }

  @Test public void readMeasureChunkTest() {
    FileHolder fileHolder = new MockUp<FileHolder>() {
      @Mock public byte[] readByteArray(String filePath, long offset, int length) {
        byte mockedValue[] = { 1, 5, 4, 8, 7 };
        return mockedValue;
      }
    }.getMockInstance();

    new MockUp<UnCompressByteArray>() {
      @Mock public CarbonReadDataHolder getValues(int decimal, Object maxValueObject) {
        List<byte[]> valsList = new ArrayList<byte[]>();
        byte mockedValue[] = { 3, 7, 9 };
        valsList.add(mockedValue);
        CarbonReadDataHolder holder = new CarbonReadDataHolder();
        byte[][] value = new byte[valsList.size()][];
        valsList.toArray(value);
        holder.setReadableByteValues(value);
        return holder;
      }
    };

    MeasureColumnDataChunk measureColumnDataChunks =
        compressedMeasureChunkFileBasedReader.readMeasureChunk(fileHolder, 0);

    byte expectedValue[] = { 3, 7, 9 };
    for (int i = 0; i < 3; i++) {
      assertEquals(expectedValue[i],
          measureColumnDataChunks.getMeasureDataHolder().getReadableByteArrayValueByIndex(0)[i]);
    }
  }

  @Test public void readMeasureChunksTest() {
    FileHolder fileHolder = new MockUp<FileHolder>() {
      @Mock public byte[] readByteArray(String filePath, long offset, int length) {
        byte mockedValue[] = { 1, 5, 4, 8, 7 };
        return mockedValue;
      }
    }.getMockInstance();

    new MockUp<UnCompressByteArray>() {
      @Mock public CarbonReadDataHolder getValues(int decimal, Object maxValueObject) {
        List<byte[]> valsList = new ArrayList<byte[]>();
        byte mockedValue[] = { 3, 7, 9 };
        valsList.add(mockedValue);
        CarbonReadDataHolder holder = new CarbonReadDataHolder();
        byte[][] value = new byte[valsList.size()][];
        valsList.toArray(value);
        holder.setReadableByteValues(value);
        return holder;
      }
    };

    int blockIndexes[] = { 0 };
    MeasureColumnDataChunk measureColumnDataChunks[] =
        compressedMeasureChunkFileBasedReader.readMeasureChunks(fileHolder, blockIndexes);

    byte expectedValue[] = { 3, 7, 9 };
    for (int i = 0; i < 3; i++) {
      assertEquals(expectedValue[i],
          measureColumnDataChunks[0].getMeasureDataHolder().getReadableByteArrayValueByIndex(0)[i]);
    }
  }
}