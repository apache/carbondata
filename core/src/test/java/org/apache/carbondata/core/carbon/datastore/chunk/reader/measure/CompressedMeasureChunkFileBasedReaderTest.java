package org.apache.carbondata.core.carbon.datastore.chunk.reader.measure;

import static junit.framework.TestCase.assertEquals;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import mockit.Mock;
import mockit.MockUp;

import org.apache.carbondata.core.carbon.datastore.chunk.MeasureColumnDataChunk;
import org.apache.carbondata.core.carbon.datastore.chunk.reader.measure.v1.CompressedMeasureChunkFileBasedReaderV1;
import org.apache.carbondata.core.carbon.metadata.blocklet.BlockletInfo;
import org.apache.carbondata.core.carbon.metadata.blocklet.datachunk.DataChunk;
import org.apache.carbondata.core.datastorage.store.FileHolder;
import org.apache.carbondata.core.datastorage.store.compression.MeasureMetaDataModel;
import org.apache.carbondata.core.datastorage.store.compression.WriterCompressModel;
import org.apache.carbondata.core.datastorage.store.dataholder.CarbonWriteDataHolder;
import org.apache.carbondata.core.datastorage.util.StoreFactory;
import org.apache.carbondata.core.metadata.ValueEncoderMeta;
import org.apache.carbondata.core.util.ValueCompressionUtil;
import org.junit.BeforeClass;
import org.junit.Test;

public class CompressedMeasureChunkFileBasedReaderTest {

  static CompressedMeasureChunkFileBasedReaderV1 compressedMeasureChunkFileBasedReader;
  static CarbonWriteDataHolder[] dataHolder = new CarbonWriteDataHolder[1];

  static WriterCompressModel writerCompressModel;
  @BeforeClass public static void setup() {
    List<DataChunk> dataChunkList = new ArrayList<>();
    dataChunkList.add(new DataChunk());

    writerCompressModel = new WriterCompressModel();
    Object maxValue[] = new Object[]{new Long[]{8L, 0L}};
    Object minValue[] = new Object[]{new Long[]{1L,0L}};
    byte[] dataTypeSelected = new byte[1];
    char[] aggType = new char[]{'b'};
    MeasureMetaDataModel measureMDMdl =
                new MeasureMetaDataModel(minValue, maxValue, new int[]{1}, maxValue.length, null,
                    aggType, dataTypeSelected);
    writerCompressModel = ValueCompressionUtil.getWriterCompressModel(measureMDMdl);
    

    ValueEncoderMeta meta = new ValueEncoderMeta();
    meta.setMaxValue(new Long[]{8L,0L});
    meta.setMinValue(new Long[]{1L,0L});
    meta.setMantissa(1);
    meta.setType('b');
    List<ValueEncoderMeta> valueEncoderMetaList = new ArrayList<>();
    valueEncoderMetaList.add(meta);
    dataChunkList.get(0).setValueEncoderMeta(valueEncoderMetaList);
    BlockletInfo info = new BlockletInfo();
    info.setMeasureColumnChunk(dataChunkList);
    compressedMeasureChunkFileBasedReader =
        new CompressedMeasureChunkFileBasedReaderV1(info, "filePath");
  }

  @Test public void readMeasureChunkTest() {
    FileHolder fileHolder = new MockUp<FileHolder>() {
      @Mock public byte[] readByteArray(String filePath, long offset, int length) {
        dataHolder[0] = new CarbonWriteDataHolder();
        dataHolder[0].initialiseBigDecimalValues(1);
        dataHolder[0].setWritableBigDecimalValueByIndex(0, new long[]{2L,1L});
        byte[][] writableMeasureDataArray =
            StoreFactory.createDataStore(writerCompressModel).getWritableMeasureDataArray(dataHolder)
                .clone();
        return writableMeasureDataArray[0];
      }
    }.getMockInstance();

    MeasureColumnDataChunk measureColumnDataChunks =
        compressedMeasureChunkFileBasedReader.readMeasureChunk(fileHolder, 0);

    BigDecimal bigD = new BigDecimal("2.1");
    assertEquals(bigD,
        measureColumnDataChunks.getMeasureDataHolder().getReadableBigDecimalValueByIndex(0));
      
  }

  @Test public void readMeasureChunksTest() {
    FileHolder fileHolder = new MockUp<FileHolder>() {
      @Mock public byte[] readByteArray(String filePath, long offset, int length) {
        dataHolder[0] = new CarbonWriteDataHolder();
        dataHolder[0].initialiseBigDecimalValues(1);
        dataHolder[0].setWritableBigDecimalValueByIndex(0, new long[]{2L,1L});
        byte[][] writableMeasureDataArray =
            StoreFactory.createDataStore(writerCompressModel).getWritableMeasureDataArray(dataHolder)
                .clone();
        return writableMeasureDataArray[0];
      }
    }.getMockInstance();

    int[][] blockIndexes = {{0,0}};
    MeasureColumnDataChunk measureColumnDataChunks[] =
        compressedMeasureChunkFileBasedReader.readMeasureChunks(fileHolder, blockIndexes);

    BigDecimal bigD = new BigDecimal("2.1");
    assertEquals(bigD,
        measureColumnDataChunks[0].getMeasureDataHolder().getReadableBigDecimalValueByIndex(0));

  }
}