package org.carbondata.core.datastorage.store.compression.type;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.datastorage.store.compression.Compressor;
import org.carbondata.core.datastorage.store.compression.SnappyCompression;
import org.carbondata.core.datastorage.store.compression.ValueCompressonHolder;
import org.carbondata.core.datastorage.store.dataholder.CarbonReadDataHolder;
import org.carbondata.core.util.ValueCompressionUtil;

public class UnCompressMaxMinByteForLong extends UnCompressMaxMinByte {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(UnCompressMaxMinByteForLong.class.getName());
  private static Compressor<byte[]> byteCompressor =
      SnappyCompression.SnappyByteCompression.INSTANCE;

  @Override public ValueCompressonHolder.UnCompressValue getNew() {
    try {
      return (ValueCompressonHolder.UnCompressValue) clone();
    } catch (CloneNotSupportedException e) {
      LOGGER.error(e, e.getMessage());
    }
    return null;
  }

  @Override public ValueCompressonHolder.UnCompressValue compress() {

    UnCompressMaxMinByteForLong byte1 = new UnCompressMaxMinByteForLong();
    byte1.setValue(byteCompressor.compress(value));
    return byte1;
  }

  @Override
  public ValueCompressonHolder.UnCompressValue uncompress(ValueCompressionUtil.DataType dataType) {
    ValueCompressonHolder.UnCompressValue byte1 =
        ValueCompressionUtil.unCompressMaxMin(dataType, dataType);
    ValueCompressonHolder.unCompress(dataType, byte1, value);
    return byte1;
  }

  @Override public ValueCompressonHolder.UnCompressValue getCompressorObject() {
    return new UnCompressMaxMinByteForLong();
  }

  @Override public CarbonReadDataHolder getValues(int decimal, Object maxValueObject) {
    long maxValue = (long) maxValueObject;
    long[] vals = new long[value.length];
    CarbonReadDataHolder dataHolder = new CarbonReadDataHolder();
    for (int i = 0; i < vals.length; i++) {
      if (value[i] == 0) {
        vals[i] = maxValue;
      } else {
        vals[i] = maxValue - value[i];
      }
    }
    dataHolder.setReadableLongValues(vals);
    return dataHolder;
  }
}
