package org.carbondata.core.datastorage.store.compression.type;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.datastorage.store.compression.Compressor;
import org.carbondata.core.datastorage.store.compression.SnappyCompression;
import org.carbondata.core.datastorage.store.compression.ValueCompressonHolder;
import org.carbondata.core.datastorage.store.dataholder.CarbonReadDataHolder;
import org.carbondata.core.util.ValueCompressionUtil;

public class UnCompressMaxMinDefaultLong extends UnCompressMaxMinLong {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(UnCompressMaxMinDefaultLong.class.getName());
  private static Compressor<long[]> longCompressor =
      SnappyCompression.SnappyLongCompression.INSTANCE;

  @Override public ValueCompressonHolder.UnCompressValue getNew() {
    try {
      return (ValueCompressonHolder.UnCompressValue) clone();
    } catch (CloneNotSupportedException ex5) {
      LOGGER.error(ex5, ex5.getMessage());
    }
    return null;
  }

  @Override public ValueCompressonHolder.UnCompressValue compress() {
    UnCompressMaxMinByteForLong byte1 = new UnCompressMaxMinByteForLong();
    byte1.setValue(longCompressor.compress(value));
    return byte1;
  }

  @Override public byte[] getBackArrayData() {
    return ValueCompressionUtil.convertToBytes(value);
  }

  @Override public ValueCompressonHolder.UnCompressValue getCompressorObject() {
    return new UnCompressMaxMinByteForLong();
  }

  @Override public CarbonReadDataHolder getValues(int decimal, Object maxValueObject) {
    long maxValue = (long) maxValueObject;
    long[] vals = new long[value.length];
    CarbonReadDataHolder dataHolderInfoObj = new CarbonReadDataHolder();
    for (int i = 0; i < vals.length; i++) {
      if (value[i] == 0) {
        vals[i] = maxValue;
      } else {
        vals[i] = maxValue - value[i];
      }

    }
    dataHolderInfoObj.setReadableLongValues(vals);
    return dataHolderInfoObj;
  }

}
