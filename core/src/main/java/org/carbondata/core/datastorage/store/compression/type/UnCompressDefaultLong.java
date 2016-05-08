package org.carbondata.core.datastorage.store.compression.type;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.datastorage.store.compression.ValueCompressonHolder;
import org.carbondata.core.datastorage.store.dataholder.CarbonReadDataHolder;

public class UnCompressDefaultLong extends UnCompressNoneLong {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(UnCompressDefaultLong.class.getName());

  public ValueCompressonHolder.UnCompressValue getNew() {
    try {
      return (ValueCompressonHolder.UnCompressValue) clone();
    } catch (CloneNotSupportedException clnNotSupportedExc) {
      LOGGER.error(clnNotSupportedExc,
          clnNotSupportedExc.getMessage());
    }
    return null;
  }

  @Override public CarbonReadDataHolder getValues(int decimal, Object maxValueObject) {
    CarbonReadDataHolder dataHolder = new CarbonReadDataHolder();
    long[] vals = new long[value.length];
    for (int i = 0; i < vals.length; i++) {
      vals[i] = value[i];
    }
    dataHolder.setReadableLongValues(vals);
    return dataHolder;
  }

}
