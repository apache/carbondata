package org.apache.carbondata.scan.result.vector;

import org.apache.spark.sql.types.Decimal;

public interface CarbonColumnVector {

  public void putShort(int rowId, short value);

  public void putInt(int rowId, int value);

  public void putLong(int rowId, long value);

  public void putDecimal(int rowId, Decimal value, int precision);

  public void putBytes(int rowId, byte[] value);

  public void putBytes(int rowId, int offset, int length, byte[] value);

}
