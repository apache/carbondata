package org.carbondata.core.util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

import org.carbondata.core.carbon.metadata.datatype.DataType;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonMeasure;
import org.carbondata.core.constants.CarbonCommonConstants;

public final class DataTypeUtil {

  private DataTypeUtil() {

  }

  /**
   * This method will convert a given value to its specific type
   *
   * @param msrValue
   * @param dataType
   * @param carbonMeasure
   * @return
   */
  public static Object getMeasureValueBasedOnDataType(String msrValue, DataType dataType,
      CarbonMeasure carbonMeasure) {
    switch (dataType) {
      case DECIMAL:
        BigDecimal bigDecimal =
            new BigDecimal(msrValue).setScale(carbonMeasure.getScale(), RoundingMode.HALF_UP);
        return normalizeDecimalValue(bigDecimal, carbonMeasure.getPrecision());
      case LONG:
        return Long.valueOf(msrValue);
      default:
        return Double.valueOf(msrValue);
    }
  }

  /**
   * This method will check the digits before dot with the max precision allowed
   *
   * @param bigDecimal
   * @param allowedPrecision precision configured by the user
   * @return
   */
  public static BigDecimal normalizeDecimalValue(BigDecimal bigDecimal, int allowedPrecision) {
    if (bigDecimal.precision() > allowedPrecision) {
      return null;
    }
    return bigDecimal;
  }

  /**
   * This method will return the type of measure based on its data type
   *
   * @param dataType
   * @return
   */
  public static char getAggType(DataType dataType) {
    switch (dataType) {
      case DECIMAL:
        return CarbonCommonConstants.BIG_DECIMAL_MEASURE;
      case LONG:
        return CarbonCommonConstants.BIG_INT_MEASURE;
      default:
        return CarbonCommonConstants.SUM_COUNT_VALUE_MEASURE;
    }
  }

  /**
   * This method will convert a big decimal value to bytes
   *
   * @param num
   * @return
   */
  public static byte[] bigDecimalToByte(BigDecimal num) {
    BigInteger sig = new BigInteger(num.unscaledValue().toString());
    int scale = num.scale();
    byte[] bscale = new byte[] { (byte) (scale) };
    byte[] buff = sig.toByteArray();
    byte[] completeArr = new byte[buff.length + bscale.length];
    System.arraycopy(bscale, 0, completeArr, 0, bscale.length);
    System.arraycopy(buff, 0, completeArr, bscale.length, buff.length);
    return completeArr;
  }

  /**
   * This method will convert a byte value back to big decimal value
   *
   * @param raw
   * @return
   */
  public static BigDecimal byteToBigDecimal(byte[] raw) {
    int scale = (raw[0] & 0xFF);
    byte[] unscale = new byte[raw.length - 1];
    System.arraycopy(raw, 1, unscale, 0, unscale.length);
    BigInteger sig = new BigInteger(unscale);
    return new BigDecimal(sig, scale);
  }

  /**
   * returns the SqlStatement.Type of corresponding string value
   *
   * @param dataTypeStr
   * @return return the SqlStatement.Type
   */
  public static DataType getDataType(String dataTypeStr) {
    DataType dataType = null;
    switch (dataTypeStr) {
      case "TIMESTAMP":
        dataType = DataType.TIMESTAMP;
        break;
      case "STRING":
        dataType = DataType.STRING;
        break;
      case "INT":
        dataType = DataType.INT;
        break;
      case "LONG":
        dataType = DataType.LONG;
        break;
      case "DOUBLE":
        dataType = DataType.DOUBLE;
        break;
      case "DECIMAL":
        dataType = DataType.DECIMAL;
        break;
      case "ARRAY":
        dataType = DataType.ARRAY;
        break;
      case "STRUCT":
        dataType = DataType.STRUCT;
        break;
      case "MAP":
      default:
        dataType = DataType.STRING;
    }
    return dataType;
  }
}
