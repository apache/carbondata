package org.carbondata.core.util;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.carbondata.core.carbon.metadata.datatype.DataType;
import org.carbondata.core.constants.CarbonCommonConstants;

public final class DataTypeUtil {
  private DataTypeUtil() {

  }

  public static Object getMeasureValueBasedOnDataType(String msrValue, String dataType) {
    switch (dataType) {
      case "Decimal":
        return new BigDecimal(msrValue);
      case "BigInt":
      case "Long":
        return Long.valueOf(msrValue);
      default:
        return Double.valueOf(msrValue);
    }
  }

  public static char getAggType(String dataType) {
    switch (dataType) {
      case "Decimal":
        return CarbonCommonConstants.BIG_DECIMAL_MEASURE;
      case "BigInt":
      case "Long":
        return CarbonCommonConstants.BIG_INT_MEASURE;
      default:
        return 'n';
    }
  }

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
