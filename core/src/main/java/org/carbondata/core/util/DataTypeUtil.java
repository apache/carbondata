package org.carbondata.core.util;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.carbondata.core.carbon.SqlStatement;
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

  public static char getAggType(SqlStatement.Type dataType, String agg) {
    if (CarbonCommonConstants.SUM.equals(agg) || CarbonCommonConstants.COUNT.equals(agg)) {
      switch (dataType) {
        case DECIMAL:
          return CarbonCommonConstants.BIG_DECIMAL_MEASURE;
        case LONG:
          return CarbonCommonConstants.BIG_INT_MEASURE;
        default:
          return CarbonCommonConstants.SUM_COUNT_VALUE_MEASURE;
      }
    } else {
      return CarbonCommonConstants.BYTE_VALUE_MEASURE;
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
  public static SqlStatement.Type getDataType(String dataTypeStr) {
    SqlStatement.Type dataType = null;
    switch (dataTypeStr) {
      case "TIMESTAMP":
        dataType = SqlStatement.Type.TIMESTAMP;
        break;
      case "STRING":
        dataType = SqlStatement.Type.STRING;
        break;
      case "INT":
        dataType = SqlStatement.Type.INT;
        break;
      case "LONG":
        dataType = SqlStatement.Type.LONG;
        break;
      case "DOUBLE":
        dataType = SqlStatement.Type.DOUBLE;
        break;
      case "DECIMAL":
        dataType = SqlStatement.Type.DECIMAL;
        break;
      case "ARRAY":
        dataType = SqlStatement.Type.ARRAY;
        break;
      case "STRUCT":
        dataType = SqlStatement.Type.STRUCT;
        break;
      case "MAP":
      default:
        dataType = SqlStatement.Type.STRING;
    }
    return dataType;
  }
}
