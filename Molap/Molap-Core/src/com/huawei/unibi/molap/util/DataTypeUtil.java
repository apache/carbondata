package com.huawei.unibi.molap.util;

import java.math.BigDecimal;
import java.math.BigInteger;

import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.olap.SqlStatement.Type;

public final class DataTypeUtil {
    private DataTypeUtil() {

    }

    public static Object getMeasureValueBasedOnDataType(String msrValue, String dataType) {
        switch (dataType) {
        case "Decimal":
            return new java.math.BigDecimal(msrValue);
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
            return MolapCommonConstants.BIG_DECIMAL_MEASURE;
        case "BigInt":
        case "Long":
            return MolapCommonConstants.BIG_INT_MEASURE;
        default:
            return 'n';
        }
    }

    public static char getAggType(Type dataType, String agg) {
        if (MolapCommonConstants.SUM.equals(agg) || MolapCommonConstants.COUNT.equals(agg)) {
            switch (dataType) {
            case DECIMAL:
                return MolapCommonConstants.BIG_DECIMAL_MEASURE;
            case LONG:
                return MolapCommonConstants.BIG_INT_MEASURE;
            default:
                return MolapCommonConstants.SUM_COUNT_VALUE_MEASURE;
            }
        } else {
            return MolapCommonConstants.BYTE_VALUE_MEASURE;
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

}
