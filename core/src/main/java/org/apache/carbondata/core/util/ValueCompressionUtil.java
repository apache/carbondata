/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.carbondata.core.util;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastorage.store.compression.MeasureMetaDataModel;
import org.apache.carbondata.core.datastorage.store.compression.ValueCompressionModel;
import org.apache.carbondata.core.datastorage.store.compression.ValueCompressonHolder;
import org.apache.carbondata.core.datastorage.store.compression.type.UnCompressByteArray;
import org.apache.carbondata.core.datastorage.store.compression.type.UnCompressDefaultLong;
import org.apache.carbondata.core.datastorage.store.compression.type.UnCompressMaxMinByte;
import org.apache.carbondata.core.datastorage.store.compression.type.UnCompressMaxMinDefault;
import org.apache.carbondata.core.datastorage.store.compression.type.UnCompressMaxMinFloat;
import org.apache.carbondata.core.datastorage.store.compression.type.UnCompressMaxMinInt;
import org.apache.carbondata.core.datastorage.store.compression.type.UnCompressMaxMinLong;
import org.apache.carbondata.core.datastorage.store.compression.type.UnCompressMaxMinShort;
import org.apache.carbondata.core.datastorage.store.compression.type.UnCompressNonDecimalByte;
import org.apache.carbondata.core.datastorage.store.compression.type.UnCompressNonDecimalDefault;
import org.apache.carbondata.core.datastorage.store.compression.type.UnCompressNonDecimalFloat;
import org.apache.carbondata.core.datastorage.store.compression.type.UnCompressNonDecimalInt;
import org.apache.carbondata.core.datastorage.store.compression.type.UnCompressNonDecimalLong;
import org.apache.carbondata.core.datastorage.store.compression.type.UnCompressNonDecimalMaxMinByte;
import org.apache.carbondata.core.datastorage.store.compression.type.UnCompressNonDecimalMaxMinDefault;
import org.apache.carbondata.core.datastorage.store.compression.type.UnCompressNonDecimalMaxMinFloat;
import org.apache.carbondata.core.datastorage.store.compression.type.UnCompressNonDecimalMaxMinInt;
import org.apache.carbondata.core.datastorage.store.compression.type.UnCompressNonDecimalMaxMinLong;
import org.apache.carbondata.core.datastorage.store.compression.type.UnCompressNonDecimalMaxMinShort;
import org.apache.carbondata.core.datastorage.store.compression.type.UnCompressNonDecimalShort;
import org.apache.carbondata.core.datastorage.store.compression.type.UnCompressNoneByte;
import org.apache.carbondata.core.datastorage.store.compression.type.UnCompressNoneDefault;
import org.apache.carbondata.core.datastorage.store.compression.type.UnCompressNoneFloat;
import org.apache.carbondata.core.datastorage.store.compression.type.UnCompressNoneInt;
import org.apache.carbondata.core.datastorage.store.compression.type.UnCompressNoneLong;
import org.apache.carbondata.core.datastorage.store.compression.type.UnCompressNoneShort;

public final class ValueCompressionUtil {

  /**
   * Attribute for Carbon LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(ValueCompressionUtil.class.getName());

  private ValueCompressionUtil() {

  }

  /**
   * decide actual type of value
   *
   * @param value   :the measure value
   * @param decimal :
   * @return: actual type of value
   * @see
   */
  private static DataType getDataType(double value, int decimal, byte dataTypeSelected) {
    DataType dataType = DataType.DATA_DOUBLE;
    if (decimal == 0) {
      if (value <= Byte.MAX_VALUE && value >= Byte.MIN_VALUE) {
        dataType = DataType.DATA_BYTE;
      } else if (value <= Short.MAX_VALUE && value >= Short.MIN_VALUE) {
        dataType = DataType.DATA_SHORT;
      } else if (value <= Integer.MAX_VALUE && value >= Integer.MIN_VALUE) {
        dataType = DataType.DATA_INT;
      } else if (value <= Long.MAX_VALUE && value >= Long.MIN_VALUE) {
        dataType = DataType.DATA_LONG;
      }
    } else {
      if (dataTypeSelected == 1) {
        if (value <= Float.MAX_VALUE && value >= Float.MIN_VALUE) {
          float floatValue = (float) value;
          if (floatValue - value != 0) {
            dataType = DataType.DATA_DOUBLE;

          } else {
            dataType = DataType.DATA_FLOAT;
          }
        } else if (value <= Double.MAX_VALUE && value >= Double.MIN_VALUE) {
          dataType = DataType.DATA_DOUBLE;
        }
      }
    }
    return dataType;
  }

  /**
   * Gives the size of datatype
   *
   * @param dataType : measure value type
   * @return: the size of DataType
   * @see
   */
  public static int getSize(DataType dataType) {

    switch (dataType) {
      case DATA_BYTE:
        return 1;
      case DATA_SHORT:
        return 2;
      case DATA_INT:
      case DATA_FLOAT:
        return 4;
      default:
        return 8;
    }
  }

  /**
   * get the best compression type. priority list,from high to low:
   * COMPRESSION_TYPE.NONE COMPRESSION_TYPE.MAX_MIN
   * COMPRESSION_TYPE.NON_DECIMAL_CONVERT COMPRESSION_TYPE.MAX_MIN_NDC
   *
   * @param maxValue : max value of one measure
   * @param minValue : min value of one measure
   * @param decimal  : decimal num of one measure
   * @return : the best compression type
   * @see
   */
  private static CompressionFinder getCompressionType(Object maxValue, Object minValue, int decimal,
      char aggregatorType, byte dataTypeSelected) {
    // 'c' for aggregate table,'b' fo rBigdecimal, 'l' for long,'n' for double
    switch (aggregatorType) {
      case 'c':
        return new CompressionFinder(COMPRESSION_TYPE.CUSTOM, DataType.DATA_BYTE,
            DataType.DATA_BYTE);
      case 'b':
        return new CompressionFinder(COMPRESSION_TYPE.CUSTOM_BIGDECIMAL, DataType.DATA_BYTE,
            DataType.DATA_BYTE);
      case 'l':
        return new CompressionFinder(COMPRESSION_TYPE.NONE,
                DataType.DATA_BIGINT, DataType.DATA_BIGINT);
      default:
        break;
    }
    //Here we should use the Max abs as max to getDatatype, let's say -1 and -10000000, -1 is max,
    //but we can't use -1 to getDatatype, we should use -10000000.
    double absMaxValue = Math.abs((double) maxValue) >= Math.abs((double) minValue) ?
        (double) maxValue:(double) minValue;
    // None Decimal
    if (decimal == 0) {
      if (getSize(getDataType(absMaxValue, decimal, dataTypeSelected)) > getSize(
          getDataType((double) maxValue - (double) minValue, decimal, dataTypeSelected))) {
        return new CompressionFinder(COMPRESSION_TYPE.MAX_MIN, DataType.DATA_DOUBLE,
            getDataType((double) maxValue - (double) minValue, decimal, dataTypeSelected));
      } else if (getSize(getDataType(absMaxValue, decimal, dataTypeSelected)) < getSize(
              getDataType((double) maxValue - (double) minValue, decimal, dataTypeSelected))) {
        return new CompressionFinder(COMPRESSION_TYPE.NONE, DataType.DATA_DOUBLE,
                getDataType((double) maxValue - (double) minValue, decimal, dataTypeSelected));
      } else {
        return new CompressionFinder(COMPRESSION_TYPE.NONE, DataType.DATA_DOUBLE,
            getDataType(absMaxValue, decimal, dataTypeSelected));
      }
    }
    // decimal
    else {
      DataType actualDataType = getDataType(absMaxValue, decimal, dataTypeSelected);
      DataType diffDataType =
          getDataType((double) maxValue - (double) minValue, decimal, dataTypeSelected);
      DataType maxNonDecDataType =
          getDataType(Math.pow(10, decimal) * absMaxValue, 0, dataTypeSelected);
      DataType diffNonDecDataType =
          getDataType(Math.pow(10, decimal) * ((double) maxValue - (double) minValue), 0,
              dataTypeSelected);

      CompressionFinder[] finders = new CompressionFinder[] {
          new CompressionFinder(actualDataType, actualDataType, CompressionFinder.PRIORITY.ACTUAL,
              COMPRESSION_TYPE.NONE),
          new CompressionFinder(actualDataType, diffDataType, CompressionFinder.PRIORITY.DIFFSIZE,
              COMPRESSION_TYPE.MAX_MIN), new CompressionFinder(actualDataType, maxNonDecDataType,
          CompressionFinder.PRIORITY.MAXNONDECIMAL, COMPRESSION_TYPE.NON_DECIMAL_CONVERT),
          new CompressionFinder(actualDataType, diffNonDecDataType,
              CompressionFinder.PRIORITY.DIFFNONDECIMAL, COMPRESSION_TYPE.MAX_MIN_NDC) };
      // sort the compressionFinder.The top have the highest priority
      Arrays.sort(finders);
      CompressionFinder compression = finders[0];
      return compression;
    }
  }

  /**
   * @param compType        : compression type
   * @param values          : the data of one measure
   * @param changedDataType : changed data type
   * @param maxValue        : the max value of one measure
   * @param decimal         : the decimal length of one measure
   * @return: the compress data array
   * @see
   */
  public static Object getCompressedValues(COMPRESSION_TYPE compType, double[] values,
      DataType changedDataType, double maxValue, int decimal) {
    Object o;
    switch (compType) {
      case NONE:

        o = compressNone(changedDataType, values);
        return o;

      case MAX_MIN:

        o = compressMaxMin(changedDataType, values, maxValue);
        return o;

      case NON_DECIMAL_CONVERT:

        o = compressNonDecimal(changedDataType, values, decimal);
        return o;

      default:
        o = compressNonDecimalMaxMin(changedDataType, values, decimal, maxValue);
        return o;
    }
  }

  public static Object getCompressedValues(COMPRESSION_TYPE compType, long[] values,
      DataType changedDataType, long maxValue, int decimal) {
    Object o;
    switch (compType) {
      case NONE:
      default:
        return values;
    }
  }

  private static ValueCompressonHolder.UnCompressValue[] getUncompressedValues(
      COMPRESSION_TYPE[] compType, DataType[] actualDataType, DataType[] changedDataType) {

    ValueCompressonHolder.UnCompressValue[] compressValue =
        new ValueCompressonHolder.UnCompressValue[changedDataType.length];
    for (int i = 0; i < changedDataType.length; i++) {
      switch (compType[i]) {
        case NONE:

          compressValue[i] = unCompressNone(changedDataType[i], actualDataType[i]);
          break;

        case MAX_MIN:

          compressValue[i] = unCompressMaxMin(changedDataType[i], actualDataType[i]);
          break;

        case NON_DECIMAL_CONVERT:

          compressValue[i] = unCompressNonDecimal(changedDataType[i], DataType.DATA_DOUBLE);
          break;

        case CUSTOM:
          compressValue[i] = new UnCompressByteArray(UnCompressByteArray.ByteArrayType.BYTE_ARRAY);
          break;

        case CUSTOM_BIGDECIMAL:
          compressValue[i] = new UnCompressByteArray(UnCompressByteArray.ByteArrayType.BIG_DECIMAL);
          break;

        default:
          compressValue[i] = unCompressNonDecimalMaxMin(changedDataType[i], null);
      }
    }
    return compressValue;

  }

  /**
   * compress data to other type for example: double -> int
   */
  private static Object compressNone(DataType changedDataType, double[] value) {
    int i = 0;
    switch (changedDataType) {

      case DATA_BYTE:

        byte[] result = new byte[value.length];

        for (double a : value) {
          result[i] = (byte) a;
          i++;
        }
        return result;

      case DATA_SHORT:

        short[] shortResult = new short[value.length];

        for (double a : value) {
          shortResult[i] = (short) a;
          i++;
        }
        return shortResult;

      case DATA_INT:

        int[] intResult = new int[value.length];

        for (double a : value) {
          intResult[i] = (int) a;
          i++;
        }
        return intResult;

      case DATA_LONG:
      case DATA_BIGINT:

        long[] longResult = new long[value.length];

        for (double a : value) {
          longResult[i] = (long) a;
          i++;
        }
        return longResult;

      case DATA_FLOAT:

        float[] floatResult = new float[value.length];

        for (double a : value) {
          floatResult[i] = (float) a;
          i++;
        }
        return floatResult;

      default:

        return value;

    }
  }

  /**
   * compress data to other type through sub value for example: 1. subValue =
   * maxValue - value 2. subValue: double->int
   */
  private static Object compressMaxMin(DataType changedDataType, double[] value, double maxValue) {
    int i = 0;
    switch (changedDataType) {
      case DATA_BYTE:

        byte[] result = new byte[value.length];
        for (double a : value) {
          result[i] = (byte) (maxValue - a);
          i++;
        }
        return result;

      case DATA_SHORT:

        short[] shortResult = new short[value.length];

        for (double a : value) {
          shortResult[i] = (short) (maxValue - a);
          i++;
        }
        return shortResult;

      case DATA_INT:

        int[] intResult = new int[value.length];

        for (double a : value) {
          intResult[i] = (int) (maxValue - a);
          i++;
        }
        return intResult;

      case DATA_LONG:

        long[] longResult = new long[value.length];

        for (double a : value) {
          longResult[i] = (long) (maxValue - a);
          i++;
        }
        return longResult;

      case DATA_FLOAT:

        float[] floatResult = new float[value.length];

        for (double a : value) {
          floatResult[i] = (float) (maxValue - a);
          i++;
        }
        return floatResult;

      default:

        double[] defaultResult = new double[value.length];

        for (double a : value) {
          defaultResult[i] = (double) (maxValue - a);
          i++;
        }
        return defaultResult;

    }
  }

  /**
   * compress data to other type through sub value for example: 1. subValue =
   * value * Math.pow(10, decimal) 2. subValue: double->int
   */
  private static Object compressNonDecimal(DataType changedDataType, double[] value, int decimal) {
    int i = 0;
    switch (changedDataType) {
      case DATA_BYTE:
        byte[] result = new byte[value.length];

        for (double a : value) {
          result[i] = (byte) (Math.round(Math.pow(10, decimal) * a));
          i++;
        }
        return result;
      case DATA_SHORT:
        short[] shortResult = new short[value.length];

        for (double a : value) {
          shortResult[i] = (short) (Math.round(Math.pow(10, decimal) * a));
          i++;
        }
        return shortResult;
      case DATA_INT:

        int[] intResult = new int[value.length];

        for (double a : value) {
          intResult[i] = (int) (Math.round(Math.pow(10, decimal) * a));
          i++;
        }
        return intResult;

      case DATA_LONG:

        long[] longResult = new long[value.length];

        for (double a : value) {
          longResult[i] = (long) (Math.round(Math.pow(10, decimal) * a));
          i++;
        }
        return longResult;

      case DATA_FLOAT:

        float[] floatResult = new float[value.length];

        for (double a : value) {
          floatResult[i] = (float) (Math.round(Math.pow(10, decimal) * a));
          i++;
        }
        return floatResult;

      default:
        double[] defaultResult = new double[value.length];

        for (double a : value) {
          defaultResult[i] = (double) (Math.round(Math.pow(10, decimal) * a));
          i++;
        }
        return defaultResult;
    }
  }

  /**
   * compress data to other type through sub value for example: 1. subValue =
   * maxValue - value 2. subValue = subValue * Math.pow(10, decimal) 3.
   * subValue: double->int
   */
  private static Object compressNonDecimalMaxMin(DataType changedDataType, double[] value,
      int decimal, double maxValue) {
    int i = 0;
    BigDecimal max = BigDecimal.valueOf(maxValue);
    switch (changedDataType) {
      case DATA_BYTE:

        byte[] result = new byte[value.length];

        for (double a : value) {
          BigDecimal val = BigDecimal.valueOf(a);
          double diff = max.subtract(val).doubleValue();
          result[i] = (byte) (Math.round(diff * Math.pow(10, decimal)));
          i++;
        }
        return result;

      case DATA_SHORT:

        short[] shortResult = new short[value.length];

        for (double a : value) {
          BigDecimal val = BigDecimal.valueOf(a);
          double diff = max.subtract(val).doubleValue();
          shortResult[i] = (short) (Math.round(diff * Math.pow(10, decimal)));
          i++;
        }
        return shortResult;

      case DATA_INT:

        int[] intResult = new int[value.length];

        for (double a : value) {
          BigDecimal val = BigDecimal.valueOf(a);
          double diff = max.subtract(val).doubleValue();
          intResult[i] = (int) (Math.round(diff * Math.pow(10, decimal)));
          i++;
        }
        return intResult;

      case DATA_LONG:

        long[] longResult = new long[value.length];

        for (double a : value) {
          BigDecimal val = BigDecimal.valueOf(a);
          double diff = max.subtract(val).doubleValue();
          longResult[i] = (long) (Math.round(diff * Math.pow(10, decimal)));
          i++;
        }
        return longResult;

      case DATA_FLOAT:

        float[] floatResult = new float[value.length];

        for (double a : value) {
          BigDecimal val = BigDecimal.valueOf(a);
          double diff = max.subtract(val).doubleValue();
          floatResult[i] = (float) (Math.round(diff * Math.pow(10, decimal)));
          i++;
        }
        return floatResult;

      default:

        double[] defaultResult = new double[value.length];

        for (double a : value) {
          BigDecimal val = BigDecimal.valueOf(a);
          double diff = max.subtract(val).doubleValue();
          defaultResult[i] =  (Math.round(diff * Math.pow(10, decimal)));
          i++;
        }
        return defaultResult;

    }
  }

  /**
   * uncompress data for example: int -> double
   */
  public static ValueCompressonHolder.UnCompressValue unCompressNone(DataType compDataType,
      DataType actualDataType) {
    if (actualDataType == DataType.DATA_BIGINT) {
      return new UnCompressDefaultLong();
    } else {
      switch (compDataType) {
        case DATA_BYTE:

          return new UnCompressNoneByte();

        case DATA_SHORT:

          return new UnCompressNoneShort();

        case DATA_INT:

          return new UnCompressNoneInt();

        case DATA_LONG:

          return new UnCompressNoneLong();

        case DATA_FLOAT:

          return new UnCompressNoneFloat();

        default:

          return new UnCompressNoneDefault();
      }
    }
  }

  /**
   * uncompress data 1. value = maxValue - subValue 2. value: int->double
   */
  public static ValueCompressonHolder.UnCompressValue unCompressMaxMin(DataType compDataType,
      DataType actualDataType) {
    switch (compDataType) {
      case DATA_BYTE:

        return new UnCompressMaxMinByte();

      case DATA_SHORT:

        return new UnCompressMaxMinShort();

      case DATA_INT:

        return new UnCompressMaxMinInt();

      case DATA_LONG:

        return new UnCompressMaxMinLong();

      case DATA_FLOAT:

        return new UnCompressMaxMinFloat();

      default:

        return new UnCompressMaxMinDefault();

    }
  }

  /**
   * uncompress data value = value/Math.pow(10, decimal)
   */
  public static ValueCompressonHolder.UnCompressValue unCompressNonDecimal(DataType compDataType,
      DataType actualDataType) {
    switch (compDataType) {
      case DATA_BYTE:

        return new UnCompressNonDecimalByte();

      case DATA_SHORT:

        return new UnCompressNonDecimalShort();

      case DATA_INT:

        return new UnCompressNonDecimalInt();

      case DATA_LONG:

        return new UnCompressNonDecimalLong();

      case DATA_FLOAT:

        return new UnCompressNonDecimalFloat();

      default:

        return new UnCompressNonDecimalDefault();

    }
  }

  /**
   * uncompress data value = (maxValue - subValue)/Math.pow(10, decimal)
   */
  public static ValueCompressonHolder.UnCompressValue unCompressNonDecimalMaxMin(
      DataType compDataType, DataType actualDataType) {
    switch (compDataType) {
      case DATA_BYTE:

        return new UnCompressNonDecimalMaxMinByte();

      case DATA_SHORT:

        return new UnCompressNonDecimalMaxMinShort();

      case DATA_INT:

        return new UnCompressNonDecimalMaxMinInt();

      case DATA_LONG:

        return new UnCompressNonDecimalMaxMinLong();

      case DATA_FLOAT:

        return new UnCompressNonDecimalMaxMinFloat();

      default:

        return new UnCompressNonDecimalMaxMinDefault();

    }
  }

  /**
   * Create Value compression model
   *
   * @param maxValue
   * @param minValue
   * @param decimalLength
   * @param uniqueValue
   * @param aggType
   * @param dataTypeSelected
   * @return
   */
  public static ValueCompressionModel getValueCompressionModel(Object[] maxValue, Object[] minValue,
      int[] decimalLength, Object[] uniqueValue, char[] aggType, byte[] dataTypeSelected) {

    MeasureMetaDataModel metaDataModel =
        new MeasureMetaDataModel(minValue, maxValue, decimalLength, maxValue.length, uniqueValue,
            aggType, dataTypeSelected);
    return getValueCompressionModel(metaDataModel);
  }

  public static ValueCompressionModel getValueCompressionModel(MeasureMetaDataModel measureMDMdl) {
    int measureCount = measureMDMdl.getMeasureCount();
    Object[] minValue = measureMDMdl.getMinValue();
    Object[] maxValue = measureMDMdl.getMaxValue();
    Object[] uniqueValue = measureMDMdl.getUniqueValue();
    int[] decimal = measureMDMdl.getDecimal();
    char[] type = measureMDMdl.getType();
    byte[] dataTypeSelected = measureMDMdl.getDataTypeSelected();
    ValueCompressionModel compressionModel = new ValueCompressionModel();
    DataType[] actualType = new DataType[measureCount];
    DataType[] changedType = new DataType[measureCount];
    COMPRESSION_TYPE[] compType = new COMPRESSION_TYPE[measureCount];
    for (int i = 0; i < measureCount; i++) {
      CompressionFinder compresssionFinder = ValueCompressionUtil
          .getCompressionType(maxValue[i], minValue[i], decimal[i], type[i], dataTypeSelected[i]);
      actualType[i] = compresssionFinder.actualDataType;
      changedType[i] = compresssionFinder.changedDataType;
      compType[i] = compresssionFinder.compType;
    }
    compressionModel.setMaxValue(maxValue);
    compressionModel.setDecimal(decimal);
    compressionModel.setChangedDataType(changedType);
    compressionModel.setCompType(compType);
    compressionModel.setActualDataType(actualType);
    compressionModel.setMinValue(minValue);
    compressionModel.setUniqueValue(uniqueValue);
    compressionModel.setType(type);
    compressionModel.setMinValueFactForAgg(measureMDMdl.getMinValueFactForAgg());
    compressionModel.setDataTypeSelected(dataTypeSelected);
    ValueCompressonHolder.UnCompressValue[] values = ValueCompressionUtil
        .getUncompressedValues(compressionModel.getCompType(), compressionModel.getActualDataType(),
            compressionModel.getChangedDataType());
    compressionModel.setUnCompressValues(values);
    return compressionModel;
  }

  public static byte[] convertToBytes(short[] values) {
    ByteBuffer buffer = ByteBuffer.allocate(values.length * 2);
    for (short val : values) {
      buffer.putShort(val);
    }
    return buffer.array();
  }

  public static byte[] convertToBytes(int[] values) {
    ByteBuffer buffer = ByteBuffer.allocate(values.length * 4);
    for (int val : values) {
      buffer.putInt(val);
    }
    return buffer.array();
  }

  public static byte[] convertToBytes(float[] values) {
    ByteBuffer buffer = ByteBuffer.allocate(values.length * 4);
    for (float val : values) {
      buffer.putFloat(val);
    }
    return buffer.array();
  }

  public static byte[] convertToBytes(long[] values) {
    ByteBuffer buffer = ByteBuffer.allocate(values.length * 8);
    for (long val : values) {
      buffer.putLong(val);
    }
    return buffer.array();
  }

  public static byte[] convertToBytes(double[] values) {
    ByteBuffer buffer = ByteBuffer.allocate(values.length * 8);
    for (double val : values) {
      buffer.putDouble(val);
    }
    return buffer.array();
  }

  public static short[] convertToShortArray(ByteBuffer buffer, int length) {
    buffer.rewind();
    short[] values = new short[length / 2];

    for (int i = 0; i < values.length; i++) {
      values[i] = buffer.getShort();
    }
    return values;
  }

  public static int[] convertToIntArray(ByteBuffer buffer, int length) {
    buffer.rewind();
    int[] values = new int[length / 4];

    for (int i = 0; i < values.length; i++) {
      values[i] = buffer.getInt();
    }
    return values;
  }

  public static float[] convertToFloatArray(ByteBuffer buffer, int length) {
    buffer.rewind();
    float[] values = new float[length / 4];

    for (int i = 0; i < values.length; i++) {
      values[i] = buffer.getFloat();
    }
    return values;
  }

  public static long[] convertToLongArray(ByteBuffer buffer, int length) {
    buffer.rewind();
    long[] values = new long[length / 8];
    for (int i = 0; i < values.length; i++) {
      values[i] = buffer.getLong();
    }
    return values;
  }

  public static double[] convertToDoubleArray(ByteBuffer buffer, int length) {
    buffer.rewind();
    double[] values = new double[length / 8];
    for (int i = 0; i < values.length; i++) {
      values[i] = buffer.getDouble();
    }
    return values;
  }

  /**
   * use to identify compression type.
   */
  public static enum COMPRESSION_TYPE {
    /**
     *
     */
    NONE, /**
     *
     */
    MAX_MIN, /**
     *
     */
    NON_DECIMAL_CONVERT, /**
     *
     */
    MAX_MIN_NDC,

    /**
     * custome
     */
    CUSTOM,

    CUSTOM_BIGDECIMAL
  }

  /**
   * use to identify the type of data.
   */
  public static enum DataType {
    /**
     *
     */
    DATA_BYTE(), /**
     *
     */
    DATA_SHORT(), /**
     *
     */
    DATA_INT(), /**
     *
     */
    DATA_FLOAT(), /**
     *
     */
    DATA_LONG(), /**
     *
     */
    DATA_BIGINT(), /**
     *
     */
    DATA_DOUBLE();

    /**
     * DataType.
     */
    private DataType() {
      //this.size = size;
    }

  }

  /**
   * through the size of data type,priority and compression type, select the
   * best compression type
   */
  private static class CompressionFinder implements Comparable<CompressionFinder> {
    /**
     * compType.
     */
    private COMPRESSION_TYPE compType;
    /**
     * actualDataType.
     */
    private DataType actualDataType;
    /**
     * changedDataType.
     */
    private DataType changedDataType;
    /**
     * the size of changed data
     */
    private int size;
    /**
     * priority.
     */
    private PRIORITY priority;

    /**
     * CompressionFinder constructor.
     *
     * @param compType
     * @param actualDataType
     * @param changedDataType
     */
    CompressionFinder(COMPRESSION_TYPE compType, DataType actualDataType,
        DataType changedDataType) {
      super();
      this.compType = compType;
      this.actualDataType = actualDataType;
      this.changedDataType = changedDataType;
    }

    /**
     * CompressionFinder overloaded constructor.
     *
     * @param actualDataType
     * @param changedDataType
     * @param priority
     * @param compType
     */

    CompressionFinder(DataType actualDataType, DataType changedDataType, PRIORITY priority,
        COMPRESSION_TYPE compType) {
      super();
      this.actualDataType = actualDataType;
      this.changedDataType = changedDataType;
      this.size = getSize(changedDataType);
      this.priority = priority;
      this.compType = compType;
    }

    @Override public boolean equals(Object obj) {
      boolean equals = false;
      if (obj instanceof CompressionFinder) {
        CompressionFinder cf = (CompressionFinder) obj;

        if (this.size == cf.size && this.priority == cf.priority) {
          equals = true;
        }

      }
      return equals;
    }

    @Override public int hashCode() {
      final int code = 31;
      int result = 1;

      result = code * result + this.size;
      result = code * result + ((priority == null) ? 0 : priority.hashCode());
      return result;
    }

    @Override public int compareTo(CompressionFinder o) {
      int returnVal = 0;
      // the big size have high priority
      if (this.equals(o)) {
        returnVal = 0;
      } else if (this.size == o.size) {
        // the compression type priority
        if (priority.priority > o.priority.priority) {
          returnVal = 1;
        } else if (priority.priority < o.priority.priority) {
          returnVal = -1;
        }

      } else if (this.size > o.size) {
        returnVal = 1;
      } else {
        returnVal = -1;
      }
      return returnVal;
    }

    /**
     * Compression type priority.
     * ACTUAL is the highest priority and DIFFNONDECIMAL is the lowest
     * priority
     */
    static enum PRIORITY {
      /**
       *
       */
      ACTUAL(0), /**
       *
       */
      DIFFSIZE(1), /**
       *
       */
      MAXNONDECIMAL(2), /**
       *
       */
      DIFFNONDECIMAL(3);

      /**
       * priority.
       */
      private int priority;

      private PRIORITY(int priority) {
        this.priority = priority;
      }
    }
  }

}



