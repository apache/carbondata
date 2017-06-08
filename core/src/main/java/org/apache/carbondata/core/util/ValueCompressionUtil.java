/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.core.util;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.carbondata.core.compression.BigIntCompressor;
import org.apache.carbondata.core.compression.DoubleCompressor;
import org.apache.carbondata.core.compression.ValueCompressor;
import org.apache.carbondata.core.datastore.compression.ReaderCompressModel;
import org.apache.carbondata.core.datastore.compression.ValueCompressionHolder;
import org.apache.carbondata.core.datastore.compression.decimal.CompressByteArray;
import org.apache.carbondata.core.datastore.compression.decimal.CompressionMaxMinByte;
import org.apache.carbondata.core.datastore.compression.decimal.CompressionMaxMinDefault;
import org.apache.carbondata.core.datastore.compression.decimal.CompressionMaxMinInt;
import org.apache.carbondata.core.datastore.compression.decimal.CompressionMaxMinLong;
import org.apache.carbondata.core.datastore.compression.decimal.CompressionMaxMinShort;
import org.apache.carbondata.core.datastore.compression.nondecimal.CompressionNonDecimalByte;
import org.apache.carbondata.core.datastore.compression.nondecimal.CompressionNonDecimalDefault;
import org.apache.carbondata.core.datastore.compression.nondecimal.CompressionNonDecimalInt;
import org.apache.carbondata.core.datastore.compression.nondecimal.CompressionNonDecimalLong;
import org.apache.carbondata.core.datastore.compression.nondecimal.CompressionNonDecimalMaxMinByte;
import org.apache.carbondata.core.datastore.compression.nondecimal.CompressionNonDecimalMaxMinDefault;
import org.apache.carbondata.core.datastore.compression.nondecimal.CompressionNonDecimalMaxMinInt;
import org.apache.carbondata.core.datastore.compression.nondecimal.CompressionNonDecimalMaxMinLong;
import org.apache.carbondata.core.datastore.compression.nondecimal.CompressionNonDecimalMaxMinShort;
import org.apache.carbondata.core.datastore.compression.nondecimal.CompressionNonDecimalShort;
import org.apache.carbondata.core.datastore.compression.none.CompressionNoneByte;
import org.apache.carbondata.core.datastore.compression.none.CompressionNoneDefault;
import org.apache.carbondata.core.datastore.compression.none.CompressionNoneInt;
import org.apache.carbondata.core.datastore.compression.none.CompressionNoneLong;
import org.apache.carbondata.core.datastore.compression.none.CompressionNoneShort;
import org.apache.carbondata.core.metadata.ValueEncoderMeta;
import org.apache.carbondata.core.metadata.datatype.DataType;

public final class ValueCompressionUtil {

  private ValueCompressionUtil() {
  }

  /**
   * decide actual type of value
   *
   * @param value   :the measure value
   * @param mantissa :
   * @return: actual type of value
   * @see
   */
  private static DataType getDataType(double value, int mantissa, byte dataTypeSelected) {
    DataType dataType = DataType.DOUBLE;
    if (mantissa == 0) {
      if (value <= Byte.MAX_VALUE && value >= Byte.MIN_VALUE) {
        dataType = DataType.BYTE;
      } else if (value <= Short.MAX_VALUE && value >= Short.MIN_VALUE) {
        dataType = DataType.SHORT;
      } else if (value <= Integer.MAX_VALUE && value >= Integer.MIN_VALUE) {
        dataType = DataType.INT;
      } else if (value <= Long.MAX_VALUE && value >= Long.MIN_VALUE) {
        dataType = DataType.LONG;
      }
    } else {
      if (dataTypeSelected == 1) {
        if (value <= Float.MAX_VALUE && value >= Float.MIN_VALUE) {
          float floatValue = (float) value;
          if (floatValue - value != 0) {
            dataType = DataType.DOUBLE;
          } else {
            dataType = DataType.FLOAT;
          }
        } else if (value <= Double.MAX_VALUE && value >= Double.MIN_VALUE) {
          dataType = DataType.DOUBLE;
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
      case BOOLEAN:
      case BYTE:
        return 1;
      case SHORT:
        return 2;
      case INT:
      case FLOAT:
        return 4;
      default:
        return 8;
    }
  }

  /**
   * get the best compression type. priority list,from high to low:
   * COMPRESSION_TYPE.ADAPTIVE COMPRESSION_TYPE.DELTA_DOUBLE
   * COMPRESSION_TYPE.BIGINT COMPRESSION_TYPE.DELTA_NON_DECIMAL
   *
   * @param maxValue : max value of one measure
   * @param minValue : min value of one measure
   * @param mantissa  : decimal num of one measure
   * @return : the best compression type
   * @see
   */
  public static CompressionFinder getCompressionFinder(Object maxValue, Object minValue,
      int mantissa, DataType measureStoreType, byte dataTypeSelected) {
    switch (measureStoreType) {
      case DECIMAL:
        return new CompressionFinder(COMPRESSION_TYPE.BIGDECIMAL, DataType.BYTE,
            DataType.BYTE, measureStoreType);
      case SHORT:
      case INT:
      case LONG:
        return getLongCompressorFinder(maxValue, minValue, mantissa, dataTypeSelected,
            measureStoreType);
      case DOUBLE:
        return getDoubleCompressorFinder(maxValue, minValue, mantissa, dataTypeSelected,
            measureStoreType);
      default:
        throw new IllegalArgumentException("unsupported measure type");
    }
  }

  private static CompressionFinder getDoubleCompressorFinder(Object maxValue, Object minValue,
      int mantissa, byte dataTypeSelected, DataType measureStoreType) {
    //Here we should use the Max abs as max to getDatatype, let's say -1 and -10000000, -1 is max,
    //but we can't use -1 to getDatatype, we should use -10000000.
    double absMaxValue = Math.abs((double) maxValue) >= Math.abs((double) minValue) ?
        (double) maxValue : (double) minValue;
    DataType adaptiveDataType = getDataType(absMaxValue, mantissa, dataTypeSelected);
    DataType deltaDataType = getDataType((double) maxValue - (double) minValue, mantissa,
        dataTypeSelected);

    if (mantissa == 0) {
      // short, int, long
      int adaptiveSize = getSize(adaptiveDataType);
      int deltaSize = getSize(deltaDataType);
      if (adaptiveSize > deltaSize) {
        return new CompressionFinder(COMPRESSION_TYPE.DELTA_DOUBLE, DataType.DOUBLE,
            deltaDataType, measureStoreType);
      } else if (adaptiveSize < deltaSize) {
        return new CompressionFinder(COMPRESSION_TYPE.ADAPTIVE, DataType.DOUBLE,
            deltaDataType, measureStoreType);
      } else {
        return new CompressionFinder(COMPRESSION_TYPE.ADAPTIVE, DataType.DOUBLE,
            adaptiveDataType, measureStoreType);
      }
    } else {
      // double
      DataType maxNonDecDataType =
          getDataType(Math.pow(10, mantissa) * absMaxValue, 0, dataTypeSelected);
      DataType diffNonDecDataType =
          getDataType(Math.pow(10, mantissa) * ((double) maxValue - (double) minValue), 0,
              dataTypeSelected);

      CompressionFinder[] finders = {
          new CompressionFinder(COMPRESSION_TYPE.ADAPTIVE, adaptiveDataType, adaptiveDataType,
              CompressionFinder.PRIORITY.ACTUAL, measureStoreType),
          new CompressionFinder(COMPRESSION_TYPE.DELTA_DOUBLE, adaptiveDataType, deltaDataType,
              CompressionFinder.PRIORITY.DIFFSIZE, measureStoreType),
          new CompressionFinder(COMPRESSION_TYPE.BIGINT, adaptiveDataType, maxNonDecDataType,
              CompressionFinder.PRIORITY.MAXNONDECIMAL, measureStoreType),
          new CompressionFinder(COMPRESSION_TYPE.DELTA_NON_DECIMAL, adaptiveDataType,
              diffNonDecDataType, CompressionFinder.PRIORITY.DIFFNONDECIMAL, measureStoreType) };
      // sort the compressionFinder.The top have the highest priority
      Arrays.sort(finders);
      return finders[0];
    }
  }

  private static CompressionFinder getLongCompressorFinder(Object maxValue, Object minValue,
      int mantissa, byte dataTypeSelected, DataType measureStoreType) {
    long value = Math.max(Math.abs((long)maxValue), Math.abs((long)minValue));
    DataType adaptiveDataType = getDataType(value, mantissa, dataTypeSelected);
    int adaptiveSize = getSize(adaptiveDataType);
    DataType deltaDataType = null;
    // we cannot apply compression in case actual data type of the column is long
    // consider the scenario when max and min value are equal to is long max and min value OR
    // when the max and min value are resulting in a value greater than long max value, then
    // it is not possible to determine the compression type.
    if (adaptiveDataType == DataType.LONG) {
      deltaDataType = DataType.LONG;
    } else {
      deltaDataType = getDataType((long) maxValue - (long) minValue, mantissa, dataTypeSelected);
    }
    int deltaSize = getSize(deltaDataType);
    if (adaptiveSize > deltaSize) {
      return new CompressionFinder(COMPRESSION_TYPE.DELTA_DOUBLE, DataType.LONG,
          deltaDataType, measureStoreType);
    } else if (adaptiveSize < deltaSize) {
      return new CompressionFinder(COMPRESSION_TYPE.ADAPTIVE, DataType.LONG,
          deltaDataType, measureStoreType);
    } else {
      return new CompressionFinder(COMPRESSION_TYPE.ADAPTIVE, DataType.LONG,
          adaptiveDataType, measureStoreType);
    }
  }

  /**
   * @param compType        : compression type
   * @param values          : the data of one measure
   * @param changedDataType : changed data type
   * @param maxValue        : the max value of one measure
   * @param mantissa         : the decimal length of one measure
   * @return: the compress data array
   * @see
   */
  public static Object getCompressedValues(COMPRESSION_TYPE compType, double[] values,
      DataType changedDataType, double maxValue, int mantissa) {
    switch (compType) {
      case ADAPTIVE:
        return compressNone(changedDataType, values);
      case DELTA_DOUBLE:
        return compressMaxMin(changedDataType, values, maxValue);
      case BIGINT:
        return compressNonDecimal(changedDataType, values, mantissa);
      default:
        return compressNonDecimalMaxMin(changedDataType, values, mantissa, maxValue);
    }
  }

  /**
   * It returns Compressor for given datatype
   * @param compressorFinder
   * @return compressor based on actualdatatype
   */
  public static ValueCompressor getValueCompressor(CompressionFinder compressorFinder) {
    switch (compressorFinder.getMeasureStoreType()) {
      case SHORT:
      case INT:
      case LONG:
        return new BigIntCompressor();
      default:
        return new DoubleCompressor();
    }
  }

  /**
   * get uncompressed object
   * @param compressionFinders : Compression types for measures
   * @return
   */
  public static ValueCompressionHolder[] getValueCompressionHolder(
      CompressionFinder[] compressionFinders) {
    ValueCompressionHolder[] valueCompressionHolders =
        new ValueCompressionHolder[compressionFinders.length];
    for (int i = 0; i < compressionFinders.length; i++) {
      valueCompressionHolders[i] = getValueCompressionHolder(compressionFinders[i]);
    }
    return valueCompressionHolders;
  }


  /**
   *
   * @param compressionFinder for measure other then bigdecimal
   * @return
   */
  private static ValueCompressionHolder getValueCompressionHolder(
      CompressionFinder compressionFinder) {
    switch (compressionFinder.getMeasureStoreType()) {
      default:
        return getValueCompressionHolder(compressionFinder.getCompType(),
            compressionFinder.getActualDataType(), compressionFinder.getConvertedDataType());
    }
  }

  private static ValueCompressionHolder getValueCompressionHolder(COMPRESSION_TYPE compType,
      DataType actualDataType, DataType changedDataType) {
    switch (compType) {
      case ADAPTIVE:
        return getCompressionNone(changedDataType, actualDataType);
      case DELTA_DOUBLE:
        return getCompressionDecimalMaxMin(changedDataType, actualDataType);
      case DELTA_NON_DECIMAL:
        return getCompressionNonDecimalMaxMin(changedDataType);
      case BIGINT:
        return getCompressionNonDecimal(changedDataType);
      case BIGDECIMAL:
        return new CompressByteArray();
      default:
        throw new IllegalArgumentException("unsupported compType: " + compType);
    }
  }

  /**
   * compress data to other type for example: double -> int
   */
  private static Object compressNone(DataType changedDataType, double[] value) {
    int i = 0;
    switch (changedDataType) {
      case BYTE:
        byte[] result = new byte[value.length];

        for (double a : value) {
          result[i] = (byte) a;
          i++;
        }
        return result;

      case SHORT:
        short[] shortResult = new short[value.length];

        for (double a : value) {
          shortResult[i] = (short) a;
          i++;
        }
        return shortResult;

      case INT:
        int[] intResult = new int[value.length];

        for (double a : value) {
          intResult[i] = (int) a;
          i++;
        }
        return intResult;

      case LONG:
        long[] longResult = new long[value.length];

        for (double a : value) {
          longResult[i] = (long) a;
          i++;
        }
        return longResult;

      case FLOAT:
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
      case BYTE:

        byte[] result = new byte[value.length];
        for (double a : value) {
          result[i] = (byte) (maxValue - a);
          i++;
        }
        return result;

      case SHORT:

        short[] shortResult = new short[value.length];

        for (double a : value) {
          shortResult[i] = (short) (maxValue - a);
          i++;
        }
        return shortResult;

      case INT:

        int[] intResult = new int[value.length];

        for (double a : value) {
          intResult[i] = (int) (maxValue - a);
          i++;
        }
        return intResult;

      case LONG:

        long[] longResult = new long[value.length];

        for (double a : value) {
          longResult[i] = (long) (maxValue - a);
          i++;
        }
        return longResult;

      case FLOAT:

        float[] floatResult = new float[value.length];

        for (double a : value) {
          floatResult[i] = (float) (maxValue - a);
          i++;
        }
        return floatResult;

      default:

        double[] defaultResult = new double[value.length];

        for (double a : value) {
          defaultResult[i] = maxValue - a;
          i++;
        }
        return defaultResult;

    }
  }

  /**
   * compress data to other type through sub value for example: 1. subValue =
   * value * Math.pow(10, mantissa) 2. subValue: double->int
   */
  private static Object compressNonDecimal(DataType changedDataType, double[] value, int mantissa) {
    int i = 0;
    switch (changedDataType) {
      case BYTE:
        byte[] result = new byte[value.length];

        for (double a : value) {
          result[i] = (byte) (Math.round(Math.pow(10, mantissa) * a));
          i++;
        }
        return result;
      case SHORT:
        short[] shortResult = new short[value.length];

        for (double a : value) {
          shortResult[i] = (short) (Math.round(Math.pow(10, mantissa) * a));
          i++;
        }
        return shortResult;
      case INT:

        int[] intResult = new int[value.length];

        for (double a : value) {
          intResult[i] = (int) (Math.round(Math.pow(10, mantissa) * a));
          i++;
        }
        return intResult;

      case LONG:

        long[] longResult = new long[value.length];

        for (double a : value) {
          longResult[i] = Math.round(Math.pow(10, mantissa) * a);
          i++;
        }
        return longResult;

      case FLOAT:

        float[] floatResult = new float[value.length];

        for (double a : value) {
          floatResult[i] = (float) (Math.round(Math.pow(10, mantissa) * a));
          i++;
        }
        return floatResult;

      default:
        double[] defaultResult = new double[value.length];

        for (double a : value) {
          defaultResult[i] = (double) (Math.round(Math.pow(10, mantissa) * a));
          i++;
        }
        return defaultResult;
    }
  }

  /**
   * compress data to other type through sub value for example: 1. subValue =
   * maxValue - value 2. subValue = subValue * Math.pow(10, mantissa) 3.
   * subValue: double->int
   */
  private static Object compressNonDecimalMaxMin(DataType changedDataType, double[] value,
      int mantissa, double maxValue) {
    int i = 0;
    BigDecimal max = BigDecimal.valueOf(maxValue);
    switch (changedDataType) {
      case BYTE:

        byte[] result = new byte[value.length];

        for (double a : value) {
          BigDecimal val = BigDecimal.valueOf(a);
          double diff = max.subtract(val).doubleValue();
          result[i] = (byte) (Math.round(diff * Math.pow(10, mantissa)));
          i++;
        }
        return result;

      case SHORT:

        short[] shortResult = new short[value.length];

        for (double a : value) {
          BigDecimal val = BigDecimal.valueOf(a);
          double diff = max.subtract(val).doubleValue();
          shortResult[i] = (short) (Math.round(diff * Math.pow(10, mantissa)));
          i++;
        }
        return shortResult;

      case INT:

        int[] intResult = new int[value.length];

        for (double a : value) {
          BigDecimal val = BigDecimal.valueOf(a);
          double diff = max.subtract(val).doubleValue();
          intResult[i] = (int) (Math.round(diff * Math.pow(10, mantissa)));
          i++;
        }
        return intResult;

      case LONG:

        long[] longResult = new long[value.length];

        for (double a : value) {
          BigDecimal val = BigDecimal.valueOf(a);
          double diff = max.subtract(val).doubleValue();
          longResult[i] = Math.round(diff * Math.pow(10, mantissa));
          i++;
        }
        return longResult;

      case FLOAT:

        float[] floatResult = new float[value.length];

        for (double a : value) {
          BigDecimal val = BigDecimal.valueOf(a);
          double diff = max.subtract(val).doubleValue();
          floatResult[i] = (float) (Math.round(diff * Math.pow(10, mantissa)));
          i++;
        }
        return floatResult;

      default:

        double[] defaultResult = new double[value.length];

        for (double a : value) {
          BigDecimal val = BigDecimal.valueOf(a);
          double diff = max.subtract(val).doubleValue();
          defaultResult[i] =  (Math.round(diff * Math.pow(10, mantissa)));
          i++;
        }
        return defaultResult;

    }
  }

  /**
   * uncompress data for example: int -> double
   */
  public static ValueCompressionHolder getCompressionNone(DataType compDataType,
      DataType actualDataType) {
    switch (compDataType) {
      case BYTE:
        return new CompressionNoneByte(actualDataType);
      case SHORT:
        return new CompressionNoneShort(actualDataType);
      case INT:
        return new CompressionNoneInt(actualDataType);
      case LONG:
        return new CompressionNoneLong(actualDataType);
      default:
        return new CompressionNoneDefault(actualDataType);
    }
  }

  /**
   * uncompress data 1. value = maxValue - subValue 2. value: int->double
   */
  public static ValueCompressionHolder getCompressionDecimalMaxMin(
      DataType compDataType, DataType actualDataType) {
    switch (compDataType) {
      case BYTE:
        return new CompressionMaxMinByte(actualDataType);
      case SHORT:
        return new CompressionMaxMinShort(actualDataType);
      case INT:
        return new CompressionMaxMinInt(actualDataType);
      case LONG:
        return new CompressionMaxMinLong(actualDataType);
      default:
        return new CompressionMaxMinDefault(actualDataType);
    }
  }

  /**
   * uncompress data value = value/Math.pow(10, mantissa)
   */
  public static ValueCompressionHolder getCompressionNonDecimal(
      DataType compDataType) {
    switch (compDataType) {
      case BYTE:
        return new CompressionNonDecimalByte();
      case SHORT:
        return new CompressionNonDecimalShort();
      case INT:
        return new CompressionNonDecimalInt();
      case LONG:
        return new CompressionNonDecimalLong();
      default:
        return new CompressionNonDecimalDefault();
    }
  }

  /**
   * uncompress data value = (maxValue - subValue)/Math.pow(10, mantissa)
   */
  public static ValueCompressionHolder getCompressionNonDecimalMaxMin(
      DataType compDataType) {
    switch (compDataType) {
      case BYTE:
        return new CompressionNonDecimalMaxMinByte();
      case SHORT:
        return new CompressionNonDecimalMaxMinShort();
      case INT:
        return new CompressionNonDecimalMaxMinInt();
      case LONG:
        return new CompressionNonDecimalMaxMinLong();
      default:
        return new CompressionNonDecimalMaxMinDefault();
    }
  }

  /**
   * Create Value compression model for read path
   */
  public static ReaderCompressModel getReaderCompressModel(ValueEncoderMeta meta) {
    ReaderCompressModel compressModel = new ReaderCompressModel();
    CompressionFinder compressFinder =
        getCompressionFinder(meta.getMaxValue(), meta.getMinValue(), meta.getDecimal(),
            meta.getType(), meta.getDataTypeSelected());
    compressModel.setValueCompressionHolder(
          ValueCompressionUtil.getValueCompressionHolder(compressFinder));
    compressModel.setConvertedDataType(compressFinder.getConvertedDataType());
    compressModel.setValueEncoderMeta(meta);
    return compressModel;
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
  public enum COMPRESSION_TYPE {
    /**
     * adaptive compression based on data type
     */
    ADAPTIVE,

    /**
     * min max delta compression for double
     */
    DELTA_DOUBLE,

    /**
     * min max delta compression for short, int, long
     */
    DELTA_NON_DECIMAL,

    /**
     * for bigint
     */
    BIGINT,

    /**
     * for big decimal (PR388)
     */
    BIGDECIMAL
  }
}
