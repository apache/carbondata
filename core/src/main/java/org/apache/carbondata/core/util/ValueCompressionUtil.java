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

import org.apache.carbondata.core.compression.BigIntCompressor;
import org.apache.carbondata.core.compression.DoubleCompressor;
import org.apache.carbondata.core.compression.ValueCompressor;
import org.apache.carbondata.core.datastorage.store.compression.MeasureMetaDataModel;
import org.apache.carbondata.core.datastorage.store.compression.ReaderCompressModel;
import org.apache.carbondata.core.datastorage.store.compression.ValueCompressonHolder;
import org.apache.carbondata.core.datastorage.store.compression.WriterCompressModel;
import org.apache.carbondata.core.datastorage.store.compression.decimal.UnCompressByteArray;
import org.apache.carbondata.core.datastorage.store.compression.decimal.UnCompressMaxMinByte;
import org.apache.carbondata.core.datastorage.store.compression.decimal.UnCompressMaxMinDefault;
import org.apache.carbondata.core.datastorage.store.compression.decimal.UnCompressMaxMinFloat;
import org.apache.carbondata.core.datastorage.store.compression.decimal.UnCompressMaxMinInt;
import org.apache.carbondata.core.datastorage.store.compression.decimal.UnCompressMaxMinLong;
import org.apache.carbondata.core.datastorage.store.compression.decimal.UnCompressMaxMinShort;
import org.apache.carbondata.core.datastorage.store.compression.nondecimal.UnCompressNonDecimalByte;
import org.apache.carbondata.core.datastorage.store.compression.nondecimal.UnCompressNonDecimalDefault;
import org.apache.carbondata.core.datastorage.store.compression.nondecimal.UnCompressNonDecimalFloat;
import org.apache.carbondata.core.datastorage.store.compression.nondecimal.UnCompressNonDecimalInt;
import org.apache.carbondata.core.datastorage.store.compression.nondecimal.UnCompressNonDecimalLong;
import org.apache.carbondata.core.datastorage.store.compression.nondecimal.UnCompressNonDecimalMaxMinByte;
import org.apache.carbondata.core.datastorage.store.compression.nondecimal.UnCompressNonDecimalMaxMinDefault;
import org.apache.carbondata.core.datastorage.store.compression.nondecimal.UnCompressNonDecimalMaxMinFloat;
import org.apache.carbondata.core.datastorage.store.compression.nondecimal.UnCompressNonDecimalMaxMinInt;
import org.apache.carbondata.core.datastorage.store.compression.nondecimal.UnCompressNonDecimalMaxMinLong;
import org.apache.carbondata.core.datastorage.store.compression.nondecimal.UnCompressNonDecimalMaxMinShort;
import org.apache.carbondata.core.datastorage.store.compression.nondecimal.UnCompressNonDecimalShort;
import org.apache.carbondata.core.datastorage.store.compression.none.UnCompressNoneByte;
import org.apache.carbondata.core.datastorage.store.compression.none.UnCompressNoneDefault;
import org.apache.carbondata.core.datastorage.store.compression.none.UnCompressNoneFloat;
import org.apache.carbondata.core.datastorage.store.compression.none.UnCompressNoneInt;
import org.apache.carbondata.core.datastorage.store.compression.none.UnCompressNoneLong;
import org.apache.carbondata.core.datastorage.store.compression.none.UnCompressNoneShort;
import org.apache.carbondata.core.metadata.ValueEncoderMeta;

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
    DataType dataType = DataType.DATA_DOUBLE;
    if (mantissa == 0) {
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
      int mantissa, char measureStoreType, byte dataTypeSelected) {
    // ''b' for decimal, 'l' for long, 'n' for double
    switch (measureStoreType) {
      case 'b':
        return new CompressionFinder(COMPRESSION_TYPE.BIGDECIMAL, DataType.DATA_BYTE,
            DataType.DATA_BYTE);
      case 'l':
        return getLongCompressorFinder(maxValue, minValue, mantissa, dataTypeSelected);
      case 'n':
        return getDoubleCompressorFinder(maxValue, minValue, mantissa, dataTypeSelected);
      default:
        throw new IllegalArgumentException("unsupported measure type");
    }
  }

  private static CompressionFinder getDoubleCompressorFinder(Object maxValue, Object minValue,
      int mantissa, byte dataTypeSelected) {
    //Here we should use the Max abs as max to getDatatype, let's say -1 and -10000000, -1 is max,
    //but we can't use -1 to getDatatype, we should use -10000000.
    double absMaxValue = Math.abs((double) maxValue) >= Math.abs((double) minValue) ?
        (double) maxValue:(double) minValue;
    DataType adaptiveDataType = getDataType(absMaxValue, mantissa, dataTypeSelected);
    DataType deltaDataType = getDataType((long) maxValue - (long) minValue, mantissa,
        dataTypeSelected);

    if (mantissa == 0) {
      // short, int, long
      if (getSize(adaptiveDataType) > getSize(deltaDataType)) {
        return new CompressionFinder(COMPRESSION_TYPE.DELTA_DOUBLE, DataType.DATA_DOUBLE,
            deltaDataType);
      } else {
        return new CompressionFinder(COMPRESSION_TYPE.ADAPTIVE, DataType.DATA_DOUBLE, adaptiveDataType);
      }
    } else {
      // double
      DataType maxNonDecDataType =
          getDataType(Math.pow(10, mantissa) * absMaxValue, 0, dataTypeSelected);
      DataType diffNonDecDataType =
          getDataType(Math.pow(10, mantissa) * ((double) maxValue - (double) minValue), 0,
              dataTypeSelected);

      CompressionFinder[] finders = new CompressionFinder[] {
          new CompressionFinder(COMPRESSION_TYPE.ADAPTIVE, adaptiveDataType, adaptiveDataType,
              CompressionFinder.PRIORITY.ACTUAL),
          new CompressionFinder(COMPRESSION_TYPE.DELTA_DOUBLE, adaptiveDataType, deltaDataType,
              CompressionFinder.PRIORITY.DIFFSIZE),
          new CompressionFinder(COMPRESSION_TYPE.BIGINT, adaptiveDataType, maxNonDecDataType,
              CompressionFinder.PRIORITY.MAXNONDECIMAL),
          new CompressionFinder(COMPRESSION_TYPE.DELTA_NON_DECIMAL, adaptiveDataType,
              diffNonDecDataType, CompressionFinder.PRIORITY.DIFFNONDECIMAL) };
      // sort the compressionFinder.The top have the highest priority
      Arrays.sort(finders);
      return finders[0];
    }
  }

  private static CompressionFinder getLongCompressorFinder(Object maxValue, Object minValue,
      int mantissa, byte dataTypeSelected) {
    DataType adaptiveDataType = getDataType((long) maxValue, mantissa, dataTypeSelected);
    int adaptiveSize = getSize(adaptiveDataType);
    DataType deltaDataType = getDataType((long) maxValue - (long) minValue, mantissa,
        dataTypeSelected);
    int deltaSize = getSize(deltaDataType);
    if (adaptiveSize > deltaSize) {
      return new CompressionFinder(COMPRESSION_TYPE.DELTA_DOUBLE, DataType.DATA_BIGINT,
          deltaDataType);
    } else {
      return new CompressionFinder(COMPRESSION_TYPE.ADAPTIVE, DataType.DATA_BIGINT,
          getDataType((long) maxValue, mantissa, dataTypeSelected));
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
   * @param actualDataType
   * @return compressor based on actualdatatype
   */
  public static ValueCompressor getValueCompressor(DataType actualDataType) {
    switch (actualDataType) {
      case DATA_BIGINT:
        return new BigIntCompressor();
      default:
        return new DoubleCompressor();
    }
  }

  private static ValueCompressonHolder.UnCompressValue[] getUncompressedValues(
      COMPRESSION_TYPE[] compType, DataType[] actualDataType, DataType[] changedDataType) {

    ValueCompressonHolder.UnCompressValue[] compressValue =
        new ValueCompressonHolder.UnCompressValue[changedDataType.length];
    for (int i = 0; i < changedDataType.length; i++) {
      switch (compType[i]) {
        case ADAPTIVE:
          compressValue[i] = getUnCompressNone(changedDataType[i], actualDataType[i]);
          break;

        case DELTA_DOUBLE:
          compressValue[i] = getUnCompressDecimalMaxMin(changedDataType[i], actualDataType[i]);
          break;

        case BIGINT:
          compressValue[i] = getUnCompressNonDecimal(changedDataType[i]);
          break;

        case BIGDECIMAL:
          compressValue[i] = new UnCompressByteArray(UnCompressByteArray.ByteArrayType.BIG_DECIMAL);
          break;

        default:
          compressValue[i] = getUnCompressNonDecimalMaxMin(changedDataType[i]);
      }
    }
    return compressValue;
  }

  private static ValueCompressonHolder.UnCompressValue getUncompressedValues(
      COMPRESSION_TYPE compType, DataType actualDataType, DataType changedDataType) {
    switch (compType) {
      case ADAPTIVE:
        return getUnCompressNone(changedDataType, actualDataType);
      case DELTA_DOUBLE:
        return getUnCompressDecimalMaxMin(changedDataType, actualDataType);
      case DELTA_NON_DECIMAL:
        return getUnCompressNonDecimalMaxMin(changedDataType);
      case BIGINT:
        return getUnCompressNonDecimal(changedDataType);
      case BIGDECIMAL:
        return new UnCompressByteArray(UnCompressByteArray.ByteArrayType.BIG_DECIMAL);
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
   * value * Math.pow(10, mantissa) 2. subValue: double->int
   */
  private static Object compressNonDecimal(DataType changedDataType, double[] value, int mantissa) {
    int i = 0;
    switch (changedDataType) {
      case DATA_BYTE:
        byte[] result = new byte[value.length];

        for (double a : value) {
          result[i] = (byte) (Math.round(Math.pow(10, mantissa) * a));
          i++;
        }
        return result;
      case DATA_SHORT:
        short[] shortResult = new short[value.length];

        for (double a : value) {
          shortResult[i] = (short) (Math.round(Math.pow(10, mantissa) * a));
          i++;
        }
        return shortResult;
      case DATA_INT:

        int[] intResult = new int[value.length];

        for (double a : value) {
          intResult[i] = (int) (Math.round(Math.pow(10, mantissa) * a));
          i++;
        }
        return intResult;

      case DATA_LONG:

        long[] longResult = new long[value.length];

        for (double a : value) {
          longResult[i] = (long) (Math.round(Math.pow(10, mantissa) * a));
          i++;
        }
        return longResult;

      case DATA_FLOAT:

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
      case DATA_BYTE:

        byte[] result = new byte[value.length];

        for (double a : value) {
          BigDecimal val = BigDecimal.valueOf(a);
          double diff = max.subtract(val).doubleValue();
          result[i] = (byte) (Math.round(diff * Math.pow(10, mantissa)));
          i++;
        }
        return result;

      case DATA_SHORT:

        short[] shortResult = new short[value.length];

        for (double a : value) {
          BigDecimal val = BigDecimal.valueOf(a);
          double diff = max.subtract(val).doubleValue();
          shortResult[i] = (short) (Math.round(diff * Math.pow(10, mantissa)));
          i++;
        }
        return shortResult;

      case DATA_INT:

        int[] intResult = new int[value.length];

        for (double a : value) {
          BigDecimal val = BigDecimal.valueOf(a);
          double diff = max.subtract(val).doubleValue();
          intResult[i] = (int) (Math.round(diff * Math.pow(10, mantissa)));
          i++;
        }
        return intResult;

      case DATA_LONG:

        long[] longResult = new long[value.length];

        for (double a : value) {
          BigDecimal val = BigDecimal.valueOf(a);
          double diff = max.subtract(val).doubleValue();
          longResult[i] = (long) (Math.round(diff * Math.pow(10, mantissa)));
          i++;
        }
        return longResult;

      case DATA_FLOAT:

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
  public static ValueCompressonHolder.UnCompressValue getUnCompressNone(DataType compDataType,
      DataType actualDataType) {
    switch (compDataType) {
      case DATA_BYTE:
        return new UnCompressNoneByte(actualDataType);
      case DATA_SHORT:
        return new UnCompressNoneShort(actualDataType);
      case DATA_INT:
        return new UnCompressNoneInt(actualDataType);
      case DATA_LONG:
        return new UnCompressNoneLong(actualDataType);
      case DATA_FLOAT:
        return new UnCompressNoneFloat(actualDataType);
      default:
        return new UnCompressNoneDefault(actualDataType);
    }
  }

  /**
   * uncompress data 1. value = maxValue - subValue 2. value: int->double
   */
  public static ValueCompressonHolder.UnCompressValue getUnCompressDecimalMaxMin(DataType compDataType,
      DataType actualDataType) {
    switch (compDataType) {
      case DATA_BYTE:
        return new UnCompressMaxMinByte(actualDataType);
      case DATA_SHORT:
        return new UnCompressMaxMinShort(actualDataType);
      case DATA_INT:
        return new UnCompressMaxMinInt(actualDataType);
      case DATA_LONG:
        return new UnCompressMaxMinLong(actualDataType);
      case DATA_FLOAT:
        return new UnCompressMaxMinFloat(actualDataType);
      default:
        return new UnCompressMaxMinDefault(actualDataType);
    }
  }

  /**
   * uncompress data value = value/Math.pow(10, mantissa)
   */
  public static ValueCompressonHolder.UnCompressValue getUnCompressNonDecimal(DataType compDataType) {
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
   * uncompress data value = (maxValue - subValue)/Math.pow(10, mantissa)
   */
  public static ValueCompressonHolder.UnCompressValue getUnCompressNonDecimalMaxMin(
      DataType compDataType) {
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
   * Create Value compression model for write path
   */
  public static WriterCompressModel getWriterCompressModel(Object[] maxValue, Object[] minValue,
      int[] mantissa, Object[] uniqueValue, char[] aggType, byte[] dataTypeSelected) {
    MeasureMetaDataModel metaDataModel =
        new MeasureMetaDataModel(minValue, maxValue, mantissa, maxValue.length, uniqueValue,
            aggType, dataTypeSelected);
    return getWriterCompressModel(metaDataModel);
  }

  /**
   * Create Value compression model for write path
   */
  public static WriterCompressModel getWriterCompressModel(MeasureMetaDataModel measureMDMdl) {
    int measureCount = measureMDMdl.getMeasureCount();
    Object[] minValue = measureMDMdl.getMinValue();
    Object[] maxValue = measureMDMdl.getMaxValue();
    Object[] uniqueValue = measureMDMdl.getUniqueValue();
    int[] mantissa = measureMDMdl.getMantissa();
    char[] type = measureMDMdl.getType();
    byte[] dataTypeSelected = measureMDMdl.getDataTypeSelected();
    WriterCompressModel compressionModel = new WriterCompressModel();
    DataType[] actualType = new DataType[measureCount];
    DataType[] changedType = new DataType[measureCount];
    COMPRESSION_TYPE[] compType = new COMPRESSION_TYPE[measureCount];
    for (int i = 0; i < measureCount; i++) {
      CompressionFinder compresssionFinder =
          ValueCompressionUtil.getCompressionFinder(maxValue[i],
              minValue[i], mantissa[i], type[i], dataTypeSelected[i]);
      actualType[i] = compresssionFinder.actualDataType;
      changedType[i] = compresssionFinder.changedDataType;
      compType[i] = compresssionFinder.compType;
    }
    compressionModel.setMaxValue(maxValue);
    compressionModel.setMantissa(mantissa);
    compressionModel.setChangedDataType(changedType);
    compressionModel.setCompType(compType);
    compressionModel.setActualDataType(actualType);
    compressionModel.setMinValue(minValue);
    compressionModel.setUniqueValue(uniqueValue);
    compressionModel.setType(type);
    compressionModel.setDataTypeSelected(dataTypeSelected);
    ValueCompressonHolder.UnCompressValue[] values = ValueCompressionUtil
        .getUncompressedValues(compressionModel.getCompType(), compressionModel.getActualDataType(),
            compressionModel.getChangedDataType());
    compressionModel.setUnCompressValues(values);
    return compressionModel;
  }

  /**
   * Create Value compression model for read path
   */
  public static ReaderCompressModel getReaderCompressModel(ValueEncoderMeta meta) {
    ReaderCompressModel compressModel = new ReaderCompressModel();
    CompressionFinder compressFinder =
        getCompressionFinder(meta.getMaxValue(), meta.getMinValue(), meta.getMantissa(),
            meta.getType(), meta.getDataTypeSelected());
    compressModel.setUnCompressValues(
        ValueCompressionUtil.getUncompressedValues(
            compressFinder.compType,
            compressFinder.actualDataType,
            compressFinder.changedDataType));
    compressModel.setChangedDataType(compressFinder.changedDataType);
    compressModel.setValueEncoderMeta(meta);
    return compressModel;
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

  /**
   * use to identify the type of data.
   */
  public enum DataType {
    DATA_BYTE(),
    DATA_SHORT(),
    DATA_INT(),
    DATA_FLOAT(),
    DATA_LONG(),
    DATA_BIGINT(),
    DATA_DOUBLE();
    private DataType() {
    }

  }

  /**
   * through the size of data type,priority and compression type, select the
   * best compression type
   */
  private static class CompressionFinder implements Comparable<CompressionFinder> {

    private COMPRESSION_TYPE compType;

    private DataType actualDataType;

    private DataType changedDataType;
    /**
     * the size of changed data
     */
    private int size;

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
     * @param compType
     * @param actualDataType
     * @param changedDataType
     * @param priority
     */

    CompressionFinder(COMPRESSION_TYPE compType, DataType actualDataType, DataType changedDataType,
        PRIORITY priority) {
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
    enum PRIORITY {
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



