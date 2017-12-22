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

package org.apache.carbondata.processing.loading.sort;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;

import org.apache.carbondata.core.datastore.row.WriteStepRowUtil;
import org.apache.carbondata.core.memory.CarbonUnsafe;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.core.util.NonDictionaryUtil;
import org.apache.carbondata.processing.sort.sortdata.SortParameters;
import org.apache.carbondata.processing.sort.sortdata.TableFieldStat;

/**
 * This class is used to in sort procedure, it mainly handle 3-parted row format. We can use it to
 * 1. convert raw row to 3-parted row;
 * 2. read/write 3-parted row from/to sort temp files during intermediate merge
 * 3. read/write 3-parted/raw from/to unsafe memory during unsafe in-memory merge
 */
public class SortStepRowHandler implements Serializable {
  private static final long serialVersionUID = 1L;
  private TableFieldStat tableFieldStat;

  /**
   * constructor
   * @param tableFieldStat
   */
  public SortStepRowHandler(TableFieldStat tableFieldStat) {
    this.tableFieldStat = tableFieldStat;
  }

  /**
   * constructor
   * @param sortParameters
   */
  public SortStepRowHandler(SortParameters sortParameters) {
    this.tableFieldStat = new TableFieldStat(sortParameters);
  }

  /**
   * convert carbon row from raw format to 3-parted format
   * @param row raw row whose length is the same as field number
   * @return 3-parted row whose length is 3. (1 for dict dims ,1 for non-dict and complex,
   * 1 for measures)
   */
  public Object[] convertRawRowTo3Parts(Object[] row) {
    Object[] holder = new Object[3];
    try {
      int[] dictDims = new int[tableFieldStat.getDictDimCnt()];
      byte[][] nonDictArray =
          new byte[tableFieldStat.getNoDictDimCnt() + tableFieldStat.getComplexDimCnt()][];
      Object[] measures = new Object[tableFieldStat.getMeasureCnt()];

      // write dict dim data
      for (int idx = 0; idx < tableFieldStat.getDictDimIdx().length; idx++) {
        dictDims[idx] = (int) row[tableFieldStat.getDictDimIdx()[idx]];
      }

      // write non dict dim data
      for (int idx = 0; idx < tableFieldStat.getAllNonDictDimIdx().length; idx++) {
        nonDictArray[idx] = (byte[]) row[tableFieldStat.getAllNonDictDimIdx()[idx]];
      }

      // write measure data
      for (int idx = 0; idx < tableFieldStat.getMeasureIdx().length; idx++) {
        measures[idx] = row[tableFieldStat.getMeasureIdx()[idx]];
      }
      NonDictionaryUtil.prepareOutObj(holder, dictDims, nonDictArray, measures);
    } catch (Exception e) {
      throw new RuntimeException("Problem while converting row to 3 parts", e);
    }
    return holder;
  }

  /**
   * read one parted row from InputStream
   * @param inputStream input stream
   * @return a row that contains three parts
   */
  public Object[] readPartedRowFromInputStream(DataInputStream inputStream) {
    Object[] holder = new Object[3];
    try {
      int[] dictDims = new int[tableFieldStat.getDictDimCnt()];
      byte[][] nonDictArray =
          new byte[tableFieldStat.getNoDictDimCnt() + tableFieldStat.getComplexDimCnt()][];
      Object[] measures = new Object[tableFieldStat.getMeasureCnt()];

      // read dict dim
      for (int idx = 0; idx < dictDims.length; idx++) {
        dictDims[idx] = inputStream.readInt();
      }

      // read non dict dim, containing complex
      for (int idx = 0; idx < nonDictArray.length; idx++) {
        short len = inputStream.readShort();
        byte[] bytes = new byte[len];
        inputStream.readFully(bytes);
        nonDictArray[idx] = bytes;
      }

      // read measure
      for (int idx = 0; idx < measures.length; idx++) {
        if (inputStream.readByte() == 1) {
          DataType dataType = tableFieldStat.getMeasureDataType()[idx];
          if (dataType == DataTypes.BOOLEAN) {
            measures[idx] = inputStream.readBoolean();
          } else if (dataType == DataTypes.SHORT) {
            measures[idx] = inputStream.readShort();
          } else if (dataType == DataTypes.INT) {
            measures[idx] = inputStream.readInt();
          } else if (dataType == DataTypes.LONG) {
            measures[idx] = inputStream.readLong();
          } else if (dataType == DataTypes.DOUBLE) {
            measures[idx] = inputStream.readDouble();
          } else if (DataTypes.isDecimal(dataType)) {
            int len = inputStream.readInt();
            byte[] buff = new byte[len];
            inputStream.readFully(buff);
            measures[idx] = DataTypeUtil.byteToBigDecimal(buff);
          } else {
            throw new IllegalArgumentException("unsupported data type:"
                + tableFieldStat.getMeasureDataType()[idx]);
          }
        } else {
          measures[idx] = null;
        }
      }
      NonDictionaryUtil.prepareOutObj(holder, dictDims, nonDictArray, measures);

    } catch (IOException e) {
      throw new RuntimeException("Encounter problem while reading row from stream ", e);
    }
    return holder;
  }

  /**
   * write one parted row to OutputStream
   *
   * @param row row
   * @param outputStream output stream
   */
  public void writePartedRowToOutputStream(Object[] row, DataOutputStream outputStream) {
    try {
      int[] dictDims = (int[]) row[WriteStepRowUtil.DICTIONARY_DIMENSION];
      byte[][] nonDictArray = (byte[][]) row[WriteStepRowUtil.NO_DICTIONARY_AND_COMPLEX];
      Object[] measures = (Object[]) row[WriteStepRowUtil.MEASURE];

      // write dict dim
      for (int idx = 0; idx < dictDims.length; idx++) {
        outputStream.writeInt(dictDims[idx]);
      }

      // write non dict dim, containing complex
      for (int idx = 0; idx < nonDictArray.length; idx++) {
        byte[] bytes = nonDictArray[idx];
        outputStream.writeShort(bytes.length);
        outputStream.write(bytes);
      }

      // write measure
      for (int idx = 0; idx < measures.length; idx++) {
        Object value = measures[idx];
        if (null != value) {
          outputStream.write((byte) 1);
          DataType dataType = tableFieldStat.getMeasureDataType()[idx];
          if (dataType == DataTypes.BOOLEAN) {
            outputStream.writeBoolean((boolean) value);
          } else if (dataType == DataTypes.SHORT) {
            outputStream.writeShort((Short) value);
          } else if (dataType == DataTypes.INT) {
            outputStream.writeInt((Integer) value);
          } else if (dataType == DataTypes.LONG) {
            outputStream.writeLong((Long) value);
          } else if (dataType == DataTypes.DOUBLE) {
            outputStream.writeDouble((Double) value);
          } else if (DataTypes.isDecimal(dataType)) {
            byte[] decimalBytes = DataTypeUtil.bigDecimalToByte((BigDecimal) value);
            outputStream.writeInt(decimalBytes.length);
            outputStream.write(decimalBytes);
          } else {
            throw new IllegalArgumentException(
                "unsupported data type:" + tableFieldStat.getMeasureDataType()[idx]);
          }
        } else {
          outputStream.write((byte) 0);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("Encounter problem while writing row to stream ", e);
    }
  }

  /**
   * read parted row from unsafe memory
   * @param baseObject base object of memory block
   * @param address address of the row
   * @return a 3-parted row
   */
  public Object[] readPartedRowFromUnsafeMemory(Object baseObject, long address) {
    int size = 0;

    int[] dictDims = new int[tableFieldStat.getDictDimCnt()];
    byte[][] nonDictArray
        = new byte[tableFieldStat.getNoDictDimCnt() + tableFieldStat.getComplexDimCnt()][];
    Object[] measures = new Object[tableFieldStat.getMeasureCnt()];

    // read dict dimension
    for (int idx = 0; idx < dictDims.length; idx++) {
      dictDims[idx] = CarbonUnsafe.getUnsafe().getInt(baseObject, address + size);
      size += 4;
    }

    // read non dict & complex
    for (int idx = 0; idx < nonDictArray.length; idx++) {
      short length = CarbonUnsafe.getUnsafe().getShort(baseObject, address + size);
      size += 2;
      byte[] bytes = new byte[length];
      CarbonUnsafe.getUnsafe().copyMemory(baseObject, address + size,
          bytes, CarbonUnsafe.BYTE_ARRAY_OFFSET, length);
      size += length;
      nonDictArray[idx] = bytes;
    }

    // read measure
    for (int idx = 0; idx < measures.length; idx++) {
      byte notNullFlag = CarbonUnsafe.getUnsafe().getByte(baseObject, address + size);
      size += 1;
      if (1 == notNullFlag) {
        DataType dataType = tableFieldStat.getMeasureDataType()[idx];
        if (dataType == DataTypes.BOOLEAN) {
          measures[idx] = CarbonUnsafe.getUnsafe().getBoolean(baseObject, address + size);
          size += 1;
        } else if (dataType == DataTypes.SHORT) {
          measures[idx] = CarbonUnsafe.getUnsafe().getShort(baseObject, address + size);
          size += 2;
        } else if (dataType == DataTypes.INT) {
          measures[idx] = CarbonUnsafe.getUnsafe().getInt(baseObject, address + size);
          size += 4;
        } else if (dataType == DataTypes.LONG) {
          measures[idx] = CarbonUnsafe.getUnsafe().getLong(baseObject, address + size);
          size += 8;
        } else if (dataType == DataTypes.DOUBLE) {
          measures[idx] = CarbonUnsafe.getUnsafe().getDouble(baseObject, address + size);
          size += 8;
        } else if (DataTypes.isDecimal(dataType)) {
          short length = CarbonUnsafe.getUnsafe().getShort(baseObject, address + size);
          byte[] decimalBytes = new byte[length];
          size += 2;
          CarbonUnsafe.getUnsafe().copyMemory(baseObject, address + size, decimalBytes,
              CarbonUnsafe.BYTE_ARRAY_OFFSET, length);
          measures[idx] = DataTypeUtil.byteToBigDecimal(decimalBytes);
          size += length;
        } else {
          throw new IllegalArgumentException("unsupported data type:" + dataType);
        }
      } else {
        measures[idx] = null;
      }
    }

    Object[] rtn = new Object[3];
    NonDictionaryUtil.prepareOutObj(rtn, dictDims, nonDictArray, measures);
    return rtn;
  }

  /**
   * write raw row as 3-parted to unsafe memory
   * @param row 3-parted row
   * @param baseObject base object of the memory block
   * @param address address of the row
   * @return memory size consumed
   */
  public int writeRawRowAsPartedToUnsafeMemory(Object[] row, Object baseObject, long address) {
    if (row == null) {
      throw new RuntimeException("Row is null ??");
    }
    int size = 0;
    int[] dictDimIdx = tableFieldStat.getDictDimIdx();
    int[] nonDictIdx = tableFieldStat.getAllNonDictDimIdx();
    int[] measureIdx = tableFieldStat.getMeasureIdx();

    // put dict dimension
    for (int idx = 0; idx < dictDimIdx.length; idx++) {
      CarbonUnsafe.getUnsafe().putInt(baseObject, address + size, (int) row[dictDimIdx[idx]]);
      size += 4;
    }

    // put non dict & complex
    for (int idx = 0; idx < nonDictIdx.length; idx++) {
      byte[] bytes = (byte[]) row[nonDictIdx[idx]];
      CarbonUnsafe.getUnsafe().putShort(baseObject, address + size, (short) bytes.length);
      size += 2;
      CarbonUnsafe.getUnsafe().copyMemory(bytes, CarbonUnsafe.BYTE_ARRAY_OFFSET,
          baseObject, address + size, bytes.length);
      size += bytes.length;
    }

    // put measure
    for (int idx = 0; idx < measureIdx.length; idx++) {
      Object value = row[measureIdx[idx]];
      if (null != value) {
        CarbonUnsafe.getUnsafe().putByte(baseObject, address + size, (byte) 1);
        size += 1;
        DataType dataType = tableFieldStat.getMeasureDataType()[idx];
        if (dataType == DataTypes.BOOLEAN) {
          CarbonUnsafe.getUnsafe().putBoolean(baseObject, address + size, (Boolean) value);
          size += 1;
        } else if (dataType == DataTypes.SHORT) {
          CarbonUnsafe.getUnsafe().putShort(baseObject, address + size, (Short) value);
          size += 2;
        } else if (dataType == DataTypes.INT) {
          CarbonUnsafe.getUnsafe().putInt(baseObject, address + size, (Integer) value);
          size += 4;
        } else if (dataType == DataTypes.LONG) {
          CarbonUnsafe.getUnsafe().putLong(baseObject, address + size, (Long) value);
          size += 8;
        } else if (dataType == DataTypes.DOUBLE) {
          CarbonUnsafe.getUnsafe().putDouble(baseObject, address + size, (Double) value);
          size += 8;
        } else if (DataTypes.isDecimal(dataType)) {
          byte[] decimalBytes = DataTypeUtil.bigDecimalToByte((BigDecimal) value);
          CarbonUnsafe.getUnsafe()
              .putShort(baseObject, address + size, (short) decimalBytes.length);
          size += 2;
          CarbonUnsafe.getUnsafe()
              .copyMemory(decimalBytes, CarbonUnsafe.BYTE_ARRAY_OFFSET, baseObject,
                  address + size, decimalBytes.length);
          size += decimalBytes.length;
        } else {
          throw new IllegalArgumentException("unsupported data type:" + dataType);
        }
      } else {
        CarbonUnsafe.getUnsafe().putByte(baseObject, address + size, (byte) 0);
        size += 1;
      }
    }

    return size;
  }
}