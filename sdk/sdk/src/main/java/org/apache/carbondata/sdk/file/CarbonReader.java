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

package org.apache.carbondata.sdk.file;

import java.io.IOException;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.keygenerator.directdictionary.timestamp.DateDirectDictionaryGenerator;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.scan.model.ProjectionDimension;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.hadoop.AbstractRecordReader;
import org.apache.carbondata.hadoop.CarbonRecordReader;
import org.apache.carbondata.hadoop.util.CarbonVectorizedRecordReader;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;


/**
 * Reader for CarbonData file
 */
@InterfaceAudience.User
@InterfaceStability.Evolving
public class CarbonReader<T> {

  private List<RecordReader<Void, T>> readers;

  private RecordReader<Void, T> currentReader;

  private int index;

  private boolean initialise;

  /**
   * save batch rows data
   */
  private Object[] batchRows;

  /**
   * Call {@link #builder(String)} to construct an instance
   */
  CarbonReader(List<RecordReader<Void, T>> readers) {
    if (readers == null) {
      // In case of pagination, initialize with no reader
      return;
    }
    this.initialise = true;
    this.readers = readers;
    this.index = 0;
    if (0 == readers.size()) {
      this.currentReader = null;
    } else {
      this.currentReader = readers.get(0);
    }
  }

  /**
   * Return true if has next row
   */
  public boolean hasNext() throws IOException, InterruptedException {
    if (0 == readers.size() || currentReader == null) {
      return false;
    }
    validateReader();
    if (currentReader.nextKeyValue()) {
      return true;
    } else {
      if (index == readers.size() - 1) {
        // no more readers
        return false;
      } else {
        // current reader is closed
        currentReader.close();
        // no need to keep a reference to CarbonVectorizedRecordReader,
        // until all the readers are processed.
        // If readers count is very high,
        // we get OOM as GC not happened for any of the content in CarbonVectorizedRecordReader
        readers.set(index, null);
        index++;
        currentReader = readers.get(index);
        boolean hasNext = currentReader.nextKeyValue();
        if (hasNext) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Read and return next row object
   */
  public T readNextRow() throws IOException, InterruptedException {
    validateReader();
    T row = currentReader.getCurrentValue();
    // For CarbonRow date and timestamp are already in their correct form
    if (row instanceof CarbonRow) {
      return row;
    }
    return formatDateAndTimeStamp((Object []) row);
  }

  /**
   * This method converts the date and timestamp columns into right format. Before conversion date
   * is present as integer and timestamp is present as long. This method also flattens complex
   * columns and format the date/timestamp child present in them.
   */
  public Object getFormattedData(CarbonDimension dimension, Object row, SimpleDateFormat dateFormat,
      SimpleDateFormat timeStampFormat) {
    ColumnSchema columnSchema = dimension.getColumnSchema();
    if (row != null && columnSchema != null) {
      DataType dataType = columnSchema.getDataType();
      if (dataType == DataTypes.DATE) {
        return dateFormat
            .format(new Date(DateDirectDictionaryGenerator.MILLIS_PER_DAY * (int) row));
      } else if (dataType == DataTypes.TIMESTAMP) {
        return timeStampFormat.format(new Date((long) row / 1000));
      } else if (dataType.isComplexType()) {
        List<CarbonDimension> listOfChildDimensions = dimension.getListOfChildDimensions();
        Object[] childDimensionFormattedValues = new Object[((Object[]) row).length];
        if (listOfChildDimensions != null && listOfChildDimensions.size() > 0) {
          int i = 0;
          if (DataTypes.isArrayType(dataType)) {
            DataType childDataType = listOfChildDimensions.get(0).getColumnSchema().getDataType();
            if (childDataType == DataTypes.DATE || childDataType == DataTypes.TIMESTAMP
                || childDataType.isComplexType()) {
              for (Object val : (Object[]) row) {
                childDimensionFormattedValues[i] =
                   getFormattedData(listOfChildDimensions.get(0), val, dateFormat, timeStampFormat);
                i++;
              }
            } else {
              return row;
            }
          } else if (DataTypes.isStructType(dataType)) {
            for (Object val : (Object[]) row) {
              childDimensionFormattedValues[i] =
                  getFormattedData(listOfChildDimensions.get(i), val, dateFormat, timeStampFormat);
              i++;
            }
          } else if (DataTypes.isMapType(dataType)) {
            CarbonDimension childDimension = listOfChildDimensions.get(0);
            ColumnSchema childSchema = childDimension.getColumnSchema();
            DataType childDataType = childSchema.getDataType();
            if (DataTypes.isStructType(childDataType)) {
              /* Map is returned as array of keys and values. So convert map childrens
               * (key and value) into array dimension before processing
               */
              List<CarbonDimension> mapChilds = childDimension.getListOfChildDimensions();
              ColumnSchema arraySchema = childSchema.clone();
              arraySchema.setDataType(
                  DataTypes.createArrayType(mapChilds.get(i).getColumnSchema().getDataType()));
              CarbonDimension arrayDimension = new CarbonDimension(arraySchema,
                  childDimension.getOrdinal(), childDimension.getKeyOrdinal(),
                  childDimension.getSchemaOrdinal());
              for (Object val : (Object[]) row) {
                arrayDimension.initializeChildDimensionsList(1);
                arrayDimension.getListOfChildDimensions().add(mapChilds.get(i));
                childDimensionFormattedValues[i] =
                    getFormattedData(arrayDimension, val, dateFormat, timeStampFormat);
                i++;
              }
            }
          }
          return childDimensionFormattedValues;
        }
      }
    }
    return row;
  }

  public T formatDateAndTimeStamp(Object[] row) {
    List<ProjectionDimension> dimensions = ((AbstractRecordReader) currentReader)
            .getQueryModel().getProjectionDimensions();
    String carbonDateFormat = CarbonProperties.getInstance()
            .getProperty(CarbonCommonConstants.CARBON_DATE_FORMAT);
    if (carbonDateFormat == null) {
      carbonDateFormat = CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT;
    }
    SimpleDateFormat dateFormat = new SimpleDateFormat(carbonDateFormat);
    String carbonTimeStampFormat = CarbonProperties.getInstance()
            .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT);
    if (carbonTimeStampFormat  == null) {
      carbonTimeStampFormat  = CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT;
    }
    SimpleDateFormat timeStampFormat = new SimpleDateFormat(carbonTimeStampFormat);
    for (ProjectionDimension dimension : dimensions) {
      row[dimension.getOrdinal()] = getFormattedData(dimension.getDimension(),
          row[dimension.getOrdinal()], dateFormat, timeStampFormat);
    }
    return (T)row;
  }

  /**
   * Read and return next batch row objects
   */
  public Object[] readNextBatchRow() throws Exception {
    validateReader();
    if (currentReader instanceof CarbonRecordReader) {
      List<Object> batchValue = ((CarbonRecordReader) currentReader).getBatchValue();
      if (batchValue == null) {
        return null;
      } else {
        Object[] rows =  batchValue.toArray();
        for (Object row : rows) {
          row = formatDateAndTimeStamp((Object[]) row);
        }
        return rows;
      }
    } else if (currentReader instanceof CarbonVectorizedRecordReader) {
      int batch = Integer.parseInt(CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.DETAIL_QUERY_BATCH_SIZE,
              String.valueOf(CarbonCommonConstants.DETAIL_QUERY_BATCH_SIZE_DEFAULT)));
      batchRows = new Object[batch];
      int sum = 0;
      for (int i = 0; i < batch; i++) {
        batchRows[i] = formatDateAndTimeStamp((Object[]) currentReader.getCurrentValue());
        sum++;
        if (i != batch - 1) {
          if (!hasNext()) {
            Object[] lessBatch = new Object[sum];
            for (int j = 0; j < sum; j++) {
              lessBatch[j] = batchRows[j];
            }
            return lessBatch;
          }
        }
      }
      return batchRows;
    } else {
      throw new Exception("Didn't support read next batch row by this reader.");
    }
  }

  /**
   * Return a new {@link CarbonReaderBuilder} instance
   *
   * @param tablePath table store path
   * @param tableName table name
   * @return CarbonReaderBuilder object
   */
  public static CarbonReaderBuilder builder(String tablePath, String tableName) {
    return new CarbonReaderBuilder(tablePath, tableName);
  }

  /**
   * Return a new {@link CarbonReaderBuilder} instance
   *
   * @param inputSplit CarbonInputSplit Object
   * @return CarbonReaderBuilder object
   */
  public static CarbonReaderBuilder builder(InputSplit inputSplit) {
    return new CarbonReaderBuilder(inputSplit);
  }

  /**
   * Return a new {@link CarbonReaderBuilder} instance
   *
   * @param tablePath table path
   * @return CarbonReaderBuilder object
   */
  public static CarbonReaderBuilder builder(String tablePath) {
    UUID uuid = UUID.randomUUID();
    String tableName = "UnknownTable" + uuid;
    return builder(tablePath, tableName);
  }

  /**
   * Return a new {@link CarbonReaderBuilder} instance
   *
   * @return CarbonReaderBuilder object
   */
  public static CarbonReaderBuilder builder() {
    UUID uuid = UUID.randomUUID();
    String tableName = "UnknownTable" + uuid;
    return new CarbonReaderBuilder(tableName);
  }

  /**
   * Breaks the list of CarbonRecordReader in CarbonReader into multiple
   * CarbonReader objects, each iterating through some 'carbondata' files
   * and return that list of CarbonReader objects
   *
   * If the no. of files is greater than maxSplits, then break the
   * CarbonReader into maxSplits splits, with each split iterating
   * through >= 1 file.
   *
   * If the no. of files is less than maxSplits, then return list of
   * CarbonReader with size as the no. of files, with each CarbonReader
   * iterating through exactly one file
   *
   * @param maxSplits: Int
   * @return list of {@link CarbonReader} objects
   */
  public List<CarbonReader> split(int maxSplits) {
    validateReader();
    if (maxSplits < 1) {
      throw new RuntimeException(
          this.getClass().getSimpleName() + ".split: maxSplits must be positive");
    }

    List<CarbonReader> carbonReaders = new ArrayList<>();

    if (maxSplits < this.readers.size()) {
      // If maxSplits is less than the no. of files
      // Split the reader into maxSplits splits with each
      // element containing >= 1 CarbonRecordReader objects
      float filesPerSplit = (float) this.readers.size() / maxSplits;
      for (int i = 0; i < maxSplits; ++i) {
        carbonReaders.add(new CarbonReader<>(this.readers.subList(
            (int) Math.ceil(i * filesPerSplit),
            (int) Math.ceil(((i + 1) * filesPerSplit)))));
      }
    } else {
      // If maxSplits is greater than the no. of files
      // Split the reader into <num_files> splits with each
      // element contains exactly 1 CarbonRecordReader object
      for (int i = 0; i < this.readers.size(); ++i) {
        carbonReaders.add(new CarbonReader<>(this.readers.subList(i, i + 1)));
      }
    }

    // This is to disable the use of this CarbonReader object to iterate
    // over the files and forces user to only use the returned splits
    this.initialise = false;

    return carbonReaders;
  }

  /**
   * Close reader
   *
   * @throws IOException
   */
  public void close() throws IOException {
    validateReader();
    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.DETAIL_QUERY_BATCH_SIZE,
            String.valueOf(CarbonCommonConstants.DETAIL_QUERY_BATCH_SIZE_DEFAULT));
    if (null != this.currentReader) {
      this.currentReader.close();
    }
    this.initialise = false;
  }

  /**
   * Validate the reader
   */
  private void validateReader() {
    if (!this.initialise) {
      throw new RuntimeException(this.getClass().getSimpleName() +
          " not initialise, please create it first.");
    }
  }
}
