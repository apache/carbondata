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

package org.apache.carbondata.hadoop.stream;

import java.io.IOException;
import java.lang.reflect.Constructor;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.scan.complextypes.ArrayQueryType;
import org.apache.carbondata.core.scan.complextypes.PrimitiveQueryType;
import org.apache.carbondata.core.scan.complextypes.StructQueryType;
import org.apache.carbondata.core.scan.filter.GenericQueryType;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.hadoop.InputMetricsStats;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * Stream input format
 */
public class CarbonStreamInputFormat extends FileInputFormat<Void, Object> {

  public static final String READ_BUFFER_SIZE = "carbon.stream.read.buffer.size";
  public static final String READ_BUFFER_SIZE_DEFAULT = "65536";
  public static final String STREAM_RECORD_READER_INSTANCE =
      "org.apache.carbondata.stream.CarbonStreamRecordReader";
  // return raw row for hand off
  private boolean useRawRow = false;

  public void setUseRawRow(boolean useRawRow) {
    this.useRawRow = useRawRow;
  }

  public void setInputMetricsStats(InputMetricsStats inputMetricsStats) {
    this.inputMetricsStats = inputMetricsStats;
  }

  public void setIsVectorReader(boolean vectorReader) {
    isVectorReader = vectorReader;
  }

  public void setModel(QueryModel model) {
    this.model = model;
  }

  // InputMetricsStats
  private InputMetricsStats inputMetricsStats;
  // vector reader
  private boolean isVectorReader;
  private QueryModel model;

  @Override
  public RecordReader<Void, Object> createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException {
    try {
      Constructor cons = CarbonStreamUtils
          .getConstructorWithReflection(STREAM_RECORD_READER_INSTANCE, boolean.class,
              InputMetricsStats.class, QueryModel.class, boolean.class);
      return (RecordReader) CarbonStreamUtils
          .getInstanceWithReflection(cons, isVectorReader, inputMetricsStats, model, useRawRow);

    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public static GenericQueryType[] getComplexDimensions(CarbonColumn[] carbonColumns) {
    GenericQueryType[] queryTypes = new GenericQueryType[carbonColumns.length];
    for (int i = 0; i < carbonColumns.length; i++) {
      if (carbonColumns[i].isComplex()) {
        if (DataTypes.isArrayType(carbonColumns[i].getDataType())) {
          queryTypes[i] = new ArrayQueryType(carbonColumns[i].getColName(),
              carbonColumns[i].getColName(), i);
        } else if (DataTypes.isStructType(carbonColumns[i].getDataType())) {
          queryTypes[i] = new StructQueryType(carbonColumns[i].getColName(),
              carbonColumns[i].getColName(), i);
        } else {
          throw new UnsupportedOperationException(
              carbonColumns[i].getDataType().getName() + " is not supported");
        }

        fillChildren(queryTypes[i], (CarbonDimension) carbonColumns[i], i);
      }
    }

    return queryTypes;
  }

  private static void fillChildren(GenericQueryType parentQueryType, CarbonDimension dimension,
      int parentColumnIndex) {
    for (int i = 0; i < dimension.getNumberOfChild(); i++) {
      CarbonDimension child = dimension.getListOfChildDimensions().get(i);
      DataType dataType = child.getDataType();
      GenericQueryType queryType;
      if (DataTypes.isArrayType(dataType)) {
        queryType =
            new ArrayQueryType(child.getColName(), dimension.getColName(), ++parentColumnIndex);

      } else if (DataTypes.isStructType(dataType)) {
        queryType =
            new StructQueryType(child.getColName(), dimension.getColName(), ++parentColumnIndex);
        parentQueryType.addChildren(queryType);
      } else {
        boolean isDirectDictionary =
            CarbonUtil.hasEncoding(child.getEncoder(), Encoding.DIRECT_DICTIONARY);
        queryType =
            new PrimitiveQueryType(child.getColName(), dimension.getColName(), ++parentColumnIndex,
                child.getDataType(), isDirectDictionary);
      }
      parentQueryType.addChildren(queryType);
      if (child.getNumberOfChild() > 0) {
        fillChildren(queryType, child, parentColumnIndex);
      }
    }
  }
}
