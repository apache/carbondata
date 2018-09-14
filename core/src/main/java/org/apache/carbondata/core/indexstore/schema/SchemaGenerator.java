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

package org.apache.carbondata.core.indexstore.schema;

import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.util.BlockletDataMapUtil;

/**
 * class for creating schema for a given DataMap
 */
public class SchemaGenerator {

  /**
   * Method for creating blocklet Schema. Each blocklet row will share the same schema
   *
   * @param segmentProperties
   * @return
   */
  public static CarbonRowSchema[] createBlockSchema(SegmentProperties segmentProperties,
      List<CarbonColumn> minMaxCacheColumns) {
    List<CarbonRowSchema> indexSchemas = new ArrayList<>();
    // get MinMax Schema
    getMinMaxSchema(segmentProperties, indexSchemas, minMaxCacheColumns);
    // for number of rows.
    indexSchemas.add(new CarbonRowSchema.FixedCarbonRowSchema(DataTypes.INT));
    // for table block path
    indexSchemas.add(new CarbonRowSchema.VariableCarbonRowSchema(DataTypes.BYTE_ARRAY));
    // for version number.
    indexSchemas.add(new CarbonRowSchema.FixedCarbonRowSchema(DataTypes.SHORT));
    // for schema updated time.
    indexSchemas.add(new CarbonRowSchema.FixedCarbonRowSchema(DataTypes.LONG));
    // for block footer offset.
    indexSchemas.add(new CarbonRowSchema.FixedCarbonRowSchema(DataTypes.LONG));
    // for locations
    indexSchemas.add(new CarbonRowSchema.VariableCarbonRowSchema(DataTypes.BYTE_ARRAY));
    // for storing block length.
    indexSchemas.add(new CarbonRowSchema.FixedCarbonRowSchema(DataTypes.LONG));
    // for storing min max flag for each column which reflects whether min max for a column is
    // written in the metadata or not.
    addMinMaxFlagSchema(indexSchemas, segmentProperties);
    CarbonRowSchema[] schema = indexSchemas.toArray(new CarbonRowSchema[indexSchemas.size()]);
    return schema;
  }

  /**
   * Method for creating blocklet Schema. Each blocklet row will share the same schema
   *
   * @param segmentProperties
   * @return
   */
  public static CarbonRowSchema[] createBlockletSchema(SegmentProperties segmentProperties,
      List<CarbonColumn> minMaxCacheColumns) {
    List<CarbonRowSchema> indexSchemas = new ArrayList<>();
    // get MinMax Schema
    getMinMaxSchema(segmentProperties, indexSchemas, minMaxCacheColumns);
    // for number of rows.
    indexSchemas.add(new CarbonRowSchema.FixedCarbonRowSchema(DataTypes.INT));
    // for table block path
    indexSchemas.add(new CarbonRowSchema.VariableCarbonRowSchema(DataTypes.BYTE_ARRAY));
    // for version number.
    indexSchemas.add(new CarbonRowSchema.FixedCarbonRowSchema(DataTypes.SHORT));
    // for schema updated time.
    indexSchemas.add(new CarbonRowSchema.FixedCarbonRowSchema(DataTypes.LONG));
    // for block footer offset.
    indexSchemas.add(new CarbonRowSchema.FixedCarbonRowSchema(DataTypes.LONG));
    // for locations
    indexSchemas.add(new CarbonRowSchema.VariableCarbonRowSchema(DataTypes.BYTE_ARRAY));
    // for storing block length.
    indexSchemas.add(new CarbonRowSchema.FixedCarbonRowSchema(DataTypes.LONG));
    // for storing min max flag for each column which reflects whether min max for a column is
    // written in the metadata or not.
    addMinMaxFlagSchema(indexSchemas, segmentProperties);
    //for blocklet info
    indexSchemas.add(new CarbonRowSchema.VariableCarbonRowSchema(DataTypes.BYTE_ARRAY));
    // for number of pages.
    indexSchemas.add(new CarbonRowSchema.FixedCarbonRowSchema(DataTypes.SHORT));
    // for relative blocklet id i.e. blocklet id that belongs to a particular part file
    indexSchemas.add(new CarbonRowSchema.FixedCarbonRowSchema(DataTypes.SHORT));
    CarbonRowSchema[] schema = indexSchemas.toArray(new CarbonRowSchema[indexSchemas.size()]);
    return schema;
  }

  /**
   * Creates the schema to store summary information or the information which can be stored only
   * once per datamap. It stores datamap level max/min of each column and partition information of
   * datamap
   *
   * @param segmentProperties
   * @throws MemoryException
   */
  public static CarbonRowSchema[] createTaskSummarySchema(SegmentProperties segmentProperties,
      List<CarbonColumn> minMaxCacheColumns,
      boolean storeBlockletCount, boolean filePathToBeStored) throws MemoryException {
    List<CarbonRowSchema> taskMinMaxSchemas = new ArrayList<>();
    // get MinMax Schema
    getMinMaxSchema(segmentProperties, taskMinMaxSchemas, minMaxCacheColumns);
    // for storing file name
    taskMinMaxSchemas
        .add(new CarbonRowSchema.VariableCarbonRowSchema(DataTypes.BYTE_ARRAY));
    // for storing segmentid
    taskMinMaxSchemas
        .add(new CarbonRowSchema.VariableCarbonRowSchema(DataTypes.BYTE_ARRAY));
    // for storing min max flag for each column which reflects whether min max for a column is
    // written in the metadata or not.
    addMinMaxFlagSchema(taskMinMaxSchemas, segmentProperties);
    // store path only in case of partition table or non transactional table
    if (filePathToBeStored) {
      // for storing file path
      taskMinMaxSchemas
          .add(new CarbonRowSchema.VariableCarbonRowSchema(DataTypes.BYTE_ARRAY));
    }
    // flag to check whether it is required to store blocklet count of each carbondata file as
    // binary in summary schema. This will be true when it is not a legacy store (>1.1 version)
    // and CACHE_LEVEL=BLOCK
    if (storeBlockletCount) {
      taskMinMaxSchemas.add(new CarbonRowSchema.VariableCarbonRowSchema(DataTypes.BYTE_ARRAY));
    }
    CarbonRowSchema[] schema =
        taskMinMaxSchemas.toArray(new CarbonRowSchema[taskMinMaxSchemas.size()]);
    return schema;
  }

  /**
   * Method to create schema for storing min/max data
   *
   * @param segmentProperties
   * @param minMaxSchemas
   */
  private static void getMinMaxSchema(SegmentProperties segmentProperties,
      List<CarbonRowSchema> minMaxSchemas, List<CarbonColumn> minMaxCacheColumns) {
    // Index key
    int[] minMaxLen = getMinMaxLength(segmentProperties, minMaxCacheColumns);
    int[] columnOrdinals = getColumnOrdinalsToAccess(segmentProperties, minMaxCacheColumns);
    // do it 2 times, one for min and one for max.
    for (int k = 0; k < 2; k++) {
      CarbonRowSchema[] mapSchemas = new CarbonRowSchema[minMaxLen.length];
      for (int i = 0; i < minMaxLen.length; i++) {
        if (minMaxLen[i] <= 0) {
          boolean isVarchar = false;
          if (columnOrdinals[i] < segmentProperties.getDimensions().size()
              && segmentProperties.getDimensions().get(columnOrdinals[i]).getDataType()
              == DataTypes.VARCHAR) {
            isVarchar = true;
          }
          mapSchemas[i] =
              new CarbonRowSchema.VariableCarbonRowSchema(DataTypes.BYTE_ARRAY, isVarchar);
        } else {
          mapSchemas[i] =
              new CarbonRowSchema.FixedCarbonRowSchema(DataTypes.BYTE_ARRAY, minMaxLen[i]);
        }
      }
      CarbonRowSchema mapSchema =
          new CarbonRowSchema.StructCarbonRowSchema(DataTypes.createDefaultStructType(),
              mapSchemas);
      minMaxSchemas.add(mapSchema);
    }
  }

  /**
   * Method to add min max flag schema for all the dimensions
   *
   * @param indexSchemas
   * @param segmentProperties
   */
  private static void addMinMaxFlagSchema(List<CarbonRowSchema> indexSchemas,
      SegmentProperties segmentProperties) {
    int totalDimensions = segmentProperties.getDimensions().size();
    CarbonRowSchema[] minMaxFlagSchemas = new CarbonRowSchema[totalDimensions];
    for (int i = 0; i < totalDimensions; i++) {
      minMaxFlagSchemas[i] = new CarbonRowSchema.FixedCarbonRowSchema(DataTypes.BOOLEAN);
    }
    CarbonRowSchema structMinMaxFlagSchema =
        new CarbonRowSchema.StructCarbonRowSchema(DataTypes.createDefaultStructType(),
            minMaxFlagSchemas);
    indexSchemas.add(structMinMaxFlagSchema);
  }

  /**
   * Method to get the min max length of each column. It will return the length of only column
   * which will be cached
   *
   * @param segmentProperties
   * @param minMaxCacheColumns
   * @return
   */
  private static int[] getMinMaxLength(SegmentProperties segmentProperties,
      List<CarbonColumn> minMaxCacheColumns) {
    int[] minMaxLen = null;
    if (null != minMaxCacheColumns) {
      minMaxLen = new int[minMaxCacheColumns.size()];
      int counter = 0;
      for (CarbonColumn column : minMaxCacheColumns) {
        minMaxLen[counter++] = segmentProperties.getColumnsValueSize()[BlockletDataMapUtil
            .getColumnOrdinal(segmentProperties, column)];
      }
    } else {
      minMaxLen = segmentProperties.getColumnsValueSize();
    }
    return minMaxLen;
  }

  /**
   * Method to fill the column ordinals to access based on the columns to be cached
   *
   * @param segmentProperties
   * @param minMaxCacheColumns
   * @return
   */
  private static int[] getColumnOrdinalsToAccess(SegmentProperties segmentProperties,
      List<CarbonColumn> minMaxCacheColumns) {
    int[] columnOrdinalsTOAccess = null;
    if (null != minMaxCacheColumns) {
      columnOrdinalsTOAccess = new int[minMaxCacheColumns.size()];
      int counter = 0;
      for (CarbonColumn column : minMaxCacheColumns) {
        columnOrdinalsTOAccess[counter++] =
            BlockletDataMapUtil.getColumnOrdinal(segmentProperties, column);
      }
    } else {
      // when columns to cache is not specified then column access order will be same as the array
      // index of min max length
      columnOrdinalsTOAccess = new int[segmentProperties.getColumnsValueSize().length];
      for (int i = 0; i < columnOrdinalsTOAccess.length; i++) {
        columnOrdinalsTOAccess[i] = i;
      }
    }
    return columnOrdinalsTOAccess;
  }
}
