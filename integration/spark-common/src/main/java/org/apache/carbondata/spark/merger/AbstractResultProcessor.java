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

package org.apache.carbondata.spark.merger;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.mutate.CarbonUpdateUtil;
import org.apache.carbondata.core.scan.result.iterator.RawResultIterator;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.core.util.path.CarbonStorePath;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.processing.datatypes.GenericDataType;
import org.apache.carbondata.processing.model.CarbonLoadModel;
import org.apache.carbondata.processing.store.CarbonDataFileAttributes;
import org.apache.carbondata.processing.store.CarbonFactDataHandlerModel;

/**
 * This class contains the common methods required for result processing during compaction based on
 * restructure and mormal scenarios
 */
public abstract class AbstractResultProcessor {

  /**
   * This method will perform the desired tasks of merging the selected slices
   *
   * @param resultIteratorList
   * @return
   */
  public abstract boolean execute(List<RawResultIterator> resultIteratorList);

  /**
   * This method will create a model object for carbon fact data handler
   *
   * @param loadModel
   * @return
   */
  protected CarbonFactDataHandlerModel getCarbonFactDataHandlerModel(CarbonLoadModel loadModel,
      CarbonTable carbonTable, SegmentProperties segmentProperties, String tableName,
      String tempStoreLocation) {
    CarbonFactDataHandlerModel carbonFactDataHandlerModel = new CarbonFactDataHandlerModel();
    carbonFactDataHandlerModel.setDatabaseName(loadModel.getDatabaseName());
    carbonFactDataHandlerModel.setTableName(tableName);
    carbonFactDataHandlerModel.setMeasureCount(segmentProperties.getMeasures().size());
    carbonFactDataHandlerModel
        .setMdKeyLength(segmentProperties.getDimensionKeyGenerator().getKeySizeInBytes());
    carbonFactDataHandlerModel.setStoreLocation(tempStoreLocation);
    carbonFactDataHandlerModel.setDimLens(segmentProperties.getDimColumnsCardinality());
    carbonFactDataHandlerModel.setSegmentProperties(segmentProperties);
    carbonFactDataHandlerModel
        .setNoDictionaryCount(segmentProperties.getNumberOfNoDictionaryDimension());
    carbonFactDataHandlerModel.setDimensionCount(
        segmentProperties.getDimensions().size() - carbonFactDataHandlerModel
            .getNoDictionaryCount());
    List<ColumnSchema> wrapperColumnSchema = CarbonUtil
        .getColumnSchemaList(carbonTable.getDimensionByTableName(tableName),
            carbonTable.getMeasureByTableName(tableName));
    carbonFactDataHandlerModel.setWrapperColumnSchema(wrapperColumnSchema);
    // get the cardinality for all all the columns including no dictionary columns
    int[] formattedCardinality = CarbonUtil
        .getFormattedCardinality(segmentProperties.getDimColumnsCardinality(), wrapperColumnSchema);
    carbonFactDataHandlerModel.setColCardinality(formattedCardinality);
    //TO-DO Need to handle complex types here .
    Map<Integer, GenericDataType> complexIndexMap =
        new HashMap<Integer, GenericDataType>(segmentProperties.getComplexDimensions().size());
    carbonFactDataHandlerModel.setComplexIndexMap(complexIndexMap);
    carbonFactDataHandlerModel.setDataWritingRequest(true);
    char[] aggType = new char[segmentProperties.getMeasures().size()];
    Arrays.fill(aggType, 'n');
    int i = 0;
    for (CarbonMeasure msr : segmentProperties.getMeasures()) {
      aggType[i++] = DataTypeUtil.getAggType(msr.getDataType());
    }
    carbonFactDataHandlerModel.setAggType(aggType);
    carbonFactDataHandlerModel.setFactDimLens(segmentProperties.getDimColumnsCardinality());
    String carbonDataDirectoryPath =
        checkAndCreateCarbonStoreLocation(loadModel.getStorePath(), loadModel.getDatabaseName(),
            tableName, loadModel.getPartitionId(), loadModel.getSegmentId());
    carbonFactDataHandlerModel.setCarbonDataDirectoryPath(carbonDataDirectoryPath);
    List<CarbonDimension> dimensionByTableName =
        loadModel.getCarbonDataLoadSchema().getCarbonTable().getDimensionByTableName(tableName);
    boolean[] isUseInvertedIndexes = new boolean[dimensionByTableName.size()];
    int index = 0;
    for (CarbonDimension dimension : dimensionByTableName) {
      isUseInvertedIndexes[index++] = dimension.isUseInvertedIndex();
    }
    carbonFactDataHandlerModel.setIsUseInvertedIndex(isUseInvertedIndexes);
    carbonFactDataHandlerModel.setPrimitiveDimLens(segmentProperties.getDimColumnsCardinality());
    carbonFactDataHandlerModel.setBlockSizeInMB(carbonTable.getBlockSizeInMB());
    if (segmentProperties.getNumberOfNoDictionaryDimension() > 0
        || segmentProperties.getComplexDimensions().size() > 0) {
      carbonFactDataHandlerModel.setMdKeyIndex(segmentProperties.getMeasures().size() + 1);
    } else {
      carbonFactDataHandlerModel.setMdKeyIndex(segmentProperties.getMeasures().size());
    }
    return carbonFactDataHandlerModel;
  }

  protected void setDataFileAttributesInModel(CarbonLoadModel loadModel,
      CompactionType compactionType, CarbonTable carbonTable,
      CarbonFactDataHandlerModel carbonFactDataHandlerModel) {
    CarbonDataFileAttributes carbonDataFileAttributes;
    if (compactionType == CompactionType.IUD_UPDDEL_DELTA_COMPACTION) {
      int taskNo = CarbonUpdateUtil.getLatestTaskIdForSegment(loadModel.getSegmentId(),
          CarbonStorePath.getCarbonTablePath(loadModel.getStorePath(),
              carbonTable.getCarbonTableIdentifier()));
      // Increase the Task Index as in IUD_UPDDEL_DELTA_COMPACTION the new file will
      // be written in same segment. So the TaskNo should be incremented by 1 from max val.
      int index = taskNo + 1;
      carbonDataFileAttributes = new CarbonDataFileAttributes(index, loadModel.getFactTimeStamp());
    } else {
      carbonDataFileAttributes =
          new CarbonDataFileAttributes(Integer.parseInt(loadModel.getTaskNo()),
              loadModel.getFactTimeStamp());
    }
    carbonFactDataHandlerModel.setCarbonDataFileAttributes(carbonDataFileAttributes);
  }

  /**
   * This method will get the store location for the given path, segment id and partition id
   *
   * @return data directory path
   */
  protected String checkAndCreateCarbonStoreLocation(String factStoreLocation, String databaseName,
      String tableName, String partitionId, String segmentId) {
    CarbonTable carbonTable = CarbonMetadata.getInstance()
        .getCarbonTable(databaseName + CarbonCommonConstants.UNDERSCORE + tableName);
    CarbonTableIdentifier carbonTableIdentifier = carbonTable.getCarbonTableIdentifier();
    CarbonTablePath carbonTablePath =
        CarbonStorePath.getCarbonTablePath(factStoreLocation, carbonTableIdentifier);
    String carbonDataDirectoryPath =
        carbonTablePath.getCarbonDataDirectoryPath(partitionId, segmentId);
    CarbonUtil.checkAndCreateFolder(carbonDataDirectoryPath);
    return carbonDataDirectoryPath;
  }
}
