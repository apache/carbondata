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

package org.apache.carbondata.processing.loading;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.constants.CarbonLoadOptionConstants;
import org.apache.carbondata.core.constants.SortScopeOptions;
import org.apache.carbondata.core.datastore.TableSpec;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.SortColumnRangeInfo;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.processing.loading.constants.DataLoadProcessorConstants;
import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.loading.steps.CarbonRowDataWriterProcessorStepImpl;
import org.apache.carbondata.processing.loading.steps.DataConverterProcessorStepImpl;
import org.apache.carbondata.processing.loading.steps.DataWriterBatchProcessorStepImpl;
import org.apache.carbondata.processing.loading.steps.DataWriterProcessorStepImpl;
import org.apache.carbondata.processing.loading.steps.InputProcessorStepImpl;
import org.apache.carbondata.processing.loading.steps.InputProcessorStepWithNoConverterImpl;
import org.apache.carbondata.processing.loading.steps.JsonInputProcessorStepImpl;
import org.apache.carbondata.processing.loading.steps.SortProcessorStepImpl;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

/**
 * It builds the pipe line of steps for loading data to carbon.
 */
public final class DataLoadProcessBuilder {
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(DataLoadProcessBuilder.class.getName());

  public AbstractDataLoadProcessorStep build(CarbonLoadModel loadModel, String[] storeLocation,
      CarbonIterator[] inputIterators) throws Exception {
    CarbonDataLoadConfiguration configuration = createConfiguration(loadModel, storeLocation);
    SortScopeOptions.SortScope sortScope = CarbonDataProcessorUtil.getSortScope(configuration);
    if (loadModel.isLoadWithoutConverterStep()) {
      return buildInternalWithNoConverter(inputIterators, configuration, sortScope);
    } else if (loadModel.isJsonFileLoad()) {
      return buildInternalWithJsonInputProcessor(inputIterators, configuration, sortScope);
    } else if (!configuration.isSortTable() || sortScope.equals(
        SortScopeOptions.SortScope.NO_SORT)) {
      return buildInternalForNoSort(inputIterators, configuration);
    } else if (configuration.getBucketingInfo() != null) {
      return buildInternalForBucketing(inputIterators, configuration);
    } else if (sortScope.equals(SortScopeOptions.SortScope.BATCH_SORT)) {
      return buildInternalForBatchSort(inputIterators, configuration);
    } else {
      return buildInternal(inputIterators, configuration);
    }
  }

  private AbstractDataLoadProcessorStep buildInternal(CarbonIterator[] inputIterators,
      CarbonDataLoadConfiguration configuration) {
    // 1. Reads the data input iterators and parses the data.
    AbstractDataLoadProcessorStep inputProcessorStep =
        new InputProcessorStepImpl(configuration, inputIterators);
    // 2. Converts the data like dictionary or non dictionary or complex objects depends on
    // data types and configurations.
    AbstractDataLoadProcessorStep converterProcessorStep =
        new DataConverterProcessorStepImpl(configuration, inputProcessorStep);
    // 3. Sorts the data by SortColumn
    AbstractDataLoadProcessorStep sortProcessorStep =
        new SortProcessorStepImpl(configuration, converterProcessorStep);
    // 4. Writes the sorted data in carbondata format.
    return new DataWriterProcessorStepImpl(configuration, sortProcessorStep);
  }

  private AbstractDataLoadProcessorStep buildInternalForNoSort(CarbonIterator[] inputIterators,
      CarbonDataLoadConfiguration configuration) {
    // 1. Reads the data input iterators and parses the data.
    AbstractDataLoadProcessorStep inputProcessorStep =
        new InputProcessorStepImpl(configuration, inputIterators);
    // 2. Converts the data like dictionary or non dictionary or complex objects depends on
    // data types and configurations.
    AbstractDataLoadProcessorStep converterProcessorStep =
        new DataConverterProcessorStepImpl(configuration, inputProcessorStep);
    // 3. Writes the sorted data in carbondata format.
    return new CarbonRowDataWriterProcessorStepImpl(configuration, converterProcessorStep);
  }

  /**
   * Build pipe line for Load without Conversion Step.
   */
  private AbstractDataLoadProcessorStep buildInternalWithNoConverter(
      CarbonIterator[] inputIterators, CarbonDataLoadConfiguration configuration,
      SortScopeOptions.SortScope sortScope) {
    // Wraps with dummy processor.
    AbstractDataLoadProcessorStep inputProcessorStep =
        new InputProcessorStepWithNoConverterImpl(configuration, inputIterators);
    if (sortScope.equals(SortScopeOptions.SortScope.LOCAL_SORT)) {
      AbstractDataLoadProcessorStep sortProcessorStep =
          new SortProcessorStepImpl(configuration, inputProcessorStep);
      //  Writes the sorted data in carbondata format.
      return new DataWriterProcessorStepImpl(configuration, sortProcessorStep);
    } else if (sortScope.equals(SortScopeOptions.SortScope.BATCH_SORT)) {
      //  Sorts the data by SortColumn or not
      AbstractDataLoadProcessorStep sortProcessorStep =
          new SortProcessorStepImpl(configuration, inputProcessorStep);
      // Writes the sorted data in carbondata format.
      return new DataWriterBatchProcessorStepImpl(configuration, sortProcessorStep);
    } else {
      // In all other cases like global sort and no sort uses this step
      return new CarbonRowDataWriterProcessorStepImpl(configuration, inputProcessorStep);
    }
  }

  /**
   * Build pipe line for Load with json input processor.
   */
  private AbstractDataLoadProcessorStep buildInternalWithJsonInputProcessor(
      CarbonIterator[] inputIterators, CarbonDataLoadConfiguration configuration,
      SortScopeOptions.SortScope sortScope) {
    // currently row by row conversion of string json to carbon row is supported.
    AbstractDataLoadProcessorStep inputProcessorStep =
        new JsonInputProcessorStepImpl(configuration, inputIterators);
    // 2. Converts the data like dictionary or non dictionary or complex objects depends on
    // data types and configurations.
    AbstractDataLoadProcessorStep converterProcessorStep =
        new DataConverterProcessorStepImpl(configuration, inputProcessorStep);
    if (sortScope.equals(SortScopeOptions.SortScope.LOCAL_SORT)) {
      AbstractDataLoadProcessorStep sortProcessorStep =
          new SortProcessorStepImpl(configuration, converterProcessorStep);
      //  Writes the sorted data in carbondata format.
      return new DataWriterProcessorStepImpl(configuration, sortProcessorStep);
    } else if (sortScope.equals(SortScopeOptions.SortScope.BATCH_SORT)) {
      //  Sorts the data by SortColumn or not
      AbstractDataLoadProcessorStep sortProcessorStep =
          new SortProcessorStepImpl(configuration, converterProcessorStep);
      // Writes the sorted data in carbondata format.
      return new DataWriterBatchProcessorStepImpl(configuration, sortProcessorStep);
    } else {
      // In all other cases like global sort and no sort uses this step
      return new CarbonRowDataWriterProcessorStepImpl(configuration, converterProcessorStep);
    }
  }

  private AbstractDataLoadProcessorStep buildInternalForBatchSort(CarbonIterator[] inputIterators,
      CarbonDataLoadConfiguration configuration) {
    // 1. Reads the data input iterators and parses the data.
    AbstractDataLoadProcessorStep inputProcessorStep =
        new InputProcessorStepImpl(configuration, inputIterators);
    // 2. Converts the data like dictionary or non dictionary or complex objects depends on
    // data types and configurations.
    AbstractDataLoadProcessorStep converterProcessorStep =
        new DataConverterProcessorStepImpl(configuration, inputProcessorStep);
    // 3. Sorts the data by SortColumn or not
    AbstractDataLoadProcessorStep sortProcessorStep =
        new SortProcessorStepImpl(configuration, converterProcessorStep);
    // 4. Writes the sorted data in carbondata format.
    return new DataWriterBatchProcessorStepImpl(configuration, sortProcessorStep);
  }

  private AbstractDataLoadProcessorStep buildInternalForBucketing(CarbonIterator[] inputIterators,
      CarbonDataLoadConfiguration configuration) throws Exception {
    // 1. Reads the data input iterators and parses the data.
    AbstractDataLoadProcessorStep inputProcessorStep =
        new InputProcessorStepImpl(configuration, inputIterators);
    // 2. Converts the data like dictionary or non dictionary or complex objects depends on
    // data types and configurations.
    AbstractDataLoadProcessorStep converterProcessorStep =
        new DataConverterProcessorStepImpl(configuration, inputProcessorStep);
    // 3. Sorts the data by SortColumn or not
    AbstractDataLoadProcessorStep sortProcessorStep =
        new SortProcessorStepImpl(configuration, converterProcessorStep);
    // 4. Writes the sorted data in carbondata format.
    return new DataWriterProcessorStepImpl(configuration, sortProcessorStep);
  }

  public static CarbonDataLoadConfiguration createConfiguration(CarbonLoadModel loadModel,
      String[] storeLocation) {
    CarbonDataProcessorUtil.createLocations(storeLocation);

    String databaseName = loadModel.getDatabaseName();
    String tableName = loadModel.getTableName();
    String tempLocationKey = CarbonDataProcessorUtil
        .getTempStoreLocationKey(databaseName, tableName, loadModel.getSegmentId(),
            loadModel.getTaskNo(), false, false);
    CarbonProperties.getInstance().addProperty(tempLocationKey,
        StringUtils.join(storeLocation, File.pathSeparator));

    return createConfiguration(loadModel);
  }

  public static CarbonDataLoadConfiguration createConfiguration(CarbonLoadModel loadModel) {
    CarbonDataLoadConfiguration configuration = new CarbonDataLoadConfiguration();
    CarbonTable carbonTable = loadModel.getCarbonDataLoadSchema().getCarbonTable();
    AbsoluteTableIdentifier identifier = carbonTable.getAbsoluteTableIdentifier();
    configuration.setParentTablePath(loadModel.getParentTablePath());
    configuration.setTableIdentifier(identifier);
    configuration.setCarbonTransactionalTable(loadModel.isCarbonTransactionalTable());
    configuration.setSchemaUpdatedTimeStamp(carbonTable.getTableLastUpdatedTime());
    configuration.setHeader(loadModel.getCsvHeaderColumns());
    configuration.setSegmentId(loadModel.getSegmentId());
    configuration.setTaskNo(loadModel.getTaskNo());
    String[] complexDelimiters = new String[loadModel.getComplexDelimiters().size()];
    loadModel.getComplexDelimiters().toArray(complexDelimiters);
    configuration
        .setDataLoadProperty(DataLoadProcessorConstants.COMPLEX_DELIMITERS, complexDelimiters);
    configuration.setDataLoadProperty(DataLoadProcessorConstants.SERIALIZATION_NULL_FORMAT,
        loadModel.getSerializationNullFormat().split(",")[1]);
    configuration.setDataLoadProperty(DataLoadProcessorConstants.FACT_TIME_STAMP,
        loadModel.getFactTimeStamp());
    configuration.setDataLoadProperty(DataLoadProcessorConstants.BAD_RECORDS_LOGGER_ENABLE,
        loadModel.getBadRecordsLoggerEnable().split(",")[1]);
    configuration.setDataLoadProperty(DataLoadProcessorConstants.BAD_RECORDS_LOGGER_ACTION,
        loadModel.getBadRecordsAction().split(",")[1]);
    configuration.setDataLoadProperty(DataLoadProcessorConstants.IS_EMPTY_DATA_BAD_RECORD,
        loadModel.getIsEmptyDataBadRecord().split(",")[1]);
    configuration.setDataLoadProperty(DataLoadProcessorConstants.SKIP_EMPTY_LINE,
        loadModel.getSkipEmptyLine());
    configuration.setDataLoadProperty(DataLoadProcessorConstants.FACT_FILE_PATH,
        loadModel.getFactFilePath());
    configuration.setParentTablePath(loadModel.getParentTablePath());
    configuration
        .setDataLoadProperty(CarbonCommonConstants.LOAD_SORT_SCOPE, loadModel.getSortScope());
    configuration.setDataLoadProperty(CarbonCommonConstants.LOAD_BATCH_SORT_SIZE_INMB,
        loadModel.getBatchSortSizeInMb());
    configuration.setDataLoadProperty(CarbonCommonConstants.LOAD_GLOBAL_SORT_PARTITIONS,
        loadModel.getGlobalSortPartitions());
    configuration.setDataLoadProperty(CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORD_PATH,
        loadModel.getBadRecordsLocation());
    configuration.setDataLoadProperty(CarbonLoadOptionConstants.CARBON_OPTIONS_BINARY_DECODER,
        loadModel.getBinaryDecoder());

    List<CarbonDimension> dimensions =
        carbonTable.getDimensionByTableName(carbonTable.getTableName());
    List<CarbonMeasure> measures =
        carbonTable.getMeasureByTableName(carbonTable.getTableName());
    List<DataField> dataFields = new ArrayList<>();
    List<DataField> complexDataFields = new ArrayList<>();

    // First add dictionary and non dictionary dimensions because these are part of mdk key.
    // And then add complex data types and measures.
    for (CarbonColumn column : dimensions) {
      DataField dataField = new DataField(column);
      if (column.getDataType() == DataTypes.DATE) {
        dataField.setDateFormat(loadModel.getDateFormat());
        column.setDateFormat(loadModel.getDateFormat());
      } else if (column.getDataType() == DataTypes.TIMESTAMP) {
        dataField.setTimestampFormat(loadModel.getTimestampformat());
        column.setTimestampFormat(loadModel.getTimestampformat());
      }
      if (column.isComplex()) {
        complexDataFields.add(dataField);
        List<CarbonDimension> childDimensions =
            ((CarbonDimension) dataField.getColumn()).getListOfChildDimensions();
        for (CarbonDimension childDimension : childDimensions) {
          if (childDimension.getDataType() == DataTypes.DATE) {
            childDimension.setDateFormat(loadModel.getDateFormat());
          } else if (childDimension.getDataType() == DataTypes.TIMESTAMP) {
            childDimension.setTimestampFormat(loadModel.getTimestampformat());
          }
        }
      } else {
        dataFields.add(dataField);
      }
    }
    dataFields.addAll(complexDataFields);
    for (CarbonColumn column : measures) {
      // This dummy measure is added when no measure was present. We no need to load it.
      if (!(column.getColName().equals("default_dummy_measure"))) {
        dataFields.add(new DataField(column));
      }
    }
    configuration.setDataFields(dataFields.toArray(new DataField[dataFields.size()]));
    configuration.setBucketingInfo(carbonTable.getBucketingInfo(carbonTable.getTableName()));
    // configuration for one pass load: dictionary server info
    configuration.setUseOnePass(loadModel.getUseOnePass());
    configuration.setDictionaryServerHost(loadModel.getDictionaryServerHost());
    configuration.setDictionaryServerPort(loadModel.getDictionaryServerPort());
    configuration.setDictionaryServerSecretKey(loadModel.getDictionaryServerSecretKey());
    configuration.setDictionaryEncryptServerSecure(loadModel.getDictionaryEncryptServerSecure());
    configuration.setDictionaryServiceProvider(loadModel.getDictionaryServiceProvider());
    configuration.setPreFetch(loadModel.isPreFetch());
    configuration.setNumberOfSortColumns(carbonTable.getNumberOfSortColumns());
    configuration.setNumberOfNoDictSortColumns(carbonTable.getNumberOfNoDictSortColumns());
    configuration.setDataWritePath(loadModel.getDataWritePath());
    setSortColumnInfo(carbonTable, loadModel, configuration);
    // For partition loading always use single core as it already runs in multiple
    // threads per partition
    if (carbonTable.isHivePartitionTable()) {
      configuration.setWritingCoresCount((short) 1);
    }
    TableSpec tableSpec = new TableSpec(carbonTable);
    configuration.setTableSpec(tableSpec);
    if (loadModel.getSdkWriterCores() > 0) {
      configuration.setWritingCoresCount(loadModel.getSdkWriterCores());
    }
    configuration.setNumberOfLoadingCores(CarbonProperties.getInstance().getNumberOfLoadingCores());

    configuration.setColumnCompressor(loadModel.getColumnCompressor());
    return configuration;
  }

  /**
   * set sort column info in configuration
   * @param carbonTable carbon table
   * @param loadModel load model
   * @param configuration configuration
   */
  private static void setSortColumnInfo(CarbonTable carbonTable, CarbonLoadModel loadModel,
      CarbonDataLoadConfiguration configuration) {
    List<String> sortCols = carbonTable.getSortColumns(carbonTable.getTableName());
    SortScopeOptions.SortScope sortScope = SortScopeOptions.getSortScope(loadModel.getSortScope());
    if (!SortScopeOptions.SortScope.LOCAL_SORT.equals(sortScope)
        || sortCols.size() == 0
        || StringUtils.isBlank(loadModel.getSortColumnsBoundsStr())) {
      if (!StringUtils.isBlank(loadModel.getSortColumnsBoundsStr())) {
        LOGGER.warn("sort column bounds will be ignored");
      }

      configuration.setSortColumnRangeInfo(null);
      return;
    }
    // column index for sort columns
    int[] sortColIndex = new int[sortCols.size()];
    boolean[] isSortColNoDict = new boolean[sortCols.size()];

    DataField[] outFields = configuration.getDataFields();
    int j = 0;
    boolean columnExist;
    for (String sortCol : sortCols) {
      columnExist = false;

      for (int i = 0; !columnExist && i < outFields.length; i++) {
        if (outFields[i].getColumn().getColName().equalsIgnoreCase(sortCol)) {
          columnExist = true;

          sortColIndex[j] = i;
          isSortColNoDict[j] = !outFields[i].hasDictionaryEncoding();
          j++;
        }
      }

      if (!columnExist) {
        throw new CarbonDataLoadingException("Field " + sortCol + " does not exist.");
      }
    }

    String[] sortColumnBounds = StringUtils.splitPreserveAllTokens(
        loadModel.getSortColumnsBoundsStr(),
        CarbonLoadOptionConstants.SORT_COLUMN_BOUNDS_ROW_DELIMITER, -1);
    for (String bound : sortColumnBounds) {
      String[] fieldInBounds = StringUtils.splitPreserveAllTokens(bound,
          CarbonLoadOptionConstants.SORT_COLUMN_BOUNDS_FIELD_DELIMITER, -1);
      if (fieldInBounds.length != sortCols.size()) {
        String msg = new StringBuilder(
            "The number of field in bounds should be equal to that in sort columns.")
            .append(" Expected ").append(sortCols.size())
            .append(", actual ").append(String.valueOf(fieldInBounds.length)).append(".")
            .append(" The illegal bound is '").append(bound).append("'.").toString();
        throw new CarbonDataLoadingException(msg);
      }
    }

    SortColumnRangeInfo sortColumnRangeInfo = new SortColumnRangeInfo(sortColIndex,
        isSortColNoDict,
        sortColumnBounds,
        CarbonLoadOptionConstants.SORT_COLUMN_BOUNDS_FIELD_DELIMITER);
    configuration.setSortColumnRangeInfo(sortColumnRangeInfo);
  }
}
