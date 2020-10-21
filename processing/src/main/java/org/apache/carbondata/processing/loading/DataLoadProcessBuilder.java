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
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.processing.loading.constants.DataLoadProcessorConstants;
import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.loading.steps.CarbonRowDataWriterProcessorStepImpl;
import org.apache.carbondata.processing.loading.steps.DataConverterProcessorStepImpl;
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
      CarbonIterator[] inputIterators) {
    CarbonDataLoadConfiguration configuration = createConfiguration(loadModel, storeLocation);
    SortScopeOptions.SortScope sortScope = CarbonDataProcessorUtil.getSortScope(configuration);
    if (configuration.getBucketingInfo() != null &&
            CarbonProperties.isBadRecordHandlingEnabledForInsert()) {
      // if use old flow, both load and insert of bucket table use same. Otherwise, load of bucket
      // will use buildInternalForBucketing but insert will use buildInternalWithNoConverter.
      return buildInternalForBucketing(inputIterators, configuration);
    } else if (loadModel.isLoadWithoutConverterStep()) {
      return buildInternalWithNoConverter(inputIterators, configuration, sortScope, false);
    } else if (loadModel.isLoadWithoutConverterWithoutReArrangeStep()) {
      return buildInternalWithNoConverter(inputIterators, configuration, sortScope, true);
    } else if (loadModel.isJsonFileLoad()) {
      return buildInternalWithJsonInputProcessor(inputIterators, configuration, sortScope);
    } else if (configuration.getBucketingInfo() != null) {
      return buildInternalForBucketing(inputIterators, configuration);
    } else if (!configuration.isSortTable() || sortScope.equals(
            SortScopeOptions.SortScope.NO_SORT)) {
      return buildInternalForNoSort(inputIterators, configuration);
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
      SortScopeOptions.SortScope sortScope, boolean withoutReArrange) {
    // Wraps with dummy processor.
    AbstractDataLoadProcessorStep inputProcessorStep =
        new InputProcessorStepWithNoConverterImpl(configuration, inputIterators, withoutReArrange);
    if (sortScope.equals(SortScopeOptions.SortScope.LOCAL_SORT) ||
            configuration.getBucketingInfo() != null) {
      AbstractDataLoadProcessorStep sortProcessorStep =
          new SortProcessorStepImpl(configuration, inputProcessorStep);
      //  Writes the sorted data in carbondata format.
      return new DataWriterProcessorStepImpl(configuration, sortProcessorStep);
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
    } else {
      // In all other cases like global sort and no sort uses this step
      return new CarbonRowDataWriterProcessorStepImpl(configuration, converterProcessorStep);
    }
  }

  private AbstractDataLoadProcessorStep buildInternalForBucketing(CarbonIterator[] inputIterators,
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
    configuration.setTableIdentifier(identifier);
    configuration.setCarbonTransactionalTable(loadModel.isCarbonTransactionalTable());
    configuration.setSchemaUpdatedTimeStamp(carbonTable.getTableLastUpdatedTime());
    configuration.setHeader(loadModel.getCsvHeaderColumns());
    configuration.setSegmentId(loadModel.getSegmentId());
    configuration.setNonSchemaColumnsPresent(loadModel.isNonSchemaColumnsPresent());
    List<LoadMetadataDetails> loadMetadataDetails = loadModel.getLoadMetadataDetails();
    if (loadMetadataDetails != null) {
      for (LoadMetadataDetails detail : loadMetadataDetails) {
        if (detail.getLoadName().equals(loadModel.getSegmentId()) && StringUtils
            .isNotEmpty(detail.getPath())) {
          configuration.setSegmentPath(detail.getPath());
        }
      }
    }
    configuration.setSkipParsers(loadModel.isSkipParsers());
    configuration.setTaskNo(loadModel.getTaskNo());
    configuration.setMetrics(loadModel.getMetrics());
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
    configuration
        .setDataLoadProperty(CarbonCommonConstants.LOAD_SORT_SCOPE, loadModel.getSortScope());
    configuration.setDataLoadProperty(CarbonCommonConstants.LOAD_GLOBAL_SORT_PARTITIONS,
        loadModel.getGlobalSortPartitions());
    configuration.setDataLoadProperty(CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORD_PATH,
        loadModel.getBadRecordsLocation());
    configuration.setDataLoadProperty(CarbonLoadOptionConstants.CARBON_OPTIONS_BINARY_DECODER,
        loadModel.getBinaryDecoder());
    if (loadModel.isLoadWithoutConverterWithoutReArrangeStep()) {
      configuration.setDataLoadProperty(DataLoadProcessorConstants.NO_REARRANGE_OF_ROWS,
          loadModel.isLoadWithoutConverterWithoutReArrangeStep());
    }
    List<CarbonDimension> dimensions = carbonTable.getVisibleDimensions();
    List<CarbonMeasure> measures = carbonTable.getVisibleMeasures();
    List<DataField> dataFields = new ArrayList<>();
    List<DataField> complexDataFields = new ArrayList<>();
    List<DataField> partitionColumns = new ArrayList<>();
    configuration.setNumberOfSortColumns(carbonTable.getNumberOfSortColumns());
    if (loadModel.isLoadWithoutConverterWithoutReArrangeStep()) {
      // To avoid, reArranging of the data for each row, re arrange the schema itself.
      getReArrangedDataFields(loadModel, carbonTable, dimensions, measures, complexDataFields,
          partitionColumns, dataFields);
    } else {
      getDataFields(loadModel, dimensions, measures, complexDataFields, dataFields);
      if (!(!configuration.isSortTable() || SortScopeOptions.getSortScope(loadModel.getSortScope())
          .equals(SortScopeOptions.SortScope.NO_SORT))) {
        dataFields = updateDataFieldsBasedOnSortColumns(dataFields);
      }
    }
    configuration.setDataFields(dataFields.toArray(new DataField[0]));
    configuration.setBucketingInfo(carbonTable.getBucketingInfo());
    configuration.setBucketHashMethod(carbonTable.getBucketHashMethod());
    configuration.setPreFetch(loadModel.isPreFetch());
    configuration.setNumberOfNoDictSortColumns(carbonTable.getNumberOfNoDictSortColumns());
    configuration.setDataWritePath(loadModel.getDataWritePath());
    setSortColumnInfo(carbonTable, loadModel, configuration);
    // For partition loading always use single core as it already runs in multiple
    // threads per partition
    if (carbonTable.isHivePartitionTable()) {
      configuration.setWritingCoresCount((short) 1);
    }
    TableSpec tableSpec = new TableSpec(carbonTable, false);
    configuration.setTableSpec(tableSpec);
    if (loadModel.getSdkWriterCores() > 0) {
      configuration.setWritingCoresCount(loadModel.getSdkWriterCores());
    }
    configuration.setNumberOfLoadingCores(CarbonProperties.getInstance().getNumberOfLoadingCores());

    configuration.setColumnCompressor(loadModel.getColumnCompressor());
    return configuration;
  }

  private static void getDataFields(CarbonLoadModel loadModel, List<CarbonDimension> dimensions,
      List<CarbonMeasure> measures, List<DataField> complexDataFields, List<DataField> dataFields) {
    // First add dictionary and non dictionary dimensions because these are part of mdk key.
    // And then add complex data types and measures.
    for (CarbonColumn column : dimensions) {
      DataField dataField = new DataField(column);
      if (column.getDataType() == DataTypes.DATE) {
        dataField.setDateFormat(loadModel.getDateFormat());
        column.setDateFormat(loadModel.getDateFormat());
      } else if (column.getDataType() == DataTypes.TIMESTAMP) {
        dataField.setTimestampFormat(loadModel.getTimestampFormat());
        column.setTimestampFormat(loadModel.getTimestampFormat());
      }
      if (column.isComplex()) {
        complexDataFields.add(dataField);
        List<CarbonDimension> childDimensions =
            ((CarbonDimension) dataField.getColumn()).getListOfChildDimensions();
        for (CarbonDimension childDimension : childDimensions) {
          if (childDimension.getDataType() == DataTypes.DATE) {
            childDimension.setDateFormat(loadModel.getDateFormat());
          } else if (childDimension.getDataType() == DataTypes.TIMESTAMP) {
            childDimension.setTimestampFormat(loadModel.getTimestampFormat());
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
  }

  private static void getReArrangedDataFields(CarbonLoadModel loadModel, CarbonTable carbonTable,
      List<CarbonDimension> dimensions, List<CarbonMeasure> measures,
      List<DataField> complexDataFields, List<DataField> partitionColumns,
      List<DataField> dataFields) {
    // re-arrange the data fields, as partition column data will be present in the end
    List<ColumnSchema> partitionColumnSchemaList;
    if (carbonTable.getPartitionInfo() != null) {
      partitionColumnSchemaList = carbonTable.getPartitionInfo().getColumnSchemaList();
    } else {
      partitionColumnSchemaList = new ArrayList<>();
    }
    // 1.1 compatibility, dimensions will not have sort columns in the beginning in 1.1.
    // Need to keep at the beginning now
    List<DataField> sortDataFields = new ArrayList<>();
    List<DataField> noSortDataFields = new ArrayList<>();
    for (CarbonColumn column : dimensions) {
      DataField dataField = new DataField(column);
      if (column.isComplex()) {
        List<CarbonDimension> childDimensions =
            ((CarbonDimension) dataField.getColumn()).getListOfChildDimensions();
        for (CarbonDimension childDimension : childDimensions) {
          if (childDimension.getDataType() == DataTypes.DATE) {
            childDimension.setDateFormat(loadModel.getDateFormat());
          } else if (childDimension.getDataType() == DataTypes.TIMESTAMP) {
            childDimension.setTimestampFormat(loadModel.getTimestampFormat());
          }
        }
        if (partitionColumnSchemaList.size() != 0 && partitionColumnSchemaList
            .contains(column.getColumnSchema())) {
          partitionColumns.add(dataField);
        } else {
          complexDataFields.add(dataField);
        }
      } else {
        if (column.getDataType() == DataTypes.DATE) {
          dataField.setDateFormat(loadModel.getDateFormat());
          column.setDateFormat(loadModel.getDateFormat());
        } else if (column.getDataType() == DataTypes.TIMESTAMP) {
          dataField.setTimestampFormat(loadModel.getTimestampFormat());
          column.setTimestampFormat(loadModel.getTimestampFormat());
        }
        if (partitionColumnSchemaList.size() != 0 && partitionColumnSchemaList
            .contains(column.getColumnSchema())) {
          partitionColumns.add(dataField);
        } else {
          if (dataField.getColumn().getColumnSchema().isSortColumn()) {
            sortDataFields.add(dataField);
          } else {
            noSortDataFields.add(dataField);
          }
        }
      }
    }
    if (sortDataFields.size() != 0) {
      dataFields.addAll(sortDataFields);
    }
    if (noSortDataFields.size() != 0) {
      dataFields.addAll(noSortDataFields);
    }
    if (complexDataFields.size() != 0) {
      dataFields.addAll(complexDataFields);
    }
    for (CarbonColumn column : measures) {
      if (partitionColumnSchemaList.size() != 0 && partitionColumnSchemaList
          .contains(column.getColumnSchema())) {
        partitionColumns.add(new DataField(column));
      } else {
        // This dummy measure is added when no measure was present. We no need to load it.
        if (!(column.getColName().equals("default_dummy_measure"))) {
          dataFields.add(new DataField(column));
        }
      }
    }
    if (partitionColumns.size() != 0) {
      // add partition columns at the end
      // re-arrange the partition columns as per column schema
      List<DataField> reArrangedPartitionColumns = new ArrayList<>();
      for (ColumnSchema col : partitionColumnSchemaList) {
        for (DataField field : partitionColumns) {
          if (field.getColumn().getColumnSchema().equals(col)) {
            reArrangedPartitionColumns.add(field);
            break;
          }
        }
      }
      dataFields.addAll(reArrangedPartitionColumns);
    }
  }

  /**
   * set sort column info in configuration
   * @param carbonTable carbon table
   * @param loadModel load model
   * @param configuration configuration
   */
  private static void setSortColumnInfo(CarbonTable carbonTable, CarbonLoadModel loadModel,
      CarbonDataLoadConfiguration configuration) {
    List<String> sortCols = carbonTable.getSortColumns();
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
          isSortColNoDict[j] = !outFields[i].isDateDataType();
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

  /**
   * This method rearrange the data fields where all the sort columns are added at first. Because
   * if the column gets added in old version like carbon1.1, it will be added at last, so if it is
   * sort column, bring it to first.
   */
  private static List<DataField> updateDataFieldsBasedOnSortColumns(List<DataField> dataFields) {
    List<DataField> updatedDataFields = new ArrayList<>();
    List<DataField> sortFields = new ArrayList<>();
    List<DataField> nonSortFields = new ArrayList<>();
    for (DataField dataField : dataFields) {
      if (dataField.getColumn().getColumnSchema().isSortColumn()) {
        sortFields.add(dataField);
      } else {
        nonSortFields.add(dataField);
      }
    }
    updatedDataFields.addAll(sortFields);
    updatedDataFields.addAll(nonSortFields);
    return updatedDataFields;
  }

}
