package org.apache.carbondata.processing.newflow;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.apache.carbondata.core.carbon.CarbonTableIdentifier;
import org.apache.carbondata.core.carbon.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.processing.constants.LoggerAction;
import org.apache.carbondata.processing.model.CarbonLoadModel;
import org.apache.carbondata.processing.newflow.constants.DataLoadProcessorConstants;
import org.apache.carbondata.processing.newflow.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.surrogatekeysgenerator.csvbased.BadRecordsLogger;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

/**
 * Created by david on 16-11-20.
 */
public class NewFlowDataLoadUtil {

  private static LogService LOGGER = LogServiceFactory.getLogService(
        NewFlowDataLoadUtil.class.getName());

  public static  CarbonDataLoadConfiguration createConfiguration(CarbonLoadModel loadModel,
                                                          String storeLocation) throws Exception {
    if (!new File(storeLocation).mkdirs()) {
      LOGGER.error("Error while creating the temp store path: " + storeLocation);
    }
    CarbonDataLoadConfiguration configuration = new CarbonDataLoadConfiguration();
    String databaseName = loadModel.getDatabaseName();
    String tableName = loadModel.getTableName();
    String tempLocationKey = databaseName + CarbonCommonConstants.UNDERSCORE + tableName
        + CarbonCommonConstants.UNDERSCORE + loadModel.getTaskNo();
    CarbonProperties.getInstance().addProperty(tempLocationKey, storeLocation);
    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.STORE_LOCATION_HDFS, loadModel.getStorePath());

    CarbonTable carbonTable = loadModel.getCarbonDataLoadSchema().getCarbonTable();
    AbsoluteTableIdentifier identifier = carbonTable.getAbsoluteTableIdentifier();
    configuration.setTableIdentifier(identifier);
    String csvHeader = loadModel.getCsvHeader();
    String csvFileName = null;
    if (csvHeader != null && !csvHeader.isEmpty()) {
      configuration.setHeader(CarbonDataProcessorUtil.getColumnFields(csvHeader, ","));
    } else {
      CarbonFile csvFile =
          CarbonDataProcessorUtil.getCsvFileToRead(loadModel.getFactFilePath().split(",")[0]);
      csvFileName = csvFile.getName();
      csvHeader = CarbonDataProcessorUtil.getFileHeader(csvFile);
      configuration.setHeader(
          CarbonDataProcessorUtil.getColumnFields(csvHeader, loadModel.getCsvDelimiter()));
    }
    if (!CarbonDataProcessorUtil
        .isHeaderValid(loadModel.getTableName(), csvHeader, loadModel.getCarbonDataLoadSchema(),
            loadModel.getCsvDelimiter())) {
      if (csvFileName == null) {
        LOGGER.error("CSV header provided in DDL is not proper."
            + " Column names in schema and CSV header are not the same.");
        throw new CarbonDataLoadingException(
            "CSV header provided in DDL is not proper. Column names in schema and CSV header are "
                + "not the same.");
      } else {
        LOGGER.error(
            "CSV File provided is not proper. Column names in schema and csv header are not same. "
                + "CSVFile Name : " + csvFileName);
        throw new CarbonDataLoadingException(
            "CSV File provided is not proper. Column names in schema and csv header are not same. "
                + "CSVFile Name : " + csvFileName);
      }
    }

    configuration.setPartitionId(loadModel.getPartitionId());
    configuration.setSegmentId(loadModel.getSegmentId());
    configuration.setTaskNo(loadModel.getTaskNo());
    configuration.setDataLoadProperty(DataLoadProcessorConstants.COMPLEX_DELIMITERS,
        new String[] { loadModel.getComplexDelimiterLevel1(),
            loadModel.getComplexDelimiterLevel2() });
    configuration.setDataLoadProperty(DataLoadProcessorConstants.SERIALIZATION_NULL_FORMAT,
        loadModel.getSerializationNullFormat().split(",")[1]);
    configuration.setDataLoadProperty(DataLoadProcessorConstants.FACT_TIME_STAMP,
        loadModel.getFactTimeStamp());
    configuration.setDataLoadProperty(DataLoadProcessorConstants.BAD_RECORDS_LOGGER_ENABLE,
        loadModel.getBadRecordsLoggerEnable().split(",")[1]);
    configuration.setDataLoadProperty(DataLoadProcessorConstants.BAD_RECORDS_LOGGER_ACTION,
        loadModel.getBadRecordsAction().split(",")[1]);
    configuration.setDataLoadProperty(DataLoadProcessorConstants.FACT_FILE_PATH,
        loadModel.getFactFilePath());
    List<CarbonDimension> dimensions =
        carbonTable.getDimensionByTableName(carbonTable.getFactTableName());
    List<CarbonMeasure> measures =
        carbonTable.getMeasureByTableName(carbonTable.getFactTableName());
    Map<String, String> dateFormatMap =
        CarbonDataProcessorUtil.getDateFormatMap(loadModel.getDateFormat());
    List<DataField> dataFields = new ArrayList<>();
    List<DataField> complexDataFields = new ArrayList<>();

    // First add dictionary and non dictionary dimensions because these are part of mdk key.
    // And then add complex data types and measures.
    for (CarbonColumn column : dimensions) {
      DataField dataField = new DataField(column);
      dataField.setDateFormat(dateFormatMap.get(column.getColName()));
      if (column.isComplex()) {
        complexDataFields.add(dataField);
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
    return configuration;
  }

  public static  BadRecordsLogger createBadRecordLogger(CarbonDataLoadConfiguration configuration) {
    boolean badRecordsLogRedirect = false;
    boolean badRecordConvertNullDisable = false;
    boolean badRecordsLoggerEnable = Boolean.parseBoolean(
        configuration.getDataLoadProperty(DataLoadProcessorConstants.BAD_RECORDS_LOGGER_ENABLE)
            .toString());
    Object bad_records_action =
        configuration.getDataLoadProperty(DataLoadProcessorConstants.BAD_RECORDS_LOGGER_ACTION)
            .toString();
    if (null != bad_records_action) {
      LoggerAction loggerAction = null;
      try {
        loggerAction = LoggerAction.valueOf(bad_records_action.toString().toUpperCase());
      } catch (IllegalArgumentException e) {
        loggerAction = LoggerAction.FORCE;
      }
      switch (loggerAction) {
        case FORCE:
          badRecordConvertNullDisable = false;
          break;
        case REDIRECT:
          badRecordsLogRedirect = true;
          badRecordConvertNullDisable = true;
          break;
        case IGNORE:
          badRecordsLogRedirect = false;
          badRecordConvertNullDisable = true;
          break;
      }
    }
    CarbonTableIdentifier identifier =
        configuration.getTableIdentifier().getCarbonTableIdentifier();
    BadRecordsLogger badRecordsLogger = new BadRecordsLogger(identifier.getBadRecordLoggerKey(),
        identifier.getTableName() + '_' + System.currentTimeMillis(), getBadLogStoreLocation(
        identifier.getDatabaseName() + File.separator + identifier.getTableName() + File.separator
            + configuration.getTaskNo()), badRecordsLogRedirect, badRecordsLoggerEnable,
        badRecordConvertNullDisable);
    return badRecordsLogger;
  }

  private static String getBadLogStoreLocation(String storeLocation) {
    String badLogStoreLocation =
        CarbonProperties.getInstance().getProperty(CarbonCommonConstants.CARBON_BADRECORDS_LOC);
    badLogStoreLocation = badLogStoreLocation + File.separator + storeLocation;

    return badLogStoreLocation;
  }

}
