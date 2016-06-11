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

package org.carbondata.core.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.CarbonCommonConstants;

public final class CarbonProperties {
  /**
   * Attribute for Carbon LOGGER.
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonProperties.class.getName());

  /**
   * class instance.
   */
  private static final CarbonProperties CARBONPROPERTIESINSTANCE = new CarbonProperties();

  /**
   * porpeties .
   */
  private Properties carbonProperties;

  /**
   * Private constructor this will call load properties method to load all the
   * carbon properties in memory.
   */
  private CarbonProperties() {
    carbonProperties = new Properties();
    loadProperties();
    validateAndLoadDefaultProperties();
  }

  /**
   * This method will be responsible for get this class instance
   *
   * @return carbon properties instance
   */
  public static CarbonProperties getInstance() {
    return CARBONPROPERTIESINSTANCE;
  }

  /**
   * This method validates the loaded properties and loads default
   * values in case of wrong values.
   */
  private void validateAndLoadDefaultProperties() {
    if (null == carbonProperties.getProperty(CarbonCommonConstants.STORE_LOCATION)) {
      carbonProperties.setProperty(CarbonCommonConstants.STORE_LOCATION,
          CarbonCommonConstants.STORE_LOCATION_DEFAULT_VAL);
    }

    if (null == carbonProperties.getProperty(CarbonCommonConstants.VALUESTORE_TYPE)) {
      carbonProperties.setProperty(CarbonCommonConstants.VALUESTORE_TYPE,
          CarbonCommonConstants.VALUESTORE_TYPE_DEFAULT_VAL);
    }

    if (null == carbonProperties.getProperty(CarbonCommonConstants.KEYSTORE_TYPE)) {
      carbonProperties.setProperty(CarbonCommonConstants.KEYSTORE_TYPE,
          CarbonCommonConstants.KEYSTORE_TYPE_DEFAULT_VAL);
    }

    validateBlockletSize();
    validateMaxFileSize();
    validateNumCores();
    validateNumCoresBlockSort();
    validateBatchSize();
    validateSortSize();
    validateCardinalityIncrementValue();
    validateOnlineMergerSize();
    validateOfflineMergerSize();
    validateSortBufferSize();
    validateDataLoadQSize();
    validateDataLoadConcExecSize();
    validateDecimalPointers();
    validateDecimalPointersAgg();
    validateCsvFileSize();
    validateNumberOfCsvFile();
    validateBadRecordsLocation();
    validateBadRecordsEncryption();
    validateHighCardinalityIdentify();
    validateHighCardinalityThreshold();
    validateHighCardinalityInRowCountPercentage();
  }

  private void validateBadRecordsLocation() {
    String badRecordsLocation =
        carbonProperties.getProperty(CarbonCommonConstants.CARBON_BADRECORDS_LOC);
    if (null == badRecordsLocation || badRecordsLocation.length() == 0) {
      carbonProperties.setProperty(CarbonCommonConstants.CARBON_BADRECORDS_LOC,
          CarbonCommonConstants.CARBON_BADRECORDS_LOC_DEFAULT_VAL);
    }
  }

  private void validateBadRecordsEncryption() {
    String badRecordsEncryption =
        carbonProperties.getProperty(CarbonCommonConstants.CARBON_BADRECORDS_ENCRYPTION);
    if (null == badRecordsEncryption || badRecordsEncryption.length() == 0) {
      carbonProperties.setProperty(CarbonCommonConstants.CARBON_BADRECORDS_ENCRYPTION,
          CarbonCommonConstants.CARBON_BADRECORDS_ENCRYPTION_DEFAULT_VAL);
    }
  }

  private void validateCsvFileSize() {
    try {
      int csvFileSizeProperty = Integer.parseInt(carbonProperties
          .getProperty(CarbonCommonConstants.CARBON_DATALOAD_VALID_CSVFILE_SIZE,
              CarbonCommonConstants.CARBON_DATALOAD_VALID_CSVFILE_SIZE_DEFAULTVALUE));
      if (csvFileSizeProperty < 1) {
        LOGGER.info("Invalid value for " + CarbonCommonConstants.CARBON_DATALOAD_VALID_CSVFILE_SIZE
                + "\"Only Positive Integer(greater than zero) is allowed. Using the default value\""
                + CarbonCommonConstants.CARBON_DATALOAD_VALID_CSVFILE_SIZE_DEFAULTVALUE);

        carbonProperties.setProperty(CarbonCommonConstants.CARBON_DATALOAD_VALID_CSVFILE_SIZE,
            CarbonCommonConstants.CARBON_DATALOAD_VALID_CSVFILE_SIZE_DEFAULTVALUE);
      }
    } catch (NumberFormatException e) {
      LOGGER.info("Invalid value for " + CarbonCommonConstants.CARBON_DATALOAD_VALID_CSVFILE_SIZE
              + "\"Only Positive Integer(greater than zero) is allowed. Using the default value\""
              + CarbonCommonConstants.CARBON_DATALOAD_VALID_CSVFILE_SIZE_DEFAULTVALUE);

      carbonProperties.setProperty(CarbonCommonConstants.CARBON_DATALOAD_VALID_CSVFILE_SIZE,
          CarbonCommonConstants.CARBON_DATALOAD_VALID_CSVFILE_SIZE_DEFAULTVALUE);
    }
  }

  private void validateNumberOfCsvFile() {
    String count = CarbonCommonConstants.CARBON_DATALOAD_VALID_NUMBAER_OF_CSVFILE;
    try {
      int csvFileSizeProperty = Integer.parseInt(carbonProperties
          .getProperty(count,
              CarbonCommonConstants.CARBON_DATALOAD_VALID_NUMBAER_OF_CSVFILE_DEFAULTVALUE));
      if (csvFileSizeProperty < 1) {
        LOGGER.info("Invalid value for " + count
                + "\"Only Positive Integer(greater than zero) is allowed. Using the default value\""
                + CarbonCommonConstants.CARBON_DATALOAD_VALID_NUMBAER_OF_CSVFILE_DEFAULTVALUE);

        carbonProperties.setProperty(count,
            CarbonCommonConstants.CARBON_DATALOAD_VALID_NUMBAER_OF_CSVFILE_DEFAULTVALUE);
      }
    } catch (NumberFormatException e) {
      LOGGER.info("Invalid value for " + count
              + "\"Only Positive Integer(greater than zero) is allowed. Using the default value\""
              + CarbonCommonConstants.CARBON_DATALOAD_VALID_NUMBAER_OF_CSVFILE_DEFAULTVALUE);

      carbonProperties.setProperty(count,
          CarbonCommonConstants.CARBON_DATALOAD_VALID_NUMBAER_OF_CSVFILE_DEFAULTVALUE);
    }
  }

  /**
   * This method validates the batch size
   */
  private void validateOnlineMergerSize() {
    String onlineMergeSize = carbonProperties
        .getProperty(CarbonCommonConstants.ONLINE_MERGE_FILE_SIZE,
            CarbonCommonConstants.ONLINE_MERGE_FILE_SIZE_DEFAULT_VALUE);
    try {
      int offlineMergerSize = Integer.parseInt(onlineMergeSize);

      if (offlineMergerSize < CarbonCommonConstants.ONLINE_MERGE_MIN_VALUE
          || offlineMergerSize > CarbonCommonConstants.ONLINE_MERGE_MAX_VALUE) {
        LOGGER.info("The online Merge Size value \"" + onlineMergeSize
                + "\" is invalid. Using the default value \""
                + CarbonCommonConstants.ONLINE_MERGE_FILE_SIZE_DEFAULT_VALUE);
        carbonProperties.setProperty(CarbonCommonConstants.ONLINE_MERGE_FILE_SIZE,
            CarbonCommonConstants.ONLINE_MERGE_FILE_SIZE_DEFAULT_VALUE);
      }
    } catch (NumberFormatException e) {
      LOGGER.info("The online Merge Size value \"" + onlineMergeSize
              + "\" is invalid. Using the default value \""
              + CarbonCommonConstants.ONLINE_MERGE_FILE_SIZE_DEFAULT_VALUE);
      carbonProperties.setProperty(CarbonCommonConstants.ONLINE_MERGE_FILE_SIZE,
          CarbonCommonConstants.ONLINE_MERGE_FILE_SIZE_DEFAULT_VALUE);
    }
  }

  /**
   * This method validates the batch size
   */
  private void validateOfflineMergerSize() {
    String offLineMergerSize = carbonProperties
        .getProperty(CarbonCommonConstants.OFFLINE_MERGE_FILE_SIZE,
            CarbonCommonConstants.OFFLINE_MERGE_FILE_SIZE_DEFAULT_VALUE);
    try {
      int offLineMergeSize = Integer.parseInt(offLineMergerSize);

      if (offLineMergeSize < CarbonCommonConstants.OFFLINE_MERGE_MIN_VALUE
          || offLineMergeSize > CarbonCommonConstants.OFFLINE_MERGE_MAX_VALUE) {
        LOGGER.info("The offline Merge Size value \"" + offLineMergerSize
                + "\" is invalid. Using the default value \""
                + CarbonCommonConstants.OFFLINE_MERGE_FILE_SIZE_DEFAULT_VALUE);
        carbonProperties.setProperty(CarbonCommonConstants.OFFLINE_MERGE_FILE_SIZE,
            CarbonCommonConstants.OFFLINE_MERGE_FILE_SIZE_DEFAULT_VALUE);
      }
    } catch (NumberFormatException e) {
      LOGGER.info("The offline Merge Size value \"" + offLineMergerSize
              + "\" is invalid. Using the default value \""
              + CarbonCommonConstants.OFFLINE_MERGE_FILE_SIZE_DEFAULT_VALUE);
      carbonProperties.setProperty(CarbonCommonConstants.OFFLINE_MERGE_FILE_SIZE,
          CarbonCommonConstants.OFFLINE_MERGE_FILE_SIZE_DEFAULT_VALUE);
    }
  }

  /**
   * This method validates the batch size
   */
  private void validateBatchSize() {
    String batchSizeStr = carbonProperties.getProperty(CarbonCommonConstants.BATCH_SIZE,
        CarbonCommonConstants.BATCH_SIZE_DEFAULT_VAL);
    try {
      int batchSize = Integer.parseInt(batchSizeStr);

      if (batchSize < CarbonCommonConstants.BATCH_SIZE_MIN_VAL
          || batchSize > CarbonCommonConstants.BATCH_SIZE_MAX_VAL) {
        LOGGER.info("The batch size value \"" + batchSizeStr + "\" is invalid. "
            + "Using the default value \"" + CarbonCommonConstants.BATCH_SIZE_DEFAULT_VAL);
        carbonProperties.setProperty(CarbonCommonConstants.BATCH_SIZE,
            CarbonCommonConstants.BATCH_SIZE_DEFAULT_VAL);
      }
    } catch (NumberFormatException e) {
      LOGGER.info("The batch size value \"" + batchSizeStr
          + "\" is invalid. Using the default value \""
          + CarbonCommonConstants.BATCH_SIZE_DEFAULT_VAL);
      carbonProperties.setProperty(CarbonCommonConstants.BATCH_SIZE,
          CarbonCommonConstants.BATCH_SIZE_DEFAULT_VAL);
    }
  }

  /**
   * This method validates the batch size
   */
  private void validateCardinalityIncrementValue() {
    String cardinalityIncr = carbonProperties
        .getProperty(CarbonCommonConstants.CARDINALITY_INCREMENT_VALUE,
            CarbonCommonConstants.CARDINALITY_INCREMENT_VALUE_DEFAULT_VAL);
    try {
      int batchSize = Integer.parseInt(cardinalityIncr);

      if (batchSize < CarbonCommonConstants.CARDINALITY_INCREMENT_MIN_VAL
          || batchSize > CarbonCommonConstants.CARDINALITY_INCREMENT_MAX_VAL) {
        LOGGER.info("The batch size value \"" + cardinalityIncr
                + "\" is invalid. Using the default value \""
                + CarbonCommonConstants.CARDINALITY_INCREMENT_VALUE_DEFAULT_VAL);
        carbonProperties.setProperty(CarbonCommonConstants.CARDINALITY_INCREMENT_VALUE,
            CarbonCommonConstants.CARDINALITY_INCREMENT_VALUE_DEFAULT_VAL);
      }
    } catch (NumberFormatException e) {
      LOGGER.info("The cardinality size value \"" + cardinalityIncr
              + "\" is invalid. Using the default value \""
              + CarbonCommonConstants.BATCH_SIZE_DEFAULT_VAL);
      carbonProperties.setProperty(CarbonCommonConstants.CARDINALITY_INCREMENT_VALUE,
          CarbonCommonConstants.CARDINALITY_INCREMENT_VALUE_DEFAULT_VAL);
    }
  }

  /**
   * This method validates the blocklet size
   */
  private void validateBlockletSize() {
    String blockletSizeStr = carbonProperties.getProperty(CarbonCommonConstants.BLOCKLET_SIZE,
        CarbonCommonConstants.BLOCKLET_SIZE_DEFAULT_VAL);
    try {
      int blockletSize = Integer.parseInt(blockletSizeStr);

      if (blockletSize < CarbonCommonConstants.BLOCKLET_SIZE_MIN_VAL
          || blockletSize > CarbonCommonConstants.BLOCKLET_SIZE_MAX_VAL) {
        LOGGER.info("The blocklet size value \"" + blockletSizeStr
                + "\" is invalid. Using the default value \""
                + CarbonCommonConstants.BLOCKLET_SIZE_DEFAULT_VAL);
        carbonProperties.setProperty(CarbonCommonConstants.BLOCKLET_SIZE,
            CarbonCommonConstants.BLOCKLET_SIZE_DEFAULT_VAL);
      }
    } catch (NumberFormatException e) {
      LOGGER.info("The blocklet size value \"" + blockletSizeStr
              + "\" is invalid. Using the default value \""
              + CarbonCommonConstants.BLOCKLET_SIZE_DEFAULT_VAL);
      carbonProperties.setProperty(CarbonCommonConstants.BLOCKLET_SIZE,
          CarbonCommonConstants.BLOCKLET_SIZE_DEFAULT_VAL);
    }
  }

  /**
   * This method validates data load queue size
   */
  private void validateDataLoadQSize() {
    String dataLoadQSize = carbonProperties.getProperty(CarbonCommonConstants.DATA_LOAD_Q_SIZE,
        CarbonCommonConstants.DATA_LOAD_Q_SIZE_DEFAULT);
    try {
      int dataLoadQSizeInt = Integer.parseInt(dataLoadQSize);

      if (dataLoadQSizeInt < CarbonCommonConstants.DATA_LOAD_Q_SIZE_MIN
          || dataLoadQSizeInt > CarbonCommonConstants.DATA_LOAD_Q_SIZE_MAX) {
        LOGGER.info("The data load queue size value \"" + dataLoadQSize
                + "\" is invalid. Using the default value \""
                + CarbonCommonConstants.DATA_LOAD_Q_SIZE_DEFAULT);
        carbonProperties.setProperty(CarbonCommonConstants.DATA_LOAD_Q_SIZE,
            CarbonCommonConstants.DATA_LOAD_Q_SIZE_DEFAULT);
      }
    } catch (NumberFormatException e) {
      LOGGER.info("The data load queue size value \"" + dataLoadQSize
              + "\" is invalid. Using the default value \""
              + CarbonCommonConstants.DATA_LOAD_Q_SIZE_DEFAULT);
      carbonProperties.setProperty(CarbonCommonConstants.DATA_LOAD_Q_SIZE,
          CarbonCommonConstants.DATA_LOAD_Q_SIZE_DEFAULT);
    }
  }

  /**
   * This method validates the data load concurrent exec size
   */
  private void validateDataLoadConcExecSize() {
    String dataLoadConcExecSize = carbonProperties
        .getProperty(CarbonCommonConstants.DATA_LOAD_CONC_EXE_SIZE,
            CarbonCommonConstants.DATA_LOAD_CONC_EXE_SIZE_DEFAULT);
    try {
      int dataLoadConcExecSizeInt = Integer.parseInt(dataLoadConcExecSize);

      if (dataLoadConcExecSizeInt < CarbonCommonConstants.DATA_LOAD_CONC_EXE_SIZE_MIN
          || dataLoadConcExecSizeInt > CarbonCommonConstants.DATA_LOAD_CONC_EXE_SIZE_MAX) {
        LOGGER.info("The data load concurrent exec size value \"" + dataLoadConcExecSize
                + "\" is invalid. Using the default value \""
                + CarbonCommonConstants.DATA_LOAD_CONC_EXE_SIZE_DEFAULT);
        carbonProperties.setProperty(CarbonCommonConstants.DATA_LOAD_CONC_EXE_SIZE,
            CarbonCommonConstants.DATA_LOAD_CONC_EXE_SIZE_DEFAULT);
      }
    } catch (NumberFormatException e) {
      LOGGER.info("The data load concurrent exec size value \"" + dataLoadConcExecSize
              + "\" is invalid. Using the default value \""
              + CarbonCommonConstants.DATA_LOAD_CONC_EXE_SIZE_DEFAULT);
      carbonProperties.setProperty(CarbonCommonConstants.DATA_LOAD_CONC_EXE_SIZE,
          CarbonCommonConstants.DATA_LOAD_CONC_EXE_SIZE_DEFAULT);
    }
  }

  /**
   * This method validates the decimal pointers size
   */
  private void validateDecimalPointers() {
    String decimalPointers = carbonProperties
        .getProperty(CarbonCommonConstants.CARBON_DECIMAL_POINTERS,
            CarbonCommonConstants.CARBON_DECIMAL_POINTERS_DEFAULT);
    try {
      int decimalPointersInt = Integer.parseInt(decimalPointers);

      if (decimalPointersInt < 0 || decimalPointersInt > 15) {
        LOGGER.info("The decimal pointers agg \"" + decimalPointers
                + "\" is invalid. Using the default value \""
                + CarbonCommonConstants.DATA_LOAD_CONC_EXE_SIZE_DEFAULT);
        carbonProperties.setProperty(CarbonCommonConstants.CARBON_DECIMAL_POINTERS,
            CarbonCommonConstants.CARBON_DECIMAL_POINTERS_DEFAULT);
      }
    } catch (NumberFormatException e) {
      LOGGER.info("The decimal pointers agg \"" + decimalPointers
              + "\" is invalid. Using the default value \""
              + CarbonCommonConstants.DATA_LOAD_CONC_EXE_SIZE_DEFAULT);
      carbonProperties.setProperty(CarbonCommonConstants.CARBON_DECIMAL_POINTERS,
          CarbonCommonConstants.CARBON_DECIMAL_POINTERS_DEFAULT);
    }
  }

  /**
   * This method validates the data load concurrent exec size
   */
  private void validateDecimalPointersAgg() {
    String decimalPointers = carbonProperties
        .getProperty(CarbonCommonConstants.CARBON_DECIMAL_POINTERS_AGG,
            CarbonCommonConstants.CARBON_DECIMAL_POINTERS_AGG_DEFAULT);
    try {
      int decimalPointersInt = Integer.parseInt(decimalPointers);

      if (decimalPointersInt < 0 || decimalPointersInt > 15) {
        LOGGER.info("The decimal pointers agg \"" + decimalPointers
                + "\" is invalid. Using the default value \""
                + CarbonCommonConstants.CARBON_DECIMAL_POINTERS_AGG_DEFAULT);
        carbonProperties.setProperty(CarbonCommonConstants.CARBON_DECIMAL_POINTERS_AGG,
            CarbonCommonConstants.CARBON_DECIMAL_POINTERS_AGG_DEFAULT);
      }
    } catch (NumberFormatException e) {
      LOGGER.info("The decimal pointers agg \"" + decimalPointers
              + "\" is invalid. Using the default value \""
              + CarbonCommonConstants.CARBON_DECIMAL_POINTERS_AGG_DEFAULT);
      carbonProperties.setProperty(CarbonCommonConstants.CARBON_DECIMAL_POINTERS_AGG,
          CarbonCommonConstants.CARBON_DECIMAL_POINTERS_AGG_DEFAULT);
    }
  }

  /**
   * TODO: This method validates the maximum number of blocklets per file ?
   */
  private void validateMaxFileSize() {
    String maxFileSizeStr = carbonProperties.getProperty(CarbonCommonConstants.MAX_FILE_SIZE,
        CarbonCommonConstants.MAX_FILE_SIZE_DEFAULT_VAL);
    try {
      int maxFileSize = Integer.parseInt(maxFileSizeStr);

      if (maxFileSize < CarbonCommonConstants.MAX_FILE_SIZE_DEFAULT_VAL_MIN_VAL
          || maxFileSize > CarbonCommonConstants.MAX_FILE_SIZE_DEFAULT_VAL_MAX_VAL) {
        LOGGER.info("The max file size value \"" + maxFileSizeStr
                + "\" is invalid. Using the default value \""
                + CarbonCommonConstants.MAX_FILE_SIZE_DEFAULT_VAL);
        carbonProperties.setProperty(CarbonCommonConstants.MAX_FILE_SIZE,
            CarbonCommonConstants.MAX_FILE_SIZE_DEFAULT_VAL);
      }
    } catch (NumberFormatException e) {
      LOGGER.info("The max file size value \"" + maxFileSizeStr
              + "\" is invalid. Using the default value \""
              + CarbonCommonConstants.MAX_FILE_SIZE_DEFAULT_VAL);

      carbonProperties.setProperty(CarbonCommonConstants.MAX_FILE_SIZE,
          CarbonCommonConstants.MAX_FILE_SIZE_DEFAULT_VAL);
    }
  }

  /**
   * This method validates the number cores specified
   */
  private void validateNumCores() {
    String numCoresStr = carbonProperties
        .getProperty(CarbonCommonConstants.NUM_CORES, CarbonCommonConstants.NUM_CORES_DEFAULT_VAL);
    try {
      int numCores = Integer.parseInt(numCoresStr);

      if (numCores < CarbonCommonConstants.NUM_CORES_MIN_VAL
          || numCores > CarbonCommonConstants.NUM_CORES_MAX_VAL) {
        LOGGER.info("The num Cores  value \"" + numCoresStr
            + "\" is invalid. Using the default value \""
            + CarbonCommonConstants.NUM_CORES_DEFAULT_VAL);
        carbonProperties.setProperty(CarbonCommonConstants.NUM_CORES,
            CarbonCommonConstants.NUM_CORES_DEFAULT_VAL);
      }
    } catch (NumberFormatException e) {
      LOGGER.info("The num Cores  value \"" + numCoresStr
          + "\" is invalid. Using the default value \""
          + CarbonCommonConstants.NUM_CORES_DEFAULT_VAL);
      carbonProperties.setProperty(CarbonCommonConstants.NUM_CORES,
          CarbonCommonConstants.NUM_CORES_DEFAULT_VAL);
    }
  }

  /**
   * This method validates the number cores specified for mdk block sort
   */
  private void validateNumCoresBlockSort() {
    String numCoresStr = carbonProperties
        .getProperty(CarbonCommonConstants.NUM_CORES_BLOCK_SORT,
            CarbonCommonConstants.NUM_CORES_BLOCK_SORT_DEFAULT_VAL);
    try {
      int numCores = Integer.parseInt(numCoresStr);

      if (numCores < CarbonCommonConstants.NUM_CORES_BLOCK_SORT_MIN_VAL
          || numCores > CarbonCommonConstants.NUM_CORES_BLOCK_SORT_MAX_VAL) {
        LOGGER.info("The num cores value \"" + numCoresStr
            + "\" for block sort is invalid. Using the default value \""
            + CarbonCommonConstants.NUM_CORES_BLOCK_SORT_DEFAULT_VAL);
        carbonProperties.setProperty(CarbonCommonConstants.NUM_CORES_BLOCK_SORT,
            CarbonCommonConstants.NUM_CORES_BLOCK_SORT_DEFAULT_VAL);
      }
    } catch (NumberFormatException e) {
      LOGGER.info("The num cores value \"" + numCoresStr
          + "\" for block sort is invalid. Using the default value \""
          + CarbonCommonConstants.NUM_CORES_BLOCK_SORT_DEFAULT_VAL);
      carbonProperties.setProperty(CarbonCommonConstants.NUM_CORES_BLOCK_SORT,
          CarbonCommonConstants.NUM_CORES_BLOCK_SORT_DEFAULT_VAL);
    }
  }

  /**
   * This method validates the sort size
   */
  private void validateSortSize() {
    String sortSizeStr = carbonProperties
        .getProperty(CarbonCommonConstants.SORT_SIZE, CarbonCommonConstants.SORT_SIZE_DEFAULT_VAL);
    try {
      int sortSize = Integer.parseInt(sortSizeStr);

      if (sortSize < CarbonCommonConstants.SORT_SIZE_MIN_VAL) {
        LOGGER.info("The batch size value \"" + sortSizeStr
            + "\" is invalid. Using the default value \""
            + CarbonCommonConstants.SORT_SIZE_DEFAULT_VAL);
        carbonProperties.setProperty(CarbonCommonConstants.SORT_SIZE,
            CarbonCommonConstants.SORT_SIZE_DEFAULT_VAL);
      }
    } catch (NumberFormatException e) {
      LOGGER.info("The batch size value \"" + sortSizeStr
          + "\" is invalid. Using the default value \""
          + CarbonCommonConstants.SORT_SIZE_DEFAULT_VAL);
      carbonProperties.setProperty(CarbonCommonConstants.SORT_SIZE,
          CarbonCommonConstants.SORT_SIZE_DEFAULT_VAL);
    }
  }

  /**
   * This method validates the sort size
   */
  private void validateSortBufferSize() {
    String sortSizeStr = carbonProperties.getProperty(CarbonCommonConstants.SORT_BUFFER_SIZE,
        CarbonCommonConstants.SORT_BUFFER_SIZE_DEFAULT_VALUE);
    try {
      int sortSize = Integer.parseInt(sortSizeStr);

      if (sortSize < CarbonCommonConstants.SORT_BUFFER_SIZE_MIN_VALUE) {
        LOGGER.info("The batch size value \"" + sortSizeStr
            + "\" is invalid. Using the default value \""
            + CarbonCommonConstants.SORT_BUFFER_SIZE_DEFAULT_VALUE);
        carbonProperties.setProperty(CarbonCommonConstants.SORT_BUFFER_SIZE,
            CarbonCommonConstants.SORT_BUFFER_SIZE_DEFAULT_VALUE);
      }
    } catch (NumberFormatException e) {
      LOGGER.info("The batch size value \"" + sortSizeStr
          + "\" is invalid. Using the default value \""
          + CarbonCommonConstants.SORT_BUFFER_SIZE_DEFAULT_VALUE);
      carbonProperties.setProperty(CarbonCommonConstants.SORT_BUFFER_SIZE,
          CarbonCommonConstants.SORT_BUFFER_SIZE_DEFAULT_VALUE);
    }
  }

  private void validateHighCardinalityIdentify() {
    String highcardIdentifyStr = carbonProperties.getProperty(
        CarbonCommonConstants.HIGH_CARDINALITY_IDENTIFY_ENABLE,
        CarbonCommonConstants.HIGH_CARDINALITY_IDENTIFY_ENABLE_DEFAULT);
    try {
      Boolean.parseBoolean(highcardIdentifyStr);
    } catch (NumberFormatException e) {
      LOGGER.info("The high cardinality identify value \"" + highcardIdentifyStr
          + "\" is invalid. Using the default value \""
          + CarbonCommonConstants.HIGH_CARDINALITY_IDENTIFY_ENABLE_DEFAULT);
      carbonProperties.setProperty(CarbonCommonConstants.HIGH_CARDINALITY_IDENTIFY_ENABLE,
          CarbonCommonConstants.HIGH_CARDINALITY_IDENTIFY_ENABLE_DEFAULT);
    }
  }

  private void validateHighCardinalityThreshold() {
    String highcardThresholdStr = carbonProperties.getProperty(
        CarbonCommonConstants.HIGH_CARDINALITY_THRESHOLD,
        CarbonCommonConstants.HIGH_CARDINALITY_THRESHOLD_DEFAULT);
    try {
      int highcardThreshold = Integer.parseInt(highcardThresholdStr);
      if(highcardThreshold <= CarbonCommonConstants.HIGH_CARDINALITY_THRESHOLD_MIN){
        LOGGER.info("The high cardinality threshold value \"" + highcardThresholdStr
            + "\" is invalid. Using the default value \""
            + CarbonCommonConstants.HIGH_CARDINALITY_THRESHOLD_DEFAULT);
        carbonProperties.setProperty(CarbonCommonConstants.HIGH_CARDINALITY_THRESHOLD,
            CarbonCommonConstants.HIGH_CARDINALITY_THRESHOLD_DEFAULT);
      }
    } catch (NumberFormatException e) {
      LOGGER.info("The high cardinality threshold value \"" + highcardThresholdStr
          + "\" is invalid. Using the default value \""
          + CarbonCommonConstants.HIGH_CARDINALITY_THRESHOLD_DEFAULT);
      carbonProperties.setProperty(CarbonCommonConstants.HIGH_CARDINALITY_THRESHOLD,
          CarbonCommonConstants.HIGH_CARDINALITY_THRESHOLD_DEFAULT);
    }
  }

  private void validateHighCardinalityInRowCountPercentage() {
    String highcardPercentageStr = carbonProperties.getProperty(
        CarbonCommonConstants.HIGH_CARDINALITY_IN_ROW_COUNT_PERCENTAGE,
        CarbonCommonConstants.HIGH_CARDINALITY_IN_ROW_COUNT_PERCENTAGE_DEFAULT);
    try {
      double highcardPercentage = Double.parseDouble(highcardPercentageStr);
      if(highcardPercentage <= 0){
        LOGGER.info("The percentage of high cardinality in row count value \""
            + highcardPercentageStr + "\" is invalid. Using the default value \""
            + CarbonCommonConstants.HIGH_CARDINALITY_IN_ROW_COUNT_PERCENTAGE_DEFAULT);
        carbonProperties.setProperty(
            CarbonCommonConstants.HIGH_CARDINALITY_IN_ROW_COUNT_PERCENTAGE,
            CarbonCommonConstants.HIGH_CARDINALITY_IN_ROW_COUNT_PERCENTAGE_DEFAULT);
      }
    } catch (NumberFormatException e) {
      LOGGER.info("The percentage of high cardinality in row count value \""
          + highcardPercentageStr + "\" is invalid. Using the default value \""
          + CarbonCommonConstants.HIGH_CARDINALITY_IN_ROW_COUNT_PERCENTAGE_DEFAULT);
      carbonProperties.setProperty(CarbonCommonConstants.HIGH_CARDINALITY_IN_ROW_COUNT_PERCENTAGE,
          CarbonCommonConstants.HIGH_CARDINALITY_IN_ROW_COUNT_PERCENTAGE_DEFAULT);
    }
  }

  /**
   * This method will read all the properties from file and load it into
   * memory
   */
  private void loadProperties() {
    String property = System.getProperty("carbon.properties.filepath");
    if (null == property) {
      property = CarbonCommonConstants.CARBON_PROPERTIES_FILE_PATH;
    }
    File file = new File(property);
    LOGGER.info("Property file path: " + file.getAbsolutePath());

    FileInputStream fis = null;
    try {
      if (file.exists()) {
        fis = new FileInputStream(file);

        carbonProperties.load(fis);
      }
    } catch (FileNotFoundException e) {
      LOGGER.error("The file: " + CarbonCommonConstants.CARBON_PROPERTIES_FILE_PATH
          + " does not exist");
    } catch (IOException e) {
      LOGGER.error("Error while reading the file: "
          + CarbonCommonConstants.CARBON_PROPERTIES_FILE_PATH);
    } finally {
      if (null != fis) {
        try {
          fis.close();
        } catch (IOException e) {
          LOGGER.error("Error while closing the file stream for file: "
                  + CarbonCommonConstants.CARBON_PROPERTIES_FILE_PATH);
        }
      }
    }

    print();
  }

  /**
   * This method will be used to get the properties value
   *
   * @param key
   * @return properties value
   */
  public String getProperty(String key) {
    //TODO temporary fix
    if ("carbon.leaf.node.size".equals(key)) {
      return "120000";
    }
    return carbonProperties.getProperty(key);
  }

  /**
   * This method will be used to get the properties value if property is not
   * present then it will return tghe default value
   *
   * @param key
   * @return properties value
   */
  public String getProperty(String key, String defaultValue) {
    String value = getProperty(key);
    if (null == value) {
      return defaultValue;
    }
    return value;
  }

  /**
   * This method will be used to add a new property
   *
   * @param key
   * @return properties value
   */
  public void addProperty(String key, String value) {
    carbonProperties.setProperty(key, value);

  }

  /**
   * Validate the restrictions
   *
   * @param actual
   * @param max
   * @param min
   * @param defaultVal
   * @return
   */
  public long validate(long actual, long max, long min, long defaultVal) {
    if (actual <= max && actual >= min) {
      return actual;
    }
    return defaultVal;
  }

  /**
   * returns major compaction size value from carbon properties or default value if it is not valid
   *
   * @return
   */
  public long getMajorCompactionSize() {
    long compactionSize;
    try {
      compactionSize = Long.parseLong(getProperty(CarbonCommonConstants.MAJOR_COMPACTION_SIZE,
          CarbonCommonConstants.DEFAULT_MAJOR_COMPACTION_SIZE));
    } catch (NumberFormatException e) {
      compactionSize = Long.parseLong(CarbonCommonConstants.DEFAULT_MAJOR_COMPACTION_SIZE);
    }
    return compactionSize;
  }

  /**
   * returns minor compaction size value from carbon properties or default value if it is not valid
   *
   * @return
   */
  public long getMinorCompactionSize() {
    long compactionSize;
    try {
      compactionSize = Long.parseLong(getProperty(CarbonCommonConstants.MINOR_COMPACTION_SIZE,
          CarbonCommonConstants.DEFAULT_MINOR_COMPACTION_SIZE));
    } catch (NumberFormatException e) {
      compactionSize = Long.parseLong(CarbonCommonConstants.DEFAULT_MINOR_COMPACTION_SIZE);
    }
    return compactionSize;
  }

  /**
   * returns the number of loads to be preserved.
   *
   * @return
   */
  public int getNumberOfSegmentsToBePreserved() {
    int numberOfSegmentsToBePreserved;
    try {
      numberOfSegmentsToBePreserved = Integer.parseInt(
          getProperty(CarbonCommonConstants.PRESERVE_LATEST_SEGMENTS_NUMBER,
              CarbonCommonConstants.DEFAULT_PRESERVE_LATEST_SEGMENTS_NUMBER));
      // checking min and max . 0  , 100 is min & max.
      if (numberOfSegmentsToBePreserved < 0 || numberOfSegmentsToBePreserved > 100) {
        LOGGER.error("The specified value for property "
            + CarbonCommonConstants.PRESERVE_LATEST_SEGMENTS_NUMBER + " is incorrect."
            + " Correct value should be in range of 0 -100. Taking the default value.");
        numberOfSegmentsToBePreserved =
            Integer.parseInt(CarbonCommonConstants.DEFAULT_PRESERVE_LATEST_SEGMENTS_NUMBER);
      }
    } catch (NumberFormatException e) {
      numberOfSegmentsToBePreserved =
          Integer.parseInt(CarbonCommonConstants.DEFAULT_PRESERVE_LATEST_SEGMENTS_NUMBER);
    }
    return numberOfSegmentsToBePreserved;
  }

  public void print() {
    LOGGER.info("------Using Carbon.properties --------");
    LOGGER.info(carbonProperties.toString());
  }

}
