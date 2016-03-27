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
import java.util.Set;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.CarbonCommonConstants;

public final class CarbonProperties {
    /**
     * Attribute for Molap LOGGER.
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(CarbonProperties.class.getName());

    /**
     * class instance.
     */
    private static final CarbonProperties MOLAPPROPERTIESINSTANCE = new CarbonProperties();

    /**
     * porpeties .
     */
    private Properties molapProperties;

    /**
     * Private constructor this will call load properties method to load all the
     * molap properties in memory.
     */
    private CarbonProperties() {
        molapProperties = new Properties();
        loadProperties();
        validateAndLoadDefaultProperties();
    }

    /**
     * This method will be responsible for get this class instance
     *
     * @return molap properties instance
     */
    public static CarbonProperties getInstance() {
        return MOLAPPROPERTIESINSTANCE;
    }

    /**
     * This method validates the loaded properties and loads default
     * values in case of wrong values.
     */
    private void validateAndLoadDefaultProperties() {
        if (null == molapProperties.getProperty(CarbonCommonConstants.STORE_LOCATION)) {
            molapProperties.setProperty(CarbonCommonConstants.STORE_LOCATION,
                    CarbonCommonConstants.STORE_LOCATION_DEFAULT_VAL);
        }

        if (null == molapProperties.getProperty(CarbonCommonConstants.VALUESTORE_TYPE)) {
            molapProperties.setProperty(CarbonCommonConstants.VALUESTORE_TYPE,
                    CarbonCommonConstants.VALUESTORE_TYPE_DEFAULT_VAL);
        }

        if (null == molapProperties.getProperty(CarbonCommonConstants.KEYSTORE_TYPE)) {
            molapProperties.setProperty(CarbonCommonConstants.KEYSTORE_TYPE,
                    CarbonCommonConstants.KEYSTORE_TYPE_DEFAULT_VAL);
        }

        validateLeafNodeSize();
        validateMaxFileSize();
        validateNumCores();
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
    }

    private void validateBadRecordsLocation() {
        String badRecordsLocation =
                molapProperties.getProperty(CarbonCommonConstants.MOLAP_BADRECORDS_LOC);
        if (null == badRecordsLocation || badRecordsLocation.length() == 0) {
            molapProperties.setProperty(CarbonCommonConstants.MOLAP_BADRECORDS_LOC,
                    CarbonCommonConstants.MOLAP_BADRECORDS_LOC_DEFAULT_VAL);
        }
    }

    private void validateBadRecordsEncryption() {
        String badRecordsEncryption =
                molapProperties.getProperty(CarbonCommonConstants.MOLAP_BADRECORDS_ENCRYPTION);
        if (null == badRecordsEncryption || badRecordsEncryption.length() == 0) {
            molapProperties.setProperty(CarbonCommonConstants.MOLAP_BADRECORDS_ENCRYPTION,
                    CarbonCommonConstants.MOLAP_BADRECORDS_ENCRYPTION_DEFAULT_VAL);
        }
    }

    private void validateCsvFileSize() {
        try {
            int csvFileSizeProperty = Integer.parseInt(molapProperties
                    .getProperty(CarbonCommonConstants.MOLAP_DATALOAD_VALID_CSVFILE_SIZE,
                            CarbonCommonConstants.MOLAP_DATALOAD_VALID_CSVFILE_SIZE_DEFAULTVALUE));
            if (csvFileSizeProperty < 1) {
                LOGGER.info(CarbonCoreLogEvent.UNIBI_MOLAPCORE_MSG, "Invalid value for "
                        + CarbonCommonConstants.MOLAP_DATALOAD_VALID_CSVFILE_SIZE
                        + "\" Only Positive Integer(greater than zero) is allowed. Using the default value \""
                        + CarbonCommonConstants.MOLAP_DATALOAD_VALID_CSVFILE_SIZE_DEFAULTVALUE);

                molapProperties.setProperty(CarbonCommonConstants.MOLAP_DATALOAD_VALID_CSVFILE_SIZE,
                        CarbonCommonConstants.MOLAP_DATALOAD_VALID_CSVFILE_SIZE_DEFAULTVALUE);
            }
        } catch (NumberFormatException e) {
            LOGGER.info(CarbonCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                    "Invalid value for " + CarbonCommonConstants.MOLAP_DATALOAD_VALID_CSVFILE_SIZE
                            + "\" Only Positive Integer(greater than zero) is allowed. Using the default value \""
                            + CarbonCommonConstants.MOLAP_DATALOAD_VALID_CSVFILE_SIZE_DEFAULTVALUE);

            molapProperties.setProperty(CarbonCommonConstants.MOLAP_DATALOAD_VALID_CSVFILE_SIZE,
                    CarbonCommonConstants.MOLAP_DATALOAD_VALID_CSVFILE_SIZE_DEFAULTVALUE);
        }
    }

    private void validateNumberOfCsvFile() {
        try {
            int csvFileSizeProperty = Integer.parseInt(molapProperties
                    .getProperty(CarbonCommonConstants.MOLAP_DATALOAD_VALID_NUMBAER_OF_CSVFILE,
                            CarbonCommonConstants.MOLAP_DATALOAD_VALID_NUMBAER_OF_CSVFILE_DEFAULTVALUE));
            if (csvFileSizeProperty < 1) {
                LOGGER.info(CarbonCoreLogEvent.UNIBI_MOLAPCORE_MSG, "Invalid value for "
                        + CarbonCommonConstants.MOLAP_DATALOAD_VALID_NUMBAER_OF_CSVFILE
                        + "\" Only Positive Integer(greater than zero) is allowed. Using the default value \""
                        + CarbonCommonConstants.MOLAP_DATALOAD_VALID_NUMBAER_OF_CSVFILE_DEFAULTVALUE);

                molapProperties
                        .setProperty(CarbonCommonConstants.MOLAP_DATALOAD_VALID_NUMBAER_OF_CSVFILE,
                                CarbonCommonConstants.MOLAP_DATALOAD_VALID_NUMBAER_OF_CSVFILE_DEFAULTVALUE);
            }
        } catch (NumberFormatException e) {
            LOGGER.info(CarbonCoreLogEvent.UNIBI_MOLAPCORE_MSG, "Invalid value for "
                    + CarbonCommonConstants.MOLAP_DATALOAD_VALID_NUMBAER_OF_CSVFILE
                    + "\" Only Positive Integer(greater than zero) is allowed. Using the default value \""
                    + CarbonCommonConstants.MOLAP_DATALOAD_VALID_NUMBAER_OF_CSVFILE_DEFAULTVALUE);

            molapProperties
                    .setProperty(CarbonCommonConstants.MOLAP_DATALOAD_VALID_NUMBAER_OF_CSVFILE,
                            CarbonCommonConstants.MOLAP_DATALOAD_VALID_NUMBAER_OF_CSVFILE_DEFAULTVALUE);
        }
    }

    /**
     * This method validates the batch size
     */
    private void validateOnlineMergerSize() {
        String onlineMergeSize = molapProperties
                .getProperty(CarbonCommonConstants.ONLINE_MERGE_FILE_SIZE,
                        CarbonCommonConstants.ONLINE_MERGE_FILE_SIZE_DEFAULT_VALUE);
        try {
            int offlineMergerSize = Integer.parseInt(onlineMergeSize);

            if (offlineMergerSize < CarbonCommonConstants.ONLINE_MERGE_MIN_VALUE
                    || offlineMergerSize > CarbonCommonConstants.ONLINE_MERGE_MAX_VALUE) {
                LOGGER.info(CarbonCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                        "The online Merge Size value \"" + onlineMergeSize
                                + "\" is invalid. Using the default value \""
                                + CarbonCommonConstants.ONLINE_MERGE_FILE_SIZE_DEFAULT_VALUE);
                molapProperties.setProperty(CarbonCommonConstants.ONLINE_MERGE_FILE_SIZE,
                        CarbonCommonConstants.ONLINE_MERGE_FILE_SIZE_DEFAULT_VALUE);
            }
        } catch (NumberFormatException e) {
            LOGGER.info(CarbonCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                    "The online Merge Size value \"" + onlineMergeSize
                            + "\" is invalid. Using the default value \""
                            + CarbonCommonConstants.ONLINE_MERGE_FILE_SIZE_DEFAULT_VALUE);
            molapProperties.setProperty(CarbonCommonConstants.ONLINE_MERGE_FILE_SIZE,
                    CarbonCommonConstants.ONLINE_MERGE_FILE_SIZE_DEFAULT_VALUE);
        }
    }

    /**
     * This method validates the batch size
     */
    private void validateOfflineMergerSize() {
        String offLineMergerSize = molapProperties
                .getProperty(CarbonCommonConstants.OFFLINE_MERGE_FILE_SIZE,
                        CarbonCommonConstants.OFFLINE_MERGE_FILE_SIZE_DEFAULT_VALUE);
        try {
            int offLineMergeSize = Integer.parseInt(offLineMergerSize);

            if (offLineMergeSize < CarbonCommonConstants.OFFLINE_MERGE_MIN_VALUE
                    || offLineMergeSize > CarbonCommonConstants.OFFLINE_MERGE_MAX_VALUE) {
                LOGGER.info(CarbonCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                        "The offline Merge Size value \"" + offLineMergerSize
                                + "\" is invalid. Using the default value \""
                                + CarbonCommonConstants.OFFLINE_MERGE_FILE_SIZE_DEFAULT_VALUE);
                molapProperties.setProperty(CarbonCommonConstants.OFFLINE_MERGE_FILE_SIZE,
                        CarbonCommonConstants.OFFLINE_MERGE_FILE_SIZE_DEFAULT_VALUE);
            }
        } catch (NumberFormatException e) {
            LOGGER.info(CarbonCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                    "The offline Merge Size value \"" + offLineMergerSize
                            + "\" is invalid. Using the default value \""
                            + CarbonCommonConstants.OFFLINE_MERGE_FILE_SIZE_DEFAULT_VALUE);
            molapProperties.setProperty(CarbonCommonConstants.OFFLINE_MERGE_FILE_SIZE,
                    CarbonCommonConstants.OFFLINE_MERGE_FILE_SIZE_DEFAULT_VALUE);
        }
    }

    /**
     * This method validates the batch size
     */
    private void validateBatchSize() {
        String batchSizeStr = molapProperties.getProperty(CarbonCommonConstants.BATCH_SIZE,
                CarbonCommonConstants.BATCH_SIZE_DEFAULT_VAL);
        try {
            int batchSize = Integer.parseInt(batchSizeStr);

            if (batchSize < CarbonCommonConstants.BATCH_SIZE_MIN_VAL
                    || batchSize > CarbonCommonConstants.BATCH_SIZE_MAX_VAL) {
                LOGGER.info(CarbonCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                        "The batch size value \"" + batchSizeStr
                                + "\" is invalid. Using the default value \""
                                + CarbonCommonConstants.BATCH_SIZE_DEFAULT_VAL);
                molapProperties.setProperty(CarbonCommonConstants.BATCH_SIZE,
                        CarbonCommonConstants.BATCH_SIZE_DEFAULT_VAL);
            }
        } catch (NumberFormatException e) {
            LOGGER.info(CarbonCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                    "The batch size value \"" + batchSizeStr
                            + "\" is invalid. Using the default value \""
                            + CarbonCommonConstants.BATCH_SIZE_DEFAULT_VAL);
            molapProperties.setProperty(CarbonCommonConstants.BATCH_SIZE,
                    CarbonCommonConstants.BATCH_SIZE_DEFAULT_VAL);
        }
    }

    /**
     * This method validates the batch size
     */
    private void validateCardinalityIncrementValue() {
        String cardinalityIncr = molapProperties
                .getProperty(CarbonCommonConstants.CARDINALITY_INCREMENT_VALUE,
                        CarbonCommonConstants.CARDINALITY_INCREMENT_VALUE_DEFAULT_VAL);
        try {
            int batchSize = Integer.parseInt(cardinalityIncr);

            if (batchSize < CarbonCommonConstants.CARDINALITY_INCREMENT_MIN_VAL
                    || batchSize > CarbonCommonConstants.CARDINALITY_INCREMENT_MAX_VAL) {
                LOGGER.info(CarbonCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                        "The batch size value \"" + cardinalityIncr
                                + "\" is invalid. Using the default value \""
                                + CarbonCommonConstants.CARDINALITY_INCREMENT_VALUE_DEFAULT_VAL);
                molapProperties.setProperty(CarbonCommonConstants.CARDINALITY_INCREMENT_VALUE,
                        CarbonCommonConstants.CARDINALITY_INCREMENT_VALUE_DEFAULT_VAL);
            }
        } catch (NumberFormatException e) {
            LOGGER.info(CarbonCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                    "The cardinality size value \"" + cardinalityIncr
                            + "\" is invalid. Using the default value \""
                            + CarbonCommonConstants.BATCH_SIZE_DEFAULT_VAL);
            molapProperties.setProperty(CarbonCommonConstants.CARDINALITY_INCREMENT_VALUE,
                    CarbonCommonConstants.CARDINALITY_INCREMENT_VALUE_DEFAULT_VAL);
        }
    }

    /**
     * This method validates the Leaf node size
     */
    private void validateLeafNodeSize() {
        String leafNodeSizeStr = molapProperties.getProperty(CarbonCommonConstants.LEAFNODE_SIZE,
                CarbonCommonConstants.LEAFNODE_SIZE_DEFAULT_VAL);
        try {
            int leafNodeSize = Integer.parseInt(leafNodeSizeStr);

            if (leafNodeSize < CarbonCommonConstants.LEAFNODE_SIZE_MIN_VAL
                    || leafNodeSize > CarbonCommonConstants.LEAFNODE_SIZE_MAX_VAL) {
                LOGGER.info(CarbonCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                        "The lefa node size value \"" + leafNodeSizeStr
                                + "\" is invalid. Using the default value \""
                                + CarbonCommonConstants.LEAFNODE_SIZE_DEFAULT_VAL);
                molapProperties.setProperty(CarbonCommonConstants.LEAFNODE_SIZE,
                        CarbonCommonConstants.LEAFNODE_SIZE_DEFAULT_VAL);
            }
        } catch (NumberFormatException e) {
            LOGGER.info(CarbonCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                    "The lefa node size value \"" + leafNodeSizeStr
                            + "\" is invalid. Using the default value \""
                            + CarbonCommonConstants.LEAFNODE_SIZE_DEFAULT_VAL);
            molapProperties.setProperty(CarbonCommonConstants.LEAFNODE_SIZE,
                    CarbonCommonConstants.LEAFNODE_SIZE_DEFAULT_VAL);
        }
    }

    /**
     * This method validates data load queue size
     */
    private void validateDataLoadQSize() {
        String dataLoadQSize = molapProperties.getProperty(CarbonCommonConstants.DATA_LOAD_Q_SIZE,
                CarbonCommonConstants.DATA_LOAD_Q_SIZE_DEFAULT);
        try {
            int dataLoadQSizeInt = Integer.parseInt(dataLoadQSize);

            if (dataLoadQSizeInt < CarbonCommonConstants.DATA_LOAD_Q_SIZE_MIN
                    || dataLoadQSizeInt > CarbonCommonConstants.DATA_LOAD_Q_SIZE_MAX) {
                LOGGER.info(CarbonCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                        "The data load queue size value \"" + dataLoadQSize
                                + "\" is invalid. Using the default value \""
                                + CarbonCommonConstants.DATA_LOAD_Q_SIZE_DEFAULT);
                molapProperties.setProperty(CarbonCommonConstants.DATA_LOAD_Q_SIZE,
                        CarbonCommonConstants.DATA_LOAD_Q_SIZE_DEFAULT);
            }
        } catch (NumberFormatException e) {
            LOGGER.info(CarbonCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                    "The data load queue size value \"" + dataLoadQSize
                            + "\" is invalid. Using the default value \""
                            + CarbonCommonConstants.DATA_LOAD_Q_SIZE_DEFAULT);
            molapProperties.setProperty(CarbonCommonConstants.DATA_LOAD_Q_SIZE,
                    CarbonCommonConstants.DATA_LOAD_Q_SIZE_DEFAULT);
        }
    }

    /**
     * This method validates the data load concurrent exec size
     */
    private void validateDataLoadConcExecSize() {
        String dataLoadConcExecSize = molapProperties
                .getProperty(CarbonCommonConstants.DATA_LOAD_CONC_EXE_SIZE,
                        CarbonCommonConstants.DATA_LOAD_CONC_EXE_SIZE_DEFAULT);
        try {
            int dataLoadConcExecSizeInt = Integer.parseInt(dataLoadConcExecSize);

            if (dataLoadConcExecSizeInt < CarbonCommonConstants.DATA_LOAD_CONC_EXE_SIZE_MIN
                    || dataLoadConcExecSizeInt > CarbonCommonConstants.DATA_LOAD_CONC_EXE_SIZE_MAX) {
                LOGGER.info(CarbonCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                        "The data load concurrent exec size value \"" + dataLoadConcExecSize
                                + "\" is invalid. Using the default value \""
                                + CarbonCommonConstants.DATA_LOAD_CONC_EXE_SIZE_DEFAULT);
                molapProperties.setProperty(CarbonCommonConstants.DATA_LOAD_CONC_EXE_SIZE,
                        CarbonCommonConstants.DATA_LOAD_CONC_EXE_SIZE_DEFAULT);
            }
        } catch (NumberFormatException e) {
            LOGGER.info(CarbonCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                    "The data load concurrent exec size value \"" + dataLoadConcExecSize
                            + "\" is invalid. Using the default value \""
                            + CarbonCommonConstants.DATA_LOAD_CONC_EXE_SIZE_DEFAULT);
            molapProperties.setProperty(CarbonCommonConstants.DATA_LOAD_CONC_EXE_SIZE,
                    CarbonCommonConstants.DATA_LOAD_CONC_EXE_SIZE_DEFAULT);
        }
    }

    /**
     * This method validates the decimal pointers size
     */
    private void validateDecimalPointers() {
        String decimalPointers = molapProperties
                .getProperty(CarbonCommonConstants.MOLAP_DECIMAL_POINTERS,
                        CarbonCommonConstants.MOLAP_DECIMAL_POINTERS_DEFAULT);
        try {
            int decimalPointersInt = Integer.parseInt(decimalPointers);

            if (decimalPointersInt < 0 || decimalPointersInt > 15) {
                LOGGER.info(CarbonCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                        "The decimal pointers agg \"" + decimalPointers
                                + "\" is invalid. Using the default value \""
                                + CarbonCommonConstants.DATA_LOAD_CONC_EXE_SIZE_DEFAULT);
                molapProperties.setProperty(CarbonCommonConstants.MOLAP_DECIMAL_POINTERS,
                        CarbonCommonConstants.MOLAP_DECIMAL_POINTERS_DEFAULT);
            }
        } catch (NumberFormatException e) {
            LOGGER.info(CarbonCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                    "The decimal pointers agg \"" + decimalPointers
                            + "\" is invalid. Using the default value \""
                            + CarbonCommonConstants.DATA_LOAD_CONC_EXE_SIZE_DEFAULT);
            molapProperties.setProperty(CarbonCommonConstants.MOLAP_DECIMAL_POINTERS,
                    CarbonCommonConstants.MOLAP_DECIMAL_POINTERS_DEFAULT);
        }
    }

    /**
     * This method validates the data load concurrent exec size
     */
    private void validateDecimalPointersAgg() {
        String decimalPointers = molapProperties
                .getProperty(CarbonCommonConstants.MOLAP_DECIMAL_POINTERS_AGG,
                        CarbonCommonConstants.MOLAP_DECIMAL_POINTERS_AGG_DEFAULT);
        try {
            int decimalPointersInt = Integer.parseInt(decimalPointers);

            if (decimalPointersInt < 0 || decimalPointersInt > 15) {
                LOGGER.info(CarbonCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                        "The decimal pointers agg \"" + decimalPointers
                                + "\" is invalid. Using the default value \""
                                + CarbonCommonConstants.MOLAP_DECIMAL_POINTERS_AGG_DEFAULT);
                molapProperties.setProperty(CarbonCommonConstants.MOLAP_DECIMAL_POINTERS_AGG,
                        CarbonCommonConstants.MOLAP_DECIMAL_POINTERS_AGG_DEFAULT);
            }
        } catch (NumberFormatException e) {
            LOGGER.info(CarbonCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                    "The decimal pointers agg \"" + decimalPointers
                            + "\" is invalid. Using the default value \""
                            + CarbonCommonConstants.MOLAP_DECIMAL_POINTERS_AGG_DEFAULT);
            molapProperties.setProperty(CarbonCommonConstants.MOLAP_DECIMAL_POINTERS_AGG,
                    CarbonCommonConstants.MOLAP_DECIMAL_POINTERS_AGG_DEFAULT);
        }
    }

    /**
     * This method validates the maximum number of
     * LeafNodes per file.
     */
    private void validateMaxFileSize() {
        String maxFileSizeStr = molapProperties.getProperty(CarbonCommonConstants.MAX_FILE_SIZE,
                CarbonCommonConstants.MAX_FILE_SIZE_DEFAULT_VAL);
        try {
            int maxFileSize = Integer.parseInt(maxFileSizeStr);

            if (maxFileSize < CarbonCommonConstants.MAX_FILE_SIZE_DEFAULT_VAL_MIN_VAL
                    || maxFileSize > CarbonCommonConstants.MAX_FILE_SIZE_DEFAULT_VAL_MAX_VAL) {
                LOGGER.info(CarbonCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                        "The max file size value \"" + maxFileSizeStr
                                + "\" is invalid. Using the default value \""
                                + CarbonCommonConstants.MAX_FILE_SIZE_DEFAULT_VAL);
                molapProperties.setProperty(CarbonCommonConstants.MAX_FILE_SIZE,
                        CarbonCommonConstants.MAX_FILE_SIZE_DEFAULT_VAL);
            }
        } catch (NumberFormatException e) {
            LOGGER.info(CarbonCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                    "The max file size value \"" + maxFileSizeStr
                            + "\" is invalid. Using the default value \""
                            + CarbonCommonConstants.MAX_FILE_SIZE_DEFAULT_VAL);

            molapProperties.setProperty(CarbonCommonConstants.MAX_FILE_SIZE,
                    CarbonCommonConstants.MAX_FILE_SIZE_DEFAULT_VAL);
        }
    }

    /**
     * This method validates the number cores specified
     */
    private void validateNumCores() {
        String numCoresStr = molapProperties.getProperty(CarbonCommonConstants.NUM_CORES,
                CarbonCommonConstants.NUM_CORES_DEFAULT_VAL);
        try {
            int numCores = Integer.parseInt(numCoresStr);

            if (numCores < CarbonCommonConstants.NUM_CORES_MIN_VAL
                    || numCores > CarbonCommonConstants.NUM_CORES_MAX_VAL) {
                LOGGER.info(CarbonCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                        "The num Cores  value \"" + numCoresStr
                                + "\" is invalid. Using the default value \""
                                + CarbonCommonConstants.NUM_CORES_DEFAULT_VAL);
                molapProperties.setProperty(CarbonCommonConstants.NUM_CORES,
                        CarbonCommonConstants.NUM_CORES_DEFAULT_VAL);
            }
        } catch (NumberFormatException e) {
            LOGGER.info(CarbonCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                    "The num Cores  value \"" + numCoresStr
                            + "\" is invalid. Using the default value \""
                            + CarbonCommonConstants.NUM_CORES_DEFAULT_VAL);
            molapProperties.setProperty(CarbonCommonConstants.NUM_CORES,
                    CarbonCommonConstants.NUM_CORES_DEFAULT_VAL);
        }
    }

    /**
     * This method validates the sort size
     */
    private void validateSortSize() {
        String sortSizeStr = molapProperties.getProperty(CarbonCommonConstants.SORT_SIZE,
                CarbonCommonConstants.SORT_SIZE_DEFAULT_VAL);
        try {
            int sortSize = Integer.parseInt(sortSizeStr);

            if (sortSize < CarbonCommonConstants.SORT_SIZE_MIN_VAL) {
                LOGGER.info(CarbonCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                        "The batch size value \"" + sortSizeStr
                                + "\" is invalid. Using the default value \""
                                + CarbonCommonConstants.SORT_SIZE_DEFAULT_VAL);
                molapProperties.setProperty(CarbonCommonConstants.SORT_SIZE,
                        CarbonCommonConstants.SORT_SIZE_DEFAULT_VAL);
            }
        } catch (NumberFormatException e) {
            LOGGER.info(CarbonCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                    "The batch size value \"" + sortSizeStr
                            + "\" is invalid. Using the default value \""
                            + CarbonCommonConstants.SORT_SIZE_DEFAULT_VAL);
            molapProperties.setProperty(CarbonCommonConstants.SORT_SIZE,
                    CarbonCommonConstants.SORT_SIZE_DEFAULT_VAL);
        }
    }

    /**
     * This method validates the sort size
     */
    private void validateSortBufferSize() {
        String sortSizeStr = molapProperties.getProperty(CarbonCommonConstants.SORT_BUFFER_SIZE,
                CarbonCommonConstants.SORT_BUFFER_SIZE_DEFAULT_VALUE);
        try {
            int sortSize = Integer.parseInt(sortSizeStr);

            if (sortSize < CarbonCommonConstants.SORT_BUFFER_SIZE_MIN_VALUE) {
                LOGGER.info(CarbonCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                        "The batch size value \"" + sortSizeStr
                                + "\" is invalid. Using the default value \""
                                + CarbonCommonConstants.SORT_BUFFER_SIZE_DEFAULT_VALUE);
                molapProperties.setProperty(CarbonCommonConstants.SORT_BUFFER_SIZE,
                        CarbonCommonConstants.SORT_BUFFER_SIZE_DEFAULT_VALUE);
            }
        } catch (NumberFormatException e) {
            LOGGER.info(CarbonCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                    "The batch size value \"" + sortSizeStr
                            + "\" is invalid. Using the default value \""
                            + CarbonCommonConstants.SORT_BUFFER_SIZE_DEFAULT_VALUE);
            molapProperties.setProperty(CarbonCommonConstants.SORT_BUFFER_SIZE,
                    CarbonCommonConstants.SORT_BUFFER_SIZE_DEFAULT_VALUE);
        }
    }

    /**
     * This method will read all the properties from file and load it into
     * memory
     */
    private void loadProperties() {
        String property = System.getProperty("molap.properties.filepath");
        if (null == property) {
            property = CarbonCommonConstants.MOLAP_PROPERTIES_FILE_PATH;
        }
        File file = new File(property);
        LOGGER.info(CarbonCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                "Property file path: " + file.getAbsolutePath());

        FileInputStream fis = null;
        try {
            if (file.exists()) {
                fis = new FileInputStream(file);

                molapProperties.load(fis);
            }
        } catch (FileNotFoundException e) {
            LOGGER.error(CarbonCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                    "The file: " + CarbonCommonConstants.MOLAP_PROPERTIES_FILE_PATH
                            + " does not exist");
        } catch (IOException e) {
            LOGGER.error(CarbonCoreLogEvent.UNIBI_MOLAPCORE_MSG, "Error while reading the file: "
                    + CarbonCommonConstants.MOLAP_PROPERTIES_FILE_PATH);
        } finally {
            if (null != fis) {
                try {
                    fis.close();
                } catch (IOException e) {
                    LOGGER.error(CarbonCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                            "Error while closing the file stream for file: "
                                    + CarbonCommonConstants.MOLAP_PROPERTIES_FILE_PATH);
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
        if ("molap.leaf.node.size".equals(key)) {
            return "120000";
        }
        return molapProperties.getProperty(key);
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

    public String[] getAllProperties() {
        Set<Object> set = molapProperties.keySet();
        String[] allProps = new String[set.size()];
        int i = 0;
        for (Object obj : set) {
            allProps[i++] = obj.toString();
        }
        return allProps;
    }

    /**
     * This method will be used to add a new property
     *
     * @param key
     * @return properties value
     */
    public void addProperty(String key, String value) {
        molapProperties.setProperty(key, value);

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

    public void print() {
        LOGGER.info(CarbonCoreLogEvent.UNIBI_MOLAPCORE_MSG, "------Using Molap.properties --------");
        LOGGER.info(CarbonCoreLogEvent.UNIBI_MOLAPCORE_MSG, molapProperties);
    }

}
