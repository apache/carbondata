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

package org.apache.carbondata.core.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.constants.CarbonV3DataFormatConstants;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;

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

    validateBlockletSize();
    validateNumCores();
    validateNumCoresBlockSort();
    validateSortSize();
    validateBadRecordsLocation();
    validateHighCardinalityIdentify();
    validateHighCardinalityThreshold();
    validateHighCardinalityInRowCountPercentage();
    validateCarbonDataFileVersion();
    validateExecutorStartUpTime();
    validatePrefetchBufferSize();
    validateBlockletGroupSizeInMB();
    validateNumberOfColumnPerIORead();
    validateNumberOfRowsPerBlockletColumnPage();
  }

  private void validatePrefetchBufferSize() {
    String prefetchBufferSizeStr =
        carbonProperties.getProperty(CarbonCommonConstants.CARBON_PREFETCH_BUFFERSIZE);

    if (null == prefetchBufferSizeStr || prefetchBufferSizeStr.length() == 0) {
      carbonProperties.setProperty(CarbonCommonConstants.CARBON_PREFETCH_BUFFERSIZE,
          CarbonCommonConstants.CARBON_PREFETCH_BUFFERSIZE_DEFAULT);
    } else {
      try {
        Integer.parseInt(prefetchBufferSizeStr);
      } catch (NumberFormatException e) {
        LOGGER.info("The prefetch buffer size value \"" + prefetchBufferSizeStr
            + "\" is invalid. Using the default value \""
            + CarbonCommonConstants.CARBON_PREFETCH_BUFFERSIZE_DEFAULT + "\"");
        carbonProperties.setProperty(CarbonCommonConstants.CARBON_PREFETCH_BUFFERSIZE,
            CarbonCommonConstants.CARBON_PREFETCH_BUFFERSIZE_DEFAULT);
      }
    }
  }

  /**
   * This method validates the number of pages per blocklet column
   */
  private void validateBlockletGroupSizeInMB() {
    String numberOfPagePerBlockletColumnString = carbonProperties
        .getProperty(CarbonV3DataFormatConstants.BLOCKLET_SIZE_IN_MB,
            CarbonV3DataFormatConstants.BLOCKLET_SIZE_IN_MB_DEFAULT_VALUE);
    try {
      short numberOfPagePerBlockletColumn = Short.parseShort(numberOfPagePerBlockletColumnString);
      if (numberOfPagePerBlockletColumn < CarbonV3DataFormatConstants.BLOCKLET_SIZE_IN_MB_MIN) {
        LOGGER.info("Blocklet Size Configured value \"" + numberOfPagePerBlockletColumnString
            + "\" is invalid. Using the default value \""
            + CarbonV3DataFormatConstants.BLOCKLET_SIZE_IN_MB_DEFAULT_VALUE);
        carbonProperties.setProperty(CarbonV3DataFormatConstants.BLOCKLET_SIZE_IN_MB,
            CarbonV3DataFormatConstants.BLOCKLET_SIZE_IN_MB_DEFAULT_VALUE);
      }
    } catch (NumberFormatException e) {
      LOGGER.info("Blocklet Size Configured value \"" + numberOfPagePerBlockletColumnString
          + "\" is invalid. Using the default value \""
          + CarbonV3DataFormatConstants.BLOCKLET_SIZE_IN_MB_DEFAULT_VALUE);
      carbonProperties.setProperty(CarbonV3DataFormatConstants.BLOCKLET_SIZE_IN_MB,
          CarbonV3DataFormatConstants.BLOCKLET_SIZE_IN_MB_DEFAULT_VALUE);
    }
    LOGGER.info("Blocklet Size Configured value is \"" + carbonProperties
        .getProperty(CarbonV3DataFormatConstants.BLOCKLET_SIZE_IN_MB,
            CarbonV3DataFormatConstants.BLOCKLET_SIZE_IN_MB_DEFAULT_VALUE));
  }

  /**
   * This method validates the number of column read in one IO
   */
  private void validateNumberOfColumnPerIORead() {
    String numberofColumnPerIOString = carbonProperties
        .getProperty(CarbonV3DataFormatConstants.NUMBER_OF_COLUMN_TO_READ_IN_IO,
            CarbonV3DataFormatConstants.NUMBER_OF_COLUMN_TO_READ_IN_IO_DEFAULTVALUE);
    try {
      short numberofColumnPerIO = Short.parseShort(numberofColumnPerIOString);
      if (numberofColumnPerIO < CarbonV3DataFormatConstants.NUMBER_OF_COLUMN_TO_READ_IN_IO_MIN
          || numberofColumnPerIO > CarbonV3DataFormatConstants.NUMBER_OF_COLUMN_TO_READ_IN_IO_MAX) {
        LOGGER.info("The Number Of pages per blocklet column value \"" + numberofColumnPerIOString
            + "\" is invalid. Using the default value \""
            + CarbonV3DataFormatConstants.NUMBER_OF_COLUMN_TO_READ_IN_IO_DEFAULTVALUE);
        carbonProperties.setProperty(CarbonV3DataFormatConstants.NUMBER_OF_COLUMN_TO_READ_IN_IO,
            CarbonV3DataFormatConstants.NUMBER_OF_COLUMN_TO_READ_IN_IO_DEFAULTVALUE);
      }
    } catch (NumberFormatException e) {
      LOGGER.info("The Number Of pages per blocklet column value \"" + numberofColumnPerIOString
          + "\" is invalid. Using the default value \""
          + CarbonV3DataFormatConstants.NUMBER_OF_COLUMN_TO_READ_IN_IO_DEFAULTVALUE);
      carbonProperties.setProperty(CarbonV3DataFormatConstants.NUMBER_OF_COLUMN_TO_READ_IN_IO,
          CarbonV3DataFormatConstants.NUMBER_OF_COLUMN_TO_READ_IN_IO_DEFAULTVALUE);
    }
  }

  /**
   * This method validates the number of column read in one IO
   */
  private void validateNumberOfRowsPerBlockletColumnPage() {
    String numberOfRowsPerBlockletColumnPageString = carbonProperties
        .getProperty(CarbonV3DataFormatConstants.NUMBER_OF_ROWS_PER_BLOCKLET_COLUMN_PAGE,
            CarbonV3DataFormatConstants.NUMBER_OF_ROWS_PER_BLOCKLET_COLUMN_PAGE_DEFAULT);
    try {
      short numberOfRowsPerBlockletColumnPage =
          Short.parseShort(numberOfRowsPerBlockletColumnPageString);
      if (numberOfRowsPerBlockletColumnPage
          < CarbonV3DataFormatConstants.NUMBER_OF_ROWS_PER_BLOCKLET_COLUMN_PAGE_MIN
          || numberOfRowsPerBlockletColumnPage
          > CarbonV3DataFormatConstants.NUMBER_OF_ROWS_PER_BLOCKLET_COLUMN_PAGE_MAX) {
        LOGGER.info("The Number Of rows per blocklet column pages value \""
            + numberOfRowsPerBlockletColumnPageString + "\" is invalid. Using the default value \""
            + CarbonV3DataFormatConstants.NUMBER_OF_ROWS_PER_BLOCKLET_COLUMN_PAGE_DEFAULT);
        carbonProperties
            .setProperty(CarbonV3DataFormatConstants.NUMBER_OF_ROWS_PER_BLOCKLET_COLUMN_PAGE,
                CarbonV3DataFormatConstants.NUMBER_OF_ROWS_PER_BLOCKLET_COLUMN_PAGE_DEFAULT);
      }
    } catch (NumberFormatException e) {
      LOGGER.info("The Number Of rows per blocklet column pages value \""
          + numberOfRowsPerBlockletColumnPageString + "\" is invalid. Using the default value \""
          + CarbonV3DataFormatConstants.NUMBER_OF_ROWS_PER_BLOCKLET_COLUMN_PAGE_DEFAULT);
      carbonProperties
          .setProperty(CarbonV3DataFormatConstants.NUMBER_OF_ROWS_PER_BLOCKLET_COLUMN_PAGE,
              CarbonV3DataFormatConstants.NUMBER_OF_ROWS_PER_BLOCKLET_COLUMN_PAGE_DEFAULT);
    }
  }

  private void validateBadRecordsLocation() {
    String badRecordsLocation =
        carbonProperties.getProperty(CarbonCommonConstants.CARBON_BADRECORDS_LOC);
    if (null == badRecordsLocation || badRecordsLocation.length() == 0) {
      carbonProperties.setProperty(CarbonCommonConstants.CARBON_BADRECORDS_LOC,
          CarbonCommonConstants.CARBON_BADRECORDS_LOC_DEFAULT_VAL);
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
   * This method validates the number cores specified
   */
  private void validateNumCores() {
    String numCoresStr = carbonProperties
        .getProperty(CarbonCommonConstants.NUM_CORES, CarbonCommonConstants.NUM_CORES_DEFAULT_VAL);
    try {
      int numCores = Integer.parseInt(numCoresStr);

      if (numCores < CarbonCommonConstants.NUM_CORES_MIN_VAL
          || numCores > CarbonCommonConstants.NUM_CORES_MAX_VAL) {
        LOGGER.info(
            "The num Cores  value \"" + numCoresStr + "\" is invalid. Using the default value \""
                + CarbonCommonConstants.NUM_CORES_DEFAULT_VAL);
        carbonProperties.setProperty(CarbonCommonConstants.NUM_CORES,
            CarbonCommonConstants.NUM_CORES_DEFAULT_VAL);
      }
    } catch (NumberFormatException e) {
      LOGGER.info(
          "The num Cores  value \"" + numCoresStr + "\" is invalid. Using the default value \""
              + CarbonCommonConstants.NUM_CORES_DEFAULT_VAL);
      carbonProperties.setProperty(CarbonCommonConstants.NUM_CORES,
          CarbonCommonConstants.NUM_CORES_DEFAULT_VAL);
    }
  }

  /**
   * This method validates the number cores specified for mdk block sort
   */
  private void validateNumCoresBlockSort() {
    String numCoresStr = carbonProperties.getProperty(CarbonCommonConstants.NUM_CORES_BLOCK_SORT,
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
        LOGGER.info(
            "The batch size value \"" + sortSizeStr + "\" is invalid. Using the default value \""
                + CarbonCommonConstants.SORT_SIZE_DEFAULT_VAL);
        carbonProperties.setProperty(CarbonCommonConstants.SORT_SIZE,
            CarbonCommonConstants.SORT_SIZE_DEFAULT_VAL);
      }
    } catch (NumberFormatException e) {
      LOGGER.info(
          "The batch size value \"" + sortSizeStr + "\" is invalid. Using the default value \""
              + CarbonCommonConstants.SORT_SIZE_DEFAULT_VAL);
      carbonProperties.setProperty(CarbonCommonConstants.SORT_SIZE,
          CarbonCommonConstants.SORT_SIZE_DEFAULT_VAL);
    }
  }

  private void validateHighCardinalityIdentify() {
    String highcardIdentifyStr = carbonProperties
        .getProperty(CarbonCommonConstants.HIGH_CARDINALITY_IDENTIFY_ENABLE,
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
    String highcardThresholdStr = carbonProperties
        .getProperty(CarbonCommonConstants.HIGH_CARDINALITY_THRESHOLD,
            CarbonCommonConstants.HIGH_CARDINALITY_THRESHOLD_DEFAULT);
    try {
      int highcardThreshold = Integer.parseInt(highcardThresholdStr);
      if (highcardThreshold < CarbonCommonConstants.HIGH_CARDINALITY_THRESHOLD_MIN) {
        LOGGER.info("The high cardinality threshold value \"" + highcardThresholdStr
            + "\" is invalid. Using the min value \""
            + CarbonCommonConstants.HIGH_CARDINALITY_THRESHOLD_MIN);
        carbonProperties.setProperty(CarbonCommonConstants.HIGH_CARDINALITY_THRESHOLD,
            CarbonCommonConstants.HIGH_CARDINALITY_THRESHOLD_MIN + "");
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
    String highcardPercentageStr = carbonProperties
        .getProperty(CarbonCommonConstants.HIGH_CARDINALITY_IN_ROW_COUNT_PERCENTAGE,
            CarbonCommonConstants.HIGH_CARDINALITY_IN_ROW_COUNT_PERCENTAGE_DEFAULT);
    try {
      double highcardPercentage = Double.parseDouble(highcardPercentageStr);
      if (highcardPercentage <= 0) {
        LOGGER.info(
            "The percentage of high cardinality in row count value \"" + highcardPercentageStr
                + "\" is invalid. Using the default value \""
                + CarbonCommonConstants.HIGH_CARDINALITY_IN_ROW_COUNT_PERCENTAGE_DEFAULT);
        carbonProperties.setProperty(CarbonCommonConstants.HIGH_CARDINALITY_IN_ROW_COUNT_PERCENTAGE,
            CarbonCommonConstants.HIGH_CARDINALITY_IN_ROW_COUNT_PERCENTAGE_DEFAULT);
      }
    } catch (NumberFormatException e) {
      LOGGER.info("The percentage of high cardinality in row count value \"" + highcardPercentageStr
          + "\" is invalid. Using the default value \""
          + CarbonCommonConstants.HIGH_CARDINALITY_IN_ROW_COUNT_PERCENTAGE_DEFAULT);
      carbonProperties.setProperty(CarbonCommonConstants.HIGH_CARDINALITY_IN_ROW_COUNT_PERCENTAGE,
          CarbonCommonConstants.HIGH_CARDINALITY_IN_ROW_COUNT_PERCENTAGE_DEFAULT);
    }
  }

  /**
   * Below method will be used to validate the data file version parameter
   * if parameter is invalid current version will be set
   */
  private void validateCarbonDataFileVersion() {
    String carbondataFileVersionString =
        carbonProperties.getProperty(CarbonCommonConstants.CARBON_DATA_FILE_VERSION);
    if (carbondataFileVersionString == null) {
      // use default property if user does not specify version property
      carbonProperties.setProperty(CarbonCommonConstants.CARBON_DATA_FILE_VERSION,
          CarbonCommonConstants.CARBON_DATA_FILE_DEFAULT_VERSION);
    } else {
      try {
        ColumnarFormatVersion.valueOf(carbondataFileVersionString);
      } catch (IllegalArgumentException e) {
        // use default property if user specifies an invalid version property
        LOGGER.warn("Specified file version property is invalid: " + carbondataFileVersionString
            + ". Using " + CarbonCommonConstants.CARBON_DATA_FILE_DEFAULT_VERSION
            + " as default file version");
        carbonProperties.setProperty(CarbonCommonConstants.CARBON_DATA_FILE_VERSION,
            CarbonCommonConstants.CARBON_DATA_FILE_DEFAULT_VERSION);
      }
    }
    LOGGER.info("Carbon Current data file version: " + carbonProperties
        .setProperty(CarbonCommonConstants.CARBON_DATA_FILE_VERSION,
            CarbonCommonConstants.CARBON_DATA_FILE_DEFAULT_VERSION));
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
      LOGGER.error(
          "The file: " + CarbonCommonConstants.CARBON_PROPERTIES_FILE_PATH + " does not exist");
    } catch (IOException e) {
      LOGGER.error(
          "Error while reading the file: " + CarbonCommonConstants.CARBON_PROPERTIES_FILE_PATH);
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
  public CarbonProperties addProperty(String key, String value) {
    carbonProperties.setProperty(key, value);
    return this;
  }

  private ColumnarFormatVersion getDefaultFormatVersion() {
    return ColumnarFormatVersion.valueOf(CarbonCommonConstants.CARBON_DATA_FILE_DEFAULT_VERSION);
  }

  public ColumnarFormatVersion getFormatVersion() {
    String versionStr = getInstance().getProperty(CarbonCommonConstants.CARBON_DATA_FILE_VERSION);
    if (versionStr == null) {
      return getDefaultFormatVersion();
    } else {
      try {
        short version = Short.parseShort(versionStr);
        return ColumnarFormatVersion.valueOf(version);
      } catch (IllegalArgumentException e) {
        return getDefaultFormatVersion();
      }
    }
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

  /**
   * gettting the unmerged segment numbers to be merged.
   *
   * @return
   */
  public int[] getCompactionSegmentLevelCount() {
    String commaSeparatedLevels;

    commaSeparatedLevels = getProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD,
        CarbonCommonConstants.DEFAULT_SEGMENT_LEVEL_THRESHOLD);
    int[] compactionSize = getIntArray(commaSeparatedLevels);

    if (null == compactionSize) {
      compactionSize = getIntArray(CarbonCommonConstants.DEFAULT_SEGMENT_LEVEL_THRESHOLD);
    }

    return compactionSize;
  }

  /**
   * @param commaSeparatedLevels
   * @return
   */
  private int[] getIntArray(String commaSeparatedLevels) {
    String[] levels = commaSeparatedLevels.split(",");
    int[] compactionSize = new int[levels.length];
    int i = 0;
    for (String levelSize : levels) {
      try {
        int size = Integer.parseInt(levelSize.trim());
        if (validate(size, 100, 0, -1) < 0) {
          // if given size is out of boundary then take default value for all levels.
          return null;
        }
        compactionSize[i++] = size;
      } catch (NumberFormatException e) {
        LOGGER.error(
            "Given value for property" + CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD
                + " is not proper. Taking the default value "
                + CarbonCommonConstants.DEFAULT_SEGMENT_LEVEL_THRESHOLD);
        return null;
      }
    }
    return compactionSize;
  }

  /**
   * Number of cores should be used while loading data.
   *
   * @return
   */
  public int getNumberOfCores() {
    int numberOfCores;
    try {
      numberOfCores = Integer.parseInt(CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.NUM_CORES_LOADING,
              CarbonCommonConstants.NUM_CORES_DEFAULT_VAL));
    } catch (NumberFormatException exc) {
      numberOfCores = Integer.parseInt(CarbonCommonConstants.NUM_CORES_DEFAULT_VAL);
    }
    return numberOfCores;
  }

  /**
   * Get the sort chunk memory size
   * @return
   */
  public int getSortMemoryChunkSizeInMB() {
    int inMemoryChunkSizeInMB;
    try {
      inMemoryChunkSizeInMB = Integer.parseInt(CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.OFFHEAP_SORT_CHUNK_SIZE_IN_MB,
              CarbonCommonConstants.OFFHEAP_SORT_CHUNK_SIZE_IN_MB_DEFAULT));
    } catch (Exception e) {
      inMemoryChunkSizeInMB =
          Integer.parseInt(CarbonCommonConstants.OFFHEAP_SORT_CHUNK_SIZE_IN_MB_DEFAULT);
      LOGGER.error("Problem in parsing the sort memory chunk size, setting with default value"
          + inMemoryChunkSizeInMB);
    }
    if (inMemoryChunkSizeInMB > 1024) {
      inMemoryChunkSizeInMB = 1024;
      LOGGER.error(
          "It is not recommended to increase the sort memory chunk size more than 1024MB, "
              + "so setting the value to "
              + inMemoryChunkSizeInMB);
    } else if (inMemoryChunkSizeInMB < 1) {
      inMemoryChunkSizeInMB = 1;
      LOGGER.error(
          "It is not recommended to decrease the sort memory chunk size less than 1MB, "
              + "so setting the value to "
              + inMemoryChunkSizeInMB);
    }
    return inMemoryChunkSizeInMB;
  }

  /**
   * Batch size of rows while sending data from one step to another in data loading.
   *
   * @return
   */
  public int getBatchSize() {
    int batchSize;
    try {
      batchSize = Integer.parseInt(CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.DATA_LOAD_BATCH_SIZE,
              CarbonCommonConstants.DATA_LOAD_BATCH_SIZE_DEFAULT));
    } catch (NumberFormatException exc) {
      batchSize = Integer.parseInt(CarbonCommonConstants.DATA_LOAD_BATCH_SIZE_DEFAULT);
    }
    return batchSize;
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
  public int validate(int actual, int max, int min, int defaultVal) {
    if (actual <= max && actual >= min) {
      return actual;
    }
    return defaultVal;
  }

  /**
   * This method will validate and set the value for executor start up waiting time out
   */
  private void validateExecutorStartUpTime() {
    int executorStartUpTimeOut = 0;
    try {
      executorStartUpTimeOut = Integer.parseInt(carbonProperties
          .getProperty(CarbonCommonConstants.CARBON_EXECUTOR_STARTUP_TIMEOUT,
              CarbonCommonConstants.CARBON_EXECUTOR_WAITING_TIMEOUT_DEFAULT));
      // If value configured by user is more than max value of time out then consider the max value
      if (executorStartUpTimeOut > CarbonCommonConstants.CARBON_EXECUTOR_WAITING_TIMEOUT_MAX) {
        executorStartUpTimeOut = CarbonCommonConstants.CARBON_EXECUTOR_WAITING_TIMEOUT_MAX;
      }
    } catch (NumberFormatException ne) {
      executorStartUpTimeOut =
          Integer.parseInt(CarbonCommonConstants.CARBON_EXECUTOR_WAITING_TIMEOUT_DEFAULT);
    }
    carbonProperties.setProperty(CarbonCommonConstants.CARBON_EXECUTOR_STARTUP_TIMEOUT,
        String.valueOf(executorStartUpTimeOut));
    LOGGER.info("Executor start up wait time: " + executorStartUpTimeOut);
  }

  /**
   * Returns configured update deleta files value for IUD compaction
   *
   * @return numberOfDeltaFilesThreshold
   */
  public int getNoUpdateDeltaFilesThresholdForIUDCompaction() {
    int numberOfDeltaFilesThreshold;
    try {
      numberOfDeltaFilesThreshold = Integer.parseInt(
          getProperty(CarbonCommonConstants.UPDATE_DELTAFILE_COUNT_THRESHOLD_IUD_COMPACTION,
              CarbonCommonConstants.DEFAULT_UPDATE_DELTAFILE_COUNT_THRESHOLD_IUD_COMPACTION));

      if (numberOfDeltaFilesThreshold < 0 || numberOfDeltaFilesThreshold > 10000) {
        LOGGER.error("The specified value for property "
            + CarbonCommonConstants.UPDATE_DELTAFILE_COUNT_THRESHOLD_IUD_COMPACTION
            + "is incorrect."
            + " Correct value should be in range of 0 -10000. Taking the default value.");
        numberOfDeltaFilesThreshold = Integer.parseInt(
            CarbonCommonConstants.DEFAULT_UPDATE_DELTAFILE_COUNT_THRESHOLD_IUD_COMPACTION);
      }
    } catch (NumberFormatException e) {
      LOGGER.error("The specified value for property "
          + CarbonCommonConstants.UPDATE_DELTAFILE_COUNT_THRESHOLD_IUD_COMPACTION + "is incorrect."
          + " Correct value should be in range of 0 -10000. Taking the default value.");
      numberOfDeltaFilesThreshold = Integer
          .parseInt(CarbonCommonConstants.DEFAULT_UPDATE_DELTAFILE_COUNT_THRESHOLD_IUD_COMPACTION);
    }
    return numberOfDeltaFilesThreshold;
  }

  /**
   * Returns configured delete deleta files value for IUD compaction
   *
   * @return numberOfDeltaFilesThreshold
   */
  public int getNoDeleteDeltaFilesThresholdForIUDCompaction() {
    int numberOfDeltaFilesThreshold;
    try {
      numberOfDeltaFilesThreshold = Integer.parseInt(
          getProperty(CarbonCommonConstants.DELETE_DELTAFILE_COUNT_THRESHOLD_IUD_COMPACTION,
              CarbonCommonConstants.DEFAULT_DELETE_DELTAFILE_COUNT_THRESHOLD_IUD_COMPACTION));

      if (numberOfDeltaFilesThreshold < 0 || numberOfDeltaFilesThreshold > 10000) {
        LOGGER.error("The specified value for property "
            + CarbonCommonConstants.DELETE_DELTAFILE_COUNT_THRESHOLD_IUD_COMPACTION
            + "is incorrect."
            + " Correct value should be in range of 0 -10000. Taking the default value.");
        numberOfDeltaFilesThreshold = Integer.parseInt(
            CarbonCommonConstants.DEFAULT_DELETE_DELTAFILE_COUNT_THRESHOLD_IUD_COMPACTION);
      }
    } catch (NumberFormatException e) {
      LOGGER.error("The specified value for property "
          + CarbonCommonConstants.DELETE_DELTAFILE_COUNT_THRESHOLD_IUD_COMPACTION + "is incorrect."
          + " Correct value should be in range of 0 -10000. Taking the default value.");
      numberOfDeltaFilesThreshold = Integer
          .parseInt(CarbonCommonConstants.DEFAULT_DELETE_DELTAFILE_COUNT_THRESHOLD_IUD_COMPACTION);
    }
    return numberOfDeltaFilesThreshold;
  }
}
