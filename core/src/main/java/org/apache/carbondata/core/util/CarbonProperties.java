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
import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.constants.CarbonLoadOptionConstants;
import org.apache.carbondata.core.constants.CarbonV3DataFormatConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.util.annotations.CarbonProperty;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import static org.apache.carbondata.core.constants.CarbonCommonConstants.BLOCKLET_SIZE;
import static org.apache.carbondata.core.constants.CarbonCommonConstants.CARBON_CUSTOM_BLOCK_DISTRIBUTION;
import static org.apache.carbondata.core.constants.CarbonCommonConstants.CARBON_DATA_FILE_VERSION;
import static org.apache.carbondata.core.constants.CarbonCommonConstants.CARBON_DATE_FORMAT;
import static org.apache.carbondata.core.constants.CarbonCommonConstants.CARBON_DYNAMIC_ALLOCATION_SCHEDULER_TIMEOUT;
import static org.apache.carbondata.core.constants.CarbonCommonConstants.CARBON_MINMAX_ALLOWED_BYTE_COUNT;
import static org.apache.carbondata.core.constants.CarbonCommonConstants.CARBON_PREFETCH_BUFFERSIZE;
import static org.apache.carbondata.core.constants.CarbonCommonConstants.CARBON_SCHEDULER_MIN_REGISTERED_RESOURCES_RATIO;
import static org.apache.carbondata.core.constants.CarbonCommonConstants.CARBON_SCHEDULER_MIN_REGISTERED_RESOURCES_RATIO_DEFAULT;
import static org.apache.carbondata.core.constants.CarbonCommonConstants.CARBON_SCHEDULER_MIN_REGISTERED_RESOURCES_RATIO_MAX;
import static org.apache.carbondata.core.constants.CarbonCommonConstants.CARBON_SCHEDULER_MIN_REGISTERED_RESOURCES_RATIO_MIN;
import static org.apache.carbondata.core.constants.CarbonCommonConstants.CARBON_SORT_FILE_WRITE_BUFFER_SIZE;
import static org.apache.carbondata.core.constants.CarbonCommonConstants.CARBON_TASK_DISTRIBUTION;
import static org.apache.carbondata.core.constants.CarbonCommonConstants.CARBON_TASK_DISTRIBUTION_BLOCK;
import static org.apache.carbondata.core.constants.CarbonCommonConstants.CARBON_TASK_DISTRIBUTION_BLOCKLET;
import static org.apache.carbondata.core.constants.CarbonCommonConstants.CARBON_TASK_DISTRIBUTION_CUSTOM;
import static org.apache.carbondata.core.constants.CarbonCommonConstants.CARBON_TASK_DISTRIBUTION_MERGE_FILES;
import static org.apache.carbondata.core.constants.CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT;
import static org.apache.carbondata.core.constants.CarbonCommonConstants.CSV_READ_BUFFER_SIZE;
import static org.apache.carbondata.core.constants.CarbonCommonConstants.DETAIL_QUERY_BATCH_SIZE;
import static org.apache.carbondata.core.constants.CarbonCommonConstants.DETAIL_QUERY_BATCH_SIZE_DEFAULT;
import static org.apache.carbondata.core.constants.CarbonCommonConstants.DETAIL_QUERY_BATCH_SIZE_MAX;
import static org.apache.carbondata.core.constants.CarbonCommonConstants.DETAIL_QUERY_BATCH_SIZE_MIN;
import static org.apache.carbondata.core.constants.CarbonCommonConstants.ENABLE_AUTO_HANDOFF;
import static org.apache.carbondata.core.constants.CarbonCommonConstants.ENABLE_OFFHEAP_SORT;
import static org.apache.carbondata.core.constants.CarbonCommonConstants.ENABLE_UNSAFE_SORT;
import static org.apache.carbondata.core.constants.CarbonCommonConstants.ENABLE_VECTOR_READER;
import static org.apache.carbondata.core.constants.CarbonCommonConstants.HANDOFF_SIZE;
import static org.apache.carbondata.core.constants.CarbonCommonConstants.LOCK_TYPE;
import static org.apache.carbondata.core.constants.CarbonCommonConstants.SORT_INTERMEDIATE_FILES_LIMIT;
import static org.apache.carbondata.core.constants.CarbonCommonConstants.SORT_SIZE;
import static org.apache.carbondata.core.constants.CarbonLoadOptionConstants.CARBON_LOAD_SORT_MEMORY_SPILL_PERCENTAGE;
import static org.apache.carbondata.core.constants.CarbonV3DataFormatConstants.BLOCKLET_SIZE_IN_MB;
import static org.apache.carbondata.core.constants.CarbonV3DataFormatConstants.NUMBER_OF_COLUMN_TO_READ_IN_IO;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

public final class CarbonProperties {
  /**
   * Attribute for Carbon LOGGER.
   */
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(CarbonProperties.class.getName());

  /**
   * class instance.
   */
  private static final CarbonProperties INSTANCE = new CarbonProperties();

  /**
   * Properties
   */
  private Properties carbonProperties;

  private Set<String> propertySet = new HashSet<String>();

  /**
   * It is purely for testing
   */
  private Map<String, String> addedProperty = new ConcurrentHashMap<>();

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
    return INSTANCE;
  }

  public Properties getAllPropertiesInstance() {
    return carbonProperties;
  }

  /**
   * This method is to validate only a specific key added to carbonProperties using addProperty
   *
   * @param key
   */
  private void validateAndLoadDefaultProperties(String key) {
    switch (key) {
      case BLOCKLET_SIZE:
        validateBlockletSize();
        break;
      case SORT_SIZE:
        validateSortSize();
        break;
      case CARBON_DATA_FILE_VERSION:
        validateCarbonDataFileVersion();
        break;
      case CARBON_DYNAMIC_ALLOCATION_SCHEDULER_TIMEOUT:
        validateDynamicSchedulerTimeOut();
        break;
      case CARBON_PREFETCH_BUFFERSIZE:
        validatePrefetchBufferSize();
        break;
      case BLOCKLET_SIZE_IN_MB:
        validateBlockletGroupSizeInMB();
        break;
      case NUMBER_OF_COLUMN_TO_READ_IN_IO:
        validateNumberOfColumnPerIORead();
        break;
      case ENABLE_UNSAFE_SORT:
        validateEnableUnsafeSort();
        break;
      case ENABLE_OFFHEAP_SORT:
        validateEnableOffHeapSort();
        break;
      case CARBON_CUSTOM_BLOCK_DISTRIBUTION:
        validateCustomBlockDistribution();
        break;
      case ENABLE_VECTOR_READER:
        validateEnableVectorReader();
        break;
      case CSV_READ_BUFFER_SIZE:
        validateCarbonCSVReadBufferSizeByte();
        break;
      case HANDOFF_SIZE:
      case ENABLE_AUTO_HANDOFF:
        validateHandoffSize();
        break;
      case CARBON_TASK_DISTRIBUTION:
        validateCarbonTaskDistribution();
        break;
      // The method validate the validity of configured carbon.timestamp.format value
      // and reset to default value if validation fail
      case CARBON_TIMESTAMP_FORMAT:
        validateTimeFormatKey(CARBON_TIMESTAMP_FORMAT,
            CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT);
        break;
      // The method validate the validity of configured carbon.date.format value
      // and reset to default value if validation fail
      case CARBON_DATE_FORMAT:
        validateTimeFormatKey(CARBON_DATE_FORMAT, CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT);
        break;
      case CARBON_SORT_FILE_WRITE_BUFFER_SIZE:
        validateSortFileWriteBufferSize();
        break;
      case SORT_INTERMEDIATE_FILES_LIMIT:
        validateSortIntermediateFilesLimit();
        break;
      case CARBON_SCHEDULER_MIN_REGISTERED_RESOURCES_RATIO:
        validateSchedulerMinRegisteredRatio();
        break;
      case CARBON_LOAD_SORT_MEMORY_SPILL_PERCENTAGE:
        validateSortMemorySpillPercentage();
        break;
      case CARBON_MINMAX_ALLOWED_BYTE_COUNT:
        validateStringCharacterLimit();
        break;
      case DETAIL_QUERY_BATCH_SIZE:
        validateDetailQueryBatchSize();
        break;
      case CarbonCommonConstants.CARBON_INDEX_SERVER_SERIALIZATION_THRESHOLD:
        validateIndexServerSerializationThreshold();
        break;
      case CarbonCommonConstants.CARBON_LOCAL_DICTIONARY_SIZE_THRESHOLD_IN_MB:
        validateAndGetLocalDictionarySizeThresholdInMB();
        break;
      case CarbonCommonConstants.CARBON_INDEX_SCHEMA_STORAGE:
        validateDMSchemaStorageProvider();
        break;
      case CarbonCommonConstants.CARBON_TRASH_RETENTION_DAYS:
        validateTrashFolderRetentionTime();
        break;
      // TODO : Validation for carbon.lock.type should be handled for addProperty flow
      default:
        // none
    }
  }

  /**
   * Validate the specified property is positive integer value
   */
  private void validatePositiveInteger(String propertyName) {
    String value = getInstance().getProperty(propertyName);
    try {
      int intValue = Integer.parseInt(value);
      if (intValue <= 0) {
        getInstance().removeProperty(propertyName);
        LOGGER.warn(String.format("The value \"%s\" configured for key \"%s\" " +
            "is invalid. Ignoring it", value, propertyName));
        throw new IllegalArgumentException();
      }
    } catch (NumberFormatException e) {
      getInstance().removeProperty(propertyName);
      LOGGER.warn(String.format("The value \"%s\" configured for key \"%s\" " +
          "is invalid. Ignoring it", value, propertyName));
      throw e;
    }
  }

  /**
   * This method validates the loaded properties and loads default
   * values in case of wrong values.
   */
  private void validateAndLoadDefaultProperties() {
    validateBlockletSize();
    validateSortSize();
    validateCarbonDataFileVersion();
    validateDynamicSchedulerTimeOut();
    validatePrefetchBufferSize();
    validateBlockletGroupSizeInMB();
    validateNumberOfColumnPerIORead();
    validateEnableUnsafeSort();
    validateEnableOffHeapSort();
    validateEnableMV();
    validateCustomBlockDistribution();
    validateEnableVectorReader();
    validateLockType();
    validateCarbonCSVReadBufferSizeByte();
    validateHandoffSize();
    validateCarbonTaskDistribution();
    // The method validate the validity of configured carbon.timestamp.format value
    // and reset to default value if validation fail
    validateTimeFormatKey(CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT);
    // The method validate the validity of configured carbon.date.format value
    // and reset to default value if validation fail
    validateTimeFormatKey(CARBON_DATE_FORMAT,
        CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT);
    validateSortFileWriteBufferSize();
    validateSortIntermediateFilesLimit();
    validateEnableAutoHandoff();
    validateSchedulerMinRegisteredRatio();
    validateWorkingMemory();
    validateSortStorageMemory();
    validateEnableQueryStatistics();
    validateSortMemorySpillPercentage();
    validateStringCharacterLimit();
    validateDetailQueryBatchSize();
    validateIndexServerSerializationThreshold();
    validateAndGetLocalDictionarySizeThresholdInMB();
    validateTrashFolderRetentionTime();
  }

  /**
   * Sort intermediate file size validation and if not valid then reset to the default value
   */
  private void validateSortIntermediateFilesLimit() {
    validateRange(SORT_INTERMEDIATE_FILES_LIMIT,
        CarbonCommonConstants.SORT_INTERMEDIATE_FILES_LIMIT_DEFAULT_VALUE,
        CarbonCommonConstants.SORT_INTERMEDIATE_FILES_LIMIT_MIN,
        CarbonCommonConstants.SORT_INTERMEDIATE_FILES_LIMIT_MAX);
  }

  /**
   *
   * @param key
   * @param defaultValue default value for the given key
   * @param minValue Minimum value for the given key
   * @param maxValue Max value for the given key
   */
  private void validateRange(String key, String defaultValue, int minValue, int maxValue) {
    String fileBufferSize = carbonProperties
        .getProperty(key, defaultValue);
    if (null != fileBufferSize) {
      try {
        int bufferSize = Integer.parseInt(fileBufferSize);

        if (bufferSize < minValue
            || bufferSize > maxValue) {
          LOGGER.warn("The value \"" + fileBufferSize + "\" configured for key "
              + key
              + "\" is not in range. Valid range is (byte) \""
              + minValue + " to \""
              + maxValue +
              ". Using the default value \""
              + defaultValue);
          carbonProperties.setProperty(key,
              defaultValue);
        }
      } catch (NumberFormatException nfe) {
        LOGGER.warn("The value \"" + fileBufferSize + "\" configured for key "
            + key
            + "\" is invalid. Using the default value \""
            + defaultValue);
        carbonProperties.setProperty(key,
            defaultValue);
      }
    }
  }

  /**
   * validate carbon.sort.file.write.buffer.size and if not valid then reset to the default value
   */
  private void validateSortFileWriteBufferSize() {
    validateRange(CARBON_SORT_FILE_WRITE_BUFFER_SIZE,
        CarbonCommonConstants.CARBON_SORT_FILE_WRITE_BUFFER_SIZE_DEFAULT_VALUE,
        CarbonCommonConstants.CARBON_SORT_FILE_WRITE_BUFFER_SIZE_MIN,
        CarbonCommonConstants.CARBON_SORT_FILE_WRITE_BUFFER_SIZE_MAX);
  }

  /**
   * minimum required registered resource for starting block distribution
   */
  private void validateSchedulerMinRegisteredRatio() {
    String value = carbonProperties
        .getProperty(CARBON_SCHEDULER_MIN_REGISTERED_RESOURCES_RATIO,
            CARBON_SCHEDULER_MIN_REGISTERED_RESOURCES_RATIO_DEFAULT);
    try {
      double minRegisteredResourceRatio = java.lang.Double.parseDouble(value);
      if (minRegisteredResourceRatio < CARBON_SCHEDULER_MIN_REGISTERED_RESOURCES_RATIO_MIN
          || minRegisteredResourceRatio > CARBON_SCHEDULER_MIN_REGISTERED_RESOURCES_RATIO_MAX) {
        LOGGER.warn("The value \"" + value
            + "\" configured for key " + CARBON_SCHEDULER_MIN_REGISTERED_RESOURCES_RATIO
            + "\" is not in range. Valid range is (byte) \""
            + CARBON_SCHEDULER_MIN_REGISTERED_RESOURCES_RATIO_MIN + " to \""
            + CARBON_SCHEDULER_MIN_REGISTERED_RESOURCES_RATIO_MAX + ". Using the default value \""
            + CARBON_SCHEDULER_MIN_REGISTERED_RESOURCES_RATIO_DEFAULT);
        carbonProperties.setProperty(CARBON_SCHEDULER_MIN_REGISTERED_RESOURCES_RATIO,
            CARBON_SCHEDULER_MIN_REGISTERED_RESOURCES_RATIO_DEFAULT);
      }
    } catch (NumberFormatException e) {
      LOGGER.warn(String.format("The value \"%s\" configured for key  \"%s\" is invalid. " +
                      "Using the default value \"%s\"",
              value,
              CARBON_SCHEDULER_MIN_REGISTERED_RESOURCES_RATIO,
              CARBON_SCHEDULER_MIN_REGISTERED_RESOURCES_RATIO_DEFAULT));
      carbonProperties.setProperty(CARBON_SCHEDULER_MIN_REGISTERED_RESOURCES_RATIO,
          CARBON_SCHEDULER_MIN_REGISTERED_RESOURCES_RATIO_DEFAULT);
    }
  }

  /**
   * The method validate the validity of configured carbon.date.format value
   * and reset to default value if validation fail
   */
  private void validateTimeFormatKey(String key, String defaultValue) {
    String dateFormat = carbonProperties
        .getProperty(key, defaultValue);
    try {
      new SimpleDateFormat(dateFormat);
    } catch (Exception e) {
      LOGGER.warn(String.format("The value \"%s\" configured for key \"%s\" is invalid. " +
              "Using the default value \"%s\"", dateFormat, key, key));
      carbonProperties.setProperty(key, defaultValue);
    }
  }

  /**
   * The method value csv read buffer size and if not valid then reset to the default value
   */
  private void validateCarbonCSVReadBufferSizeByte() {
    validateRange(CSV_READ_BUFFER_SIZE,
        CarbonCommonConstants.CSV_READ_BUFFER_SIZE_DEFAULT,
        CarbonCommonConstants.CSV_READ_BUFFER_SIZE_MIN,
        CarbonCommonConstants.CSV_READ_BUFFER_SIZE_MAX);
  }

  private void validateLockType() {
    String lockTypeConfigured = carbonProperties
        .getProperty(LOCK_TYPE, CarbonCommonConstants.LOCK_TYPE_DEFAULT);
    if (lockTypeConfigured != null) {
      switch (lockTypeConfigured.toUpperCase()) {
        // if user is setting the lock type as CARBON_LOCK_TYPE_ZOOKEEPER then no need to validate
        // else validate based on the file system type for LOCAL file system lock will be
        // CARBON_LOCK_TYPE_LOCAL and for the distributed one CARBON_LOCK_TYPE_HDFS
        case CarbonCommonConstants.CARBON_LOCK_TYPE_ZOOKEEPER:
          break;
        case  CarbonCommonConstants.CARBON_LOCK_TYPE_CUSTOM:
          break;
        case CarbonCommonConstants.CARBON_LOCK_TYPE_LOCAL:
        case CarbonCommonConstants.CARBON_LOCK_TYPE_HDFS:
        default:
          validateAndConfigureLockType(lockTypeConfigured);
      }
    } else {
      validateAndConfigureLockType(null);
    }
  }

  /**
   * the method decide and set the lock type based on the configured system type
   *
   * @param lockTypeConfigured
   */
  private void validateAndConfigureLockType(String lockTypeConfigured) {
    String lockTypeByFS = null;
    Configuration configuration = FileFactory.getConfiguration();
    String defaultFs = configuration.get("fs.defaultFS");
    if (null != defaultFs && (defaultFs.startsWith(CarbonCommonConstants.HDFSURL_PREFIX)
        || defaultFs.startsWith(CarbonCommonConstants.VIEWFSURL_PREFIX) || defaultFs
        .startsWith(CarbonCommonConstants.ALLUXIOURL_PREFIX) || defaultFs
        .startsWith(CarbonCommonConstants.S3A_PREFIX))) {
      lockTypeByFS = CarbonCommonConstants.CARBON_LOCK_TYPE_HDFS;
    } else if (null != defaultFs && defaultFs.startsWith(CarbonCommonConstants.LOCAL_FILE_PREFIX)) {
      lockTypeByFS = CarbonCommonConstants.CARBON_LOCK_TYPE_LOCAL;
    }
    if (lockTypeByFS != null && lockTypeConfigured != null &&
        !lockTypeConfigured.equalsIgnoreCase(lockTypeByFS)) {
      LOGGER.warn("The value \"" + lockTypeConfigured + "\" configured for key "
          + LOCK_TYPE + " is invalid for current file system. "
          + "Use the default value " + lockTypeByFS + " instead.");
    }
    if (lockTypeByFS != null) {
      carbonProperties.setProperty(LOCK_TYPE, lockTypeByFS);
    } else {
      carbonProperties.setProperty(LOCK_TYPE, CarbonCommonConstants.CARBON_LOCK_TYPE_LOCAL);
    }
  }

  private void validateEnableVectorReader() {
    String vectorReaderStr =
        carbonProperties.getProperty(ENABLE_VECTOR_READER);
    if (vectorReaderStr == null) {
      carbonProperties.setProperty(CarbonCommonConstants.ENABLE_VECTOR_READER,
          CarbonCommonConstants.ENABLE_VECTOR_READER_DEFAULT);
      vectorReaderStr = carbonProperties.getProperty(CarbonCommonConstants.ENABLE_VECTOR_READER);
    }
    boolean isValidBooleanValue = CarbonUtil.validateBoolean(vectorReaderStr);
    if (!isValidBooleanValue) {
      LOGGER.warn(String.format("The enable vector reader value \"%s\" is invalid. " +
              "Using the default value \"%s\"",
          vectorReaderStr,
          CarbonCommonConstants.ENABLE_VECTOR_READER_DEFAULT));
      carbonProperties.setProperty(ENABLE_VECTOR_READER,
          CarbonCommonConstants.ENABLE_VECTOR_READER_DEFAULT);
    }
  }

  private void validateCustomBlockDistribution() {
    String customBlockDistributionStr =
        carbonProperties.getProperty(CARBON_CUSTOM_BLOCK_DISTRIBUTION);
    if (customBlockDistributionStr == null) {
      carbonProperties.setProperty(CarbonCommonConstants.CARBON_CUSTOM_BLOCK_DISTRIBUTION,
          CarbonCommonConstants.CARBON_CUSTOM_BLOCK_DISTRIBUTION_DEFAULT);
      customBlockDistributionStr =
          carbonProperties.getProperty(CarbonCommonConstants.CARBON_CUSTOM_BLOCK_DISTRIBUTION);
    }
    boolean isValidBooleanValue = CarbonUtil.validateBoolean(customBlockDistributionStr);
    if (!isValidBooleanValue) {
      LOGGER.warn(String.format("The custom block distribution value \"%s\" is invalid. "
          + "Using the default value \"false\"", customBlockDistributionStr));
      carbonProperties.setProperty(CARBON_CUSTOM_BLOCK_DISTRIBUTION, "false");
    }
  }

  private void validateCarbonTaskDistribution() {
    String carbonTaskDistribution = carbonProperties.getProperty(CARBON_TASK_DISTRIBUTION);
    if (carbonTaskDistribution == null) {
      carbonProperties.setProperty(CARBON_TASK_DISTRIBUTION,
          CarbonCommonConstants.CARBON_TASK_DISTRIBUTION_DEFAULT);
      carbonTaskDistribution = carbonProperties.getProperty(CARBON_TASK_DISTRIBUTION);
    }
    boolean isValid = carbonTaskDistribution.equalsIgnoreCase(CARBON_TASK_DISTRIBUTION_MERGE_FILES)
        || carbonTaskDistribution.equalsIgnoreCase(CARBON_TASK_DISTRIBUTION_BLOCKLET)
        || carbonTaskDistribution.equalsIgnoreCase(CARBON_TASK_DISTRIBUTION_BLOCK)
        || carbonTaskDistribution.equalsIgnoreCase(CARBON_TASK_DISTRIBUTION_CUSTOM);
    if (!isValid) {
      LOGGER.warn(String.format("The carbon task distribution value \"%s\" is invalid. " +
              "Using the default value \"%s\"",
          carbonTaskDistribution,
          CarbonCommonConstants.CARBON_TASK_DISTRIBUTION_DEFAULT));
      carbonProperties.setProperty(CARBON_TASK_DISTRIBUTION,
          CarbonCommonConstants.CARBON_TASK_DISTRIBUTION_DEFAULT);
    }
  }

  private void validateEnableMV() {
    String isMVEnabled = carbonProperties.getProperty(CarbonCommonConstants.CARBON_ENABLE_MV);
    if (!CarbonUtil.validateBoolean(isMVEnabled)) {
      LOGGER.warn(String.format("The enable mv value \"%s\" is invalid. " +
              "Using the default value \"%s\"",
              isMVEnabled,
          CarbonCommonConstants.CARBON_ENABLE_MV_DEFAULT
      ));
      carbonProperties.setProperty(CarbonCommonConstants.CARBON_ENABLE_MV,
          CarbonCommonConstants.CARBON_ENABLE_MV_DEFAULT);
    }
  }

  private void validateEnableUnsafeSort() {
    String unSafeSortStr = carbonProperties.getProperty(ENABLE_UNSAFE_SORT);
    if (unSafeSortStr == null) {
      carbonProperties.setProperty(CarbonCommonConstants.ENABLE_UNSAFE_SORT,
          CarbonCommonConstants.ENABLE_UNSAFE_SORT_DEFAULT);
      unSafeSortStr = carbonProperties.getProperty(CarbonCommonConstants.ENABLE_UNSAFE_SORT);
    }
    boolean isValidBooleanValue = CarbonUtil.validateBoolean(unSafeSortStr);
    if (!isValidBooleanValue) {
      LOGGER.warn(String.format("The enable unsafe sort value \"%s\" is invalid. " +
              "Using the default value \"%s\"",
          unSafeSortStr,
          CarbonCommonConstants.ENABLE_UNSAFE_SORT_DEFAULT
      ));
      carbonProperties.setProperty(ENABLE_UNSAFE_SORT,
          CarbonCommonConstants.ENABLE_UNSAFE_SORT_DEFAULT);
    }
  }

  private void validateEnableOffHeapSort() {
    String offHeapSortStr = carbonProperties.getProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT);
    if (offHeapSortStr == null) {
      carbonProperties.setProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT,
          CarbonCommonConstants.ENABLE_OFFHEAP_SORT_DEFAULT);
      offHeapSortStr = carbonProperties.getProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT);
    }
    boolean isValidBooleanValue = CarbonUtil.validateBoolean(offHeapSortStr);
    if (!isValidBooleanValue) {
      LOGGER.warn(String.format("The enable off heap sort value \"%s\" is invalid. " +
              "Using the default value \"%s\"",
          offHeapSortStr,
          CarbonCommonConstants.ENABLE_OFFHEAP_SORT_DEFAULT));
      carbonProperties.setProperty(ENABLE_OFFHEAP_SORT,
          CarbonCommonConstants.ENABLE_OFFHEAP_SORT_DEFAULT);
    }
  }

  private void initPropertySet() throws IllegalAccessException {
    Field[] declaredFields = CarbonCommonConstants.class.getDeclaredFields();
    for (Field field : declaredFields) {
      if (field.isAnnotationPresent(CarbonProperty.class)) {
        propertySet.add(field.get(field.getName()).toString());
      }
    }
    declaredFields = CarbonV3DataFormatConstants.class.getDeclaredFields();
    for (Field field : declaredFields) {
      if (field.isAnnotationPresent(CarbonProperty.class)) {
        propertySet.add(field.get(field.getName()).toString());
      }
    }
    declaredFields = CarbonLoadOptionConstants.class.getDeclaredFields();
    for (Field field : declaredFields) {
      if (field.isAnnotationPresent(CarbonProperty.class)) {
        propertySet.add(field.get(field.getName()).toString());
      }
    }
  }

  private void validatePrefetchBufferSize() {
    String prefetchBufferSizeStr =
        carbonProperties.getProperty(CARBON_PREFETCH_BUFFERSIZE);

    if (null == prefetchBufferSizeStr || prefetchBufferSizeStr.length() == 0) {
      carbonProperties.setProperty(CARBON_PREFETCH_BUFFERSIZE,
          CarbonCommonConstants.CARBON_PREFETCH_BUFFERSIZE_DEFAULT);
    } else {
      try {
        Integer.parseInt(prefetchBufferSizeStr);
      } catch (NumberFormatException e) {
        LOGGER.info("The prefetch buffer size value \"" + prefetchBufferSizeStr
            + "\" is invalid. Using the default value \""
            + CarbonCommonConstants.CARBON_PREFETCH_BUFFERSIZE_DEFAULT + "\"");
        carbonProperties.setProperty(CARBON_PREFETCH_BUFFERSIZE,
            CarbonCommonConstants.CARBON_PREFETCH_BUFFERSIZE_DEFAULT);
      }
    }
  }

  private void validateHandoffSize() {
    String handoffSizeStr = carbonProperties.getProperty(HANDOFF_SIZE);
    if (null == handoffSizeStr || handoffSizeStr.length() == 0) {
      carbonProperties.setProperty(HANDOFF_SIZE,
          "" + CarbonCommonConstants.HANDOFF_SIZE_DEFAULT);
    } else {
      try {
        long handoffSize = Long.parseLong(handoffSizeStr);
        if (handoffSize < CarbonCommonConstants.HANDOFF_SIZE_MIN) {
          LOGGER.info("The streaming segment max size configured value " + handoffSizeStr +
              " is invalid. Using the default value "
              + CarbonCommonConstants.HANDOFF_SIZE_DEFAULT);
          carbonProperties.setProperty(HANDOFF_SIZE,
              "" + CarbonCommonConstants.HANDOFF_SIZE_DEFAULT);
        }
      } catch (NumberFormatException e) {
        LOGGER.info("The streaming segment max size value \"" + handoffSizeStr
            + "\" is invalid. Using the default value \""
            + CarbonCommonConstants.HANDOFF_SIZE_DEFAULT + "\"");
        carbonProperties.setProperty(HANDOFF_SIZE,
            "" + CarbonCommonConstants.HANDOFF_SIZE_DEFAULT);
      }
    }
  }

  private void validateEnableAutoHandoff() {
    String autoHandoffString =
        carbonProperties.getProperty(CarbonCommonConstants.ENABLE_AUTO_HANDOFF);
    if (autoHandoffString == null) {
      carbonProperties.setProperty(CarbonCommonConstants.ENABLE_AUTO_HANDOFF,
          CarbonCommonConstants.ENABLE_AUTO_HANDOFF_DEFAULT);
      autoHandoffString = carbonProperties.getProperty(CarbonCommonConstants.ENABLE_AUTO_HANDOFF);
    }
    boolean isValidBooleanValue = CarbonUtil.validateBoolean(autoHandoffString);
    if (!isValidBooleanValue) {
      LOGGER.warn(String.format("The enable auto handoff value \"%s\" is invalid. " +
              "Using the default value \"%s\"",
          autoHandoffString,
          CarbonCommonConstants.ENABLE_AUTO_HANDOFF_DEFAULT));
      carbonProperties.setProperty(ENABLE_AUTO_HANDOFF,
          CarbonCommonConstants.ENABLE_AUTO_HANDOFF_DEFAULT);
    }
  }

  public boolean isIndexParallelLoadingEnabled(String databaseName, String tableName) {
    // Check for propertyKey.dbname.table name for session based set for a specific table.
    String loadIndexParallel = getSessionPropertyValue(
        CarbonCommonConstants.CARBON_LOAD_INDEXES_PARALLEL + databaseName + "." + tableName);
    // If table table property is not specified then check for session for all the tables
    // otherwise check in carbon.properties
    if (loadIndexParallel == null) {
      loadIndexParallel = getProperty(CarbonCommonConstants.CARBON_LOAD_INDEXES_PARALLEL,
          CarbonCommonConstants.CARBON_LOAD_INDEXES_PARALLEL_DEFAULT);
    }
    boolean configuredValue = Boolean.parseBoolean(loadIndexParallel);
    if (configuredValue) {
      LOGGER.info("Loading indexes in parallel for " + databaseName + "." + tableName);
    }
    return configuredValue;
  }


  /**
   * This method validates the number of pages per blocklet column
   */
  private void validateBlockletGroupSizeInMB() {
    String numberOfPagePerBlockletColumnString = carbonProperties
        .getProperty(BLOCKLET_SIZE_IN_MB,
            CarbonV3DataFormatConstants.BLOCKLET_SIZE_IN_MB_DEFAULT_VALUE);
    try {
      short numberOfPagePerBlockletColumn = Short.parseShort(numberOfPagePerBlockletColumnString);
      if (numberOfPagePerBlockletColumn < CarbonV3DataFormatConstants.BLOCKLET_SIZE_IN_MB_MIN) {
        LOGGER.info(String.format("Blocklet Size Configured value \"%s\" is invalid. " +
                        "Using the default value \"%s\"",
                numberOfPagePerBlockletColumnString,
                CarbonV3DataFormatConstants.BLOCKLET_SIZE_IN_MB_DEFAULT_VALUE));
        carbonProperties.setProperty(BLOCKLET_SIZE_IN_MB,
            CarbonV3DataFormatConstants.BLOCKLET_SIZE_IN_MB_DEFAULT_VALUE);
      }
    } catch (NumberFormatException e) {
      LOGGER.info(String.format("Blocklet Size Configured value \"%s\" is invalid. " +
                      "Using the default value \"%s\"",
              numberOfPagePerBlockletColumnString,
              CarbonV3DataFormatConstants.BLOCKLET_SIZE_IN_MB_DEFAULT_VALUE));
      carbonProperties.setProperty(BLOCKLET_SIZE_IN_MB,
          CarbonV3DataFormatConstants.BLOCKLET_SIZE_IN_MB_DEFAULT_VALUE);
    }
    LOGGER.info(String.format("Blocklet Size Configured value is \"%s\"", carbonProperties
        .getProperty(BLOCKLET_SIZE_IN_MB,
            CarbonV3DataFormatConstants.BLOCKLET_SIZE_IN_MB_DEFAULT_VALUE)));
  }

  /**
   * This method validates the number of column read in one IO
   */
  private void validateNumberOfColumnPerIORead() {
    String numberOfColumnPerIOString = carbonProperties
        .getProperty(NUMBER_OF_COLUMN_TO_READ_IN_IO,
            CarbonV3DataFormatConstants.NUMBER_OF_COLUMN_TO_READ_IN_IO_DEFAULT_VALUE);
    try {
      short numberOfColumnPerIO = Short.parseShort(numberOfColumnPerIOString);
      if (numberOfColumnPerIO < CarbonV3DataFormatConstants.NUMBER_OF_COLUMN_TO_READ_IN_IO_MIN
          || numberOfColumnPerIO > CarbonV3DataFormatConstants.NUMBER_OF_COLUMN_TO_READ_IN_IO_MAX) {
        LOGGER.info("The Number Of pages per blocklet column value \"" + numberOfColumnPerIOString
            + "\" is invalid. Using the default value \""
            + CarbonV3DataFormatConstants.NUMBER_OF_COLUMN_TO_READ_IN_IO_DEFAULT_VALUE);
        carbonProperties.setProperty(NUMBER_OF_COLUMN_TO_READ_IN_IO,
            CarbonV3DataFormatConstants.NUMBER_OF_COLUMN_TO_READ_IN_IO_DEFAULT_VALUE);
      }
    } catch (NumberFormatException e) {
      LOGGER.info("The Number Of pages per blocklet column value \"" + numberOfColumnPerIOString
          + "\" is invalid. Using the default value \""
          + CarbonV3DataFormatConstants.NUMBER_OF_COLUMN_TO_READ_IN_IO_DEFAULT_VALUE);
      carbonProperties.setProperty(NUMBER_OF_COLUMN_TO_READ_IN_IO,
          CarbonV3DataFormatConstants.NUMBER_OF_COLUMN_TO_READ_IN_IO_DEFAULT_VALUE);
    }
  }

  /**
   * This method validates the blocklet size
   */
  private void validateBlockletSize() {
    String blockletSizeStr = carbonProperties.getProperty(BLOCKLET_SIZE,
        CarbonCommonConstants.BLOCKLET_SIZE_DEFAULT_VAL);
    try {
      int blockletSize = Integer.parseInt(blockletSizeStr);

      if (blockletSize < CarbonCommonConstants.BLOCKLET_SIZE_MIN_VAL
          || blockletSize > CarbonCommonConstants.BLOCKLET_SIZE_MAX_VAL) {
        LOGGER.info("The blocklet size value \"" + blockletSizeStr
            + "\" is invalid. Using the default value \""
            + CarbonCommonConstants.BLOCKLET_SIZE_DEFAULT_VAL);
        carbonProperties.setProperty(BLOCKLET_SIZE,
            CarbonCommonConstants.BLOCKLET_SIZE_DEFAULT_VAL);
      }
    } catch (NumberFormatException e) {
      LOGGER.info("The blocklet size value \"" + blockletSizeStr
          + "\" is invalid. Using the default value \""
          + CarbonCommonConstants.BLOCKLET_SIZE_DEFAULT_VAL);
      carbonProperties.setProperty(BLOCKLET_SIZE,
          CarbonCommonConstants.BLOCKLET_SIZE_DEFAULT_VAL);
    }
  }

  /**
   * This method validates the index server serialization size
   */
  private void validateIndexServerSerializationThreshold() {
    String serializationSizeString = carbonProperties
        .getProperty(CarbonCommonConstants.CARBON_INDEX_SERVER_SERIALIZATION_THRESHOLD,
            CarbonCommonConstants.CARBON_INDEX_SERVER_SERIALIZATION_THRESHOLD_DEFAULT);
    try {
      int serializationSize = Integer.parseInt(serializationSizeString);

      if (serializationSize < CarbonCommonConstants.CARBON_INDEX_SERVER_SERIALIZATION_THRESHOLD_MIN
          || serializationSize
          > CarbonCommonConstants.CARBON_INDEX_SERVER_SERIALIZATION_THRESHOLD_MAX) {
        LOGGER.info(
            "The " + CarbonCommonConstants.CARBON_INDEX_SERVER_SERIALIZATION_THRESHOLD + " value \""
                + serializationSize + "\" is invalid. Using the default value \""
                + CarbonCommonConstants.CARBON_INDEX_SERVER_SERIALIZATION_THRESHOLD_DEFAULT);
        carbonProperties
            .setProperty(CarbonCommonConstants.CARBON_INDEX_SERVER_SERIALIZATION_THRESHOLD,
                CarbonCommonConstants.CARBON_INDEX_SERVER_SERIALIZATION_THRESHOLD_DEFAULT);
      }
    } catch (NumberFormatException e) {
      LOGGER.info(
          "The " + CarbonCommonConstants.CARBON_INDEX_SERVER_SERIALIZATION_THRESHOLD + " value \""
              + serializationSizeString + "\" is invalid. Using the default value \""
              + CarbonCommonConstants.CARBON_INDEX_SERVER_SERIALIZATION_THRESHOLD_DEFAULT);
      carbonProperties
          .setProperty(CarbonCommonConstants.CARBON_INDEX_SERVER_SERIALIZATION_THRESHOLD,
              CarbonCommonConstants.CARBON_INDEX_SERVER_SERIALIZATION_THRESHOLD_DEFAULT);
    }
  }

  /**
   * This method validates the sort size
   */
  private void validateSortSize() {
    String sortSizeStr = carbonProperties
        .getProperty(SORT_SIZE, CarbonCommonConstants.SORT_SIZE_DEFAULT_VAL);
    try {
      int sortSize = Integer.parseInt(sortSizeStr);

      if (sortSize < CarbonCommonConstants.SORT_SIZE_MIN_VAL) {
        LOGGER.info(
            String.format("The batch size value \"%s\" is invalid. " +
                            "Using the default value \"%s\"",
                    sortSizeStr,
                    CarbonCommonConstants.SORT_SIZE_DEFAULT_VAL));
        carbonProperties.setProperty(SORT_SIZE,
            CarbonCommonConstants.SORT_SIZE_DEFAULT_VAL);
      }
    } catch (NumberFormatException e) {
      LOGGER.info(
          String.format("The batch size value \"%s\" is invalid. " +
                          "Using the default value \"%s\"",
                  sortSizeStr,
                  CarbonCommonConstants.SORT_SIZE_DEFAULT_VAL));
      carbonProperties.setProperty(SORT_SIZE,
          CarbonCommonConstants.SORT_SIZE_DEFAULT_VAL);
    }
  }

  /**
   * Below method will be used to validate the data file version parameter
   * if parameter is invalid current version will be set
   */
  private void validateCarbonDataFileVersion() {
    String carbondataFileVersionString =
        carbonProperties.getProperty(CARBON_DATA_FILE_VERSION);
    if (carbondataFileVersionString == null) {
      // use default property if user does not specify version property
      carbonProperties.setProperty(CARBON_DATA_FILE_VERSION,
          CarbonCommonConstants.CARBON_DATA_FILE_DEFAULT_VERSION);
    } else {
      try {
        carbonProperties.setProperty(CARBON_DATA_FILE_VERSION,
            ColumnarFormatVersion.valueOf(carbondataFileVersionString).name());
      } catch (IllegalArgumentException e) {
        // use default property if user specifies an invalid version property
        LOGGER.warn("Specified file version property is invalid: " + carbondataFileVersionString
            + ". Using " + CarbonCommonConstants.CARBON_DATA_FILE_DEFAULT_VERSION
            + " as default file version");
        carbonProperties.setProperty(CARBON_DATA_FILE_VERSION,
            CarbonCommonConstants.CARBON_DATA_FILE_DEFAULT_VERSION);
      }
    }
    LOGGER.info(
        "Considered file format is: " + carbonProperties.getProperty(CARBON_DATA_FILE_VERSION));
  }

  /**
   * This method will read all the properties from file and load it into
   * memory
   */
  private void loadProperties() {
    String propertyPath = System.getProperty(CarbonCommonConstants.CARBON_PROPERTIES_FILE_PATH,
            CarbonCommonConstants.CARBON_PROPERTIES_FILE_PATH_DEFAULT);

    File propertyFile = new File(propertyPath);
    LOGGER.info("Property file path: " + propertyFile.getAbsolutePath());

    FileInputStream fis = null;
    try {
      if (propertyFile.exists()) {
        fis = new FileInputStream(propertyFile);

        carbonProperties.load(fis);
      }
    } catch (FileNotFoundException e) {
      LOGGER.error("The file: " + propertyFile.getAbsolutePath() + " does not exist");
    } catch (IOException e) {
      LOGGER.error("Error while reading the file: " + propertyFile.getAbsolutePath());
    } finally {
      if (null != fis) {
        try {
          fis.close();
        } catch (IOException e) {
          LOGGER.error("Error while closing the file stream for file: "
              + propertyFile.getAbsolutePath());
        }
      }
    }

    print();
    try {
      initPropertySet();
    } catch (IllegalAccessException e) {
      LOGGER.error("Illegal access to declared field" + e.getMessage(), e);
    }
  }

  /**
   * Return the store path
   */
  public static String getStorePath() {
    String storePath = getInstance().getProperty(CarbonCommonConstants.STORE_LOCATION);
    if (storePath == null) {
      // Internally spark sets the value of spark.warehouse.dir to hive.metastore.warehouse.dir.
      // So no need to check for spark property.
      storePath = FileFactory.getConfiguration().get("hive.metastore.warehouse.dir");
    }
    return storePath;
  }

  /**
   * This method will be used to get the properties value
   *
   * @param key property key
   * @return properties value
   */
  public String getProperty(String key) {
    // get the property value from session parameters,
    // if its null then get value from carbonProperties
    String sessionPropertyValue = getSessionPropertyValue(key);
    if (null != sessionPropertyValue) {
      return sessionPropertyValue;
    }
    return carbonProperties.getProperty(key);
  }

  /**
   * returns session property value
   *
   * @param key
   * @return
   */
  private String getSessionPropertyValue(String key) {
    String value = null;
    CarbonSessionInfo carbonSessionInfo = ThreadLocalSessionInfo.getCarbonSessionInfo();
    if (null != carbonSessionInfo) {
      SessionParams sessionParams =
          ThreadLocalSessionInfo.getCarbonSessionInfo().getSessionParams();
      if (null != sessionParams) {
        value = sessionParams.getProperty(key);
      }
    }
    return value;
  }

  /**
   * This method will be used to get the properties value if property is not
   * present then it will return the default value
   *
   * @param key property key
   * @param defaultValue properties default value
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
   * @param key property key
   * @param value properties value
   * @return CarbonProperties object
   */
  public CarbonProperties addProperty(String key, String value) {
    carbonProperties.setProperty(key, value);
    addedProperty.put(key, value);
    // the method will validate the added property
    // if the added property is not valid then will reset to default value.
    validateAndLoadDefaultProperties(key.toLowerCase());
    return this;
  }

  /**
   * This method will be used to add a new property which need not be serialized
   *
   * @param key
   */
  public void addNonSerializableProperty(String key, String value) {
    carbonProperties.setProperty(key, value);
  }

  /**
   * Remove the specified key in property
   */
  public CarbonProperties removeProperty(String key) {
    carbonProperties.remove(key);
    addedProperty.remove(key);
    return this;
  }

  private ColumnarFormatVersion getDefaultFormatVersion() {
    return ColumnarFormatVersion.valueOf(CarbonCommonConstants.CARBON_DATA_FILE_DEFAULT_VERSION);
  }

  public ColumnarFormatVersion getFormatVersion() {
    String versionStr = getInstance().getProperty(CARBON_DATA_FILE_VERSION);
    if (versionStr == null) {
      return getDefaultFormatVersion();
    } else {
      try {
        return ColumnarFormatVersion.valueOf(versionStr);
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
      compactionSize = Long.parseLong(getProperty(
              CarbonCommonConstants.CARBON_MAJOR_COMPACTION_SIZE,
              CarbonCommonConstants.DEFAULT_CARBON_MAJOR_COMPACTION_SIZE));
    } catch (NumberFormatException e) {
      compactionSize = Long.parseLong(
              CarbonCommonConstants.DEFAULT_CARBON_MAJOR_COMPACTION_SIZE);
    }
    return compactionSize;
  }

  /**
   * returns minor compaction size value from carbon properties or -1 if it is not valid or
   * not configured
   *
   * @return compactionSize
   */
  public long getMinorCompactionSize() {
    long compactionSize = -1;
    // if not configured, just use default -1
    if (null != getProperty(CarbonCommonConstants.CARBON_MINOR_COMPACTION_SIZE)) {
      try {
        compactionSize = Long.parseLong(getProperty(
                CarbonCommonConstants.CARBON_MINOR_COMPACTION_SIZE));
      } catch (NumberFormatException e) {
        LOGGER.warn("Invalid value is configured for property "
                + CarbonCommonConstants.CARBON_MINOR_COMPACTION_SIZE + ", considering the default"
                + " value -1 and not considering segment Size during minor compaction.");
      }
      if (compactionSize <= 0) {
        LOGGER.warn("Invalid value is configured for property "
                + CarbonCommonConstants.CARBON_MINOR_COMPACTION_SIZE + ", considering the default"
                + " value -1 and not considering segment Size during minor compaction.");
        compactionSize = -1;
      }
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
        LOGGER.warn("The specified value for property "
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
   * getting the unmerged segment numbers to be merged.
   *
   * @return corrected value of unmerged segments to be merged
   */
  public int[] getCompactionSegmentLevelCount() {
    String commaSeparatedLevels;

    commaSeparatedLevels = getProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD,
        CarbonCommonConstants.DEFAULT_SEGMENT_LEVEL_THRESHOLD);
    int[] compactionSize = getIntArray(commaSeparatedLevels);

    if (0 == compactionSize.length) {
      compactionSize = getIntArray(CarbonCommonConstants.DEFAULT_SEGMENT_LEVEL_THRESHOLD);
    }

    return compactionSize;
  }

  /**
   * Separating the count for Number of segments to be merged in levels by comma
   *
   * @param commaSeparatedLevels the string format value before separating
   * @return the int array format value after separating by comma
   */
  public int[] getIntArray(String commaSeparatedLevels) {
    String[] levels = commaSeparatedLevels.split(",");
    int[] compactionSize = new int[levels.length];
    int i = 0;
    for (String levelSize : levels) {
      try {
        int size = Integer.parseInt(levelSize.trim());
        if (validate(size,
            CarbonCommonConstants.NUMBER_OF_SEGMENT_COMPACTED_PERTIME_UPPER_LIMIT,
            CarbonCommonConstants.NUMBER_OF_SEGMENT_COMPACTED_PERTIME_LOWER_LIMIT,
            -1) < 0) {
          // if given size is out of boundary then take default value for all levels.
          LOGGER.warn(
              "Given value for property" + size
                  + " is not proper. Taking the default value "
                  + CarbonCommonConstants.DEFAULT_SEGMENT_LEVEL_THRESHOLD);
          return new int[0];
        }
        compactionSize[i++] = size;
      } catch (NumberFormatException e) {
        LOGGER.warn(
            "Given value for property" + CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD
                + " is not proper. Taking the default value "
                + CarbonCommonConstants.DEFAULT_SEGMENT_LEVEL_THRESHOLD);
        return new int[0];
      }
    }
    return compactionSize;
  }

  private int getNumberOfCores(String key) {
    int numberOfCores;
    try {
      numberOfCores = Integer.parseInt(
          CarbonProperties.getInstance().getProperty(
              key,
              CarbonCommonConstants.NUM_CORES_DEFAULT_VAL));
    } catch (NumberFormatException exc) {
      LOGGER.warn("Configured value for property " + key
          + " is wrong. Falling back to the default value "
          + CarbonCommonConstants.NUM_CORES_DEFAULT_VAL);
      numberOfCores = Integer.parseInt(CarbonCommonConstants.NUM_CORES_DEFAULT_VAL);
    }
    return numberOfCores;
  }

  /**
   * Number of cores should be used while loading data.
   * @return the number of cores to be used while loading data
   */
  public int getNumberOfLoadingCores() {
    return getNumberOfCores(CarbonCommonConstants.NUM_CORES_LOADING);
  }

  /**
   * Number of cores to be used while compacting.
   * @return the number of cores to be used while compacting
   */
  public int getNumberOfCompactingCores() {
    return getNumberOfCores(CarbonCommonConstants.NUM_CORES_COMPACTING);
  }

  /**
   * Number of cores to be used while alter partition.
   * @return the number of cores to be used while alter partition
   */
  public int getNumberOfAltPartitionCores() {
    return getNumberOfCores(CarbonCommonConstants.NUM_CORES_ALT_PARTITION);
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
      LOGGER.warn("Problem in parsing the sort memory chunk size, setting with default value"
          + inMemoryChunkSizeInMB);
    }
    if (inMemoryChunkSizeInMB > 1024) {
      inMemoryChunkSizeInMB = 1024;
      LOGGER.warn(
          "It is not recommended to increase the sort memory chunk size more than 1024MB, "
              + "so setting the value to "
              + inMemoryChunkSizeInMB);
    } else if (inMemoryChunkSizeInMB < 1) {
      inMemoryChunkSizeInMB = 1;
      LOGGER.warn(
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

  public static int getQueryBatchSize() {
    int batchSize;
    String batchSizeString =
        CarbonProperties.getInstance().getProperty(CarbonCommonConstants.DETAIL_QUERY_BATCH_SIZE);
    if (null != batchSizeString) {
      try {
        batchSize = Integer.parseInt(batchSizeString);
      } catch (NumberFormatException ne) {
        LOGGER.error("Invalid in-memory records size. Using default value");
        batchSize = CarbonCommonConstants.DETAIL_QUERY_BATCH_SIZE_DEFAULT;
      }
    } else {
      batchSize = CarbonCommonConstants.DETAIL_QUERY_BATCH_SIZE_DEFAULT;
    }
    return batchSize;
  }

  public long getHandoffSize() {
    long handoffSize;
    try {
      handoffSize = Long.parseLong(
          CarbonProperties.getInstance().getProperty(
              HANDOFF_SIZE,
              "" + CarbonCommonConstants.HANDOFF_SIZE_DEFAULT
          )
      );
    } catch (NumberFormatException exc) {
      handoffSize = CarbonCommonConstants.HANDOFF_SIZE_DEFAULT;
    }
    return handoffSize;
  }

  public boolean isEnableAutoHandoff() {
    String enableAutoHandoffStr = CarbonProperties.getInstance().getProperty(
        ENABLE_AUTO_HANDOFF,
        CarbonCommonConstants.ENABLE_AUTO_HANDOFF_DEFAULT);
    return enableAutoHandoffStr.equalsIgnoreCase("true");
  }

  public boolean isEnableVectorReader() {
    return getInstance().getProperty(CarbonCommonConstants.ENABLE_VECTOR_READER,
        CarbonCommonConstants.ENABLE_VECTOR_READER_DEFAULT).equalsIgnoreCase("true");
  }

  public static boolean isEnableTableStatusBackup() {
    return getInstance().getProperty(CarbonCommonConstants.ENABLE_TABLE_STATUS_BACKUP,
        CarbonCommonConstants.ENABLE_TABLE_STATUS_BACKUP_DEFAULT).equalsIgnoreCase("true");
  }

  /**
   * Validate the restrictions
   *
   * @param actual the actual value for minor compaction
   * @param max max value for minor compaction
   * @param min min value for minor compaction
   * @param defaultVal default value when the actual is improper
   * @return  corrected Value after validating
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
  private void validateDynamicSchedulerTimeOut() {
    validateRange(CARBON_DYNAMIC_ALLOCATION_SCHEDULER_TIMEOUT,
        CarbonCommonConstants.CARBON_DYNAMIC_ALLOCATION_SCHEDULER_TIMEOUT_DEFAULT,
        CarbonCommonConstants.CARBON_DYNAMIC_ALLOCATION_SCHEDULER_TIMEOUT_MIN,
        CarbonCommonConstants.CARBON_DYNAMIC_ALLOCATION_SCHEDULER_TIMEOUT_MAX);
  }

  /**
   * Returns configured delete delta files value for IUD compaction
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
        LOGGER.warn("The specified value for property "
            + CarbonCommonConstants.DELETE_DELTAFILE_COUNT_THRESHOLD_IUD_COMPACTION
            + "is incorrect."
            + " Correct value should be in range of 0 -10000. Taking the default value.");
        numberOfDeltaFilesThreshold = Integer.parseInt(
            CarbonCommonConstants.DEFAULT_DELETE_DELTAFILE_COUNT_THRESHOLD_IUD_COMPACTION);
      }
    } catch (NumberFormatException e) {
      LOGGER.warn("The specified value for property "
          + CarbonCommonConstants.DELETE_DELTAFILE_COUNT_THRESHOLD_IUD_COMPACTION + "is incorrect."
          + " Correct value should be in range of 0 -10000. Taking the default value.");
      numberOfDeltaFilesThreshold = Integer
          .parseInt(CarbonCommonConstants.DEFAULT_DELETE_DELTAFILE_COUNT_THRESHOLD_IUD_COMPACTION);
    }
    return numberOfDeltaFilesThreshold;
  }

  /**
   * Return valid storage level
   * @return String
   */
  public String getGlobalSortRddStorageLevel() {
    String storageLevel = getProperty(CarbonCommonConstants.CARBON_GLOBAL_SORT_RDD_STORAGE_LEVEL,
        CarbonCommonConstants.CARBON_GLOBAL_SORT_RDD_STORAGE_LEVEL_DEFAULT);
    boolean validateStorageLevel = CarbonUtil.isValidStorageLevel(storageLevel);
    if (!validateStorageLevel) {
      LOGGER.warn("The " + CarbonCommonConstants.CARBON_GLOBAL_SORT_RDD_STORAGE_LEVEL
          + " configuration value is invalid. It will use default storage level("
          + CarbonCommonConstants.CARBON_GLOBAL_SORT_RDD_STORAGE_LEVEL_DEFAULT
          + ") to persist rdd.");
      storageLevel = CarbonCommonConstants.CARBON_GLOBAL_SORT_RDD_STORAGE_LEVEL_DEFAULT;
    }
    return storageLevel.toUpperCase();
  }

  /**
   * Returns parallelism for segment update
   * @return int
   */
  public int getParallelismForSegmentUpdate() {
    int parallelism = Integer.parseInt(
        CarbonCommonConstants.CARBON_UPDATE_SEGMENT_PARALLELISM_DEFAULT);
    boolean isInvalidValue = false;
    try {
      String strParallelism = getProperty(CarbonCommonConstants.CARBON_UPDATE_SEGMENT_PARALLELISM,
          CarbonCommonConstants.CARBON_UPDATE_SEGMENT_PARALLELISM_DEFAULT);
      parallelism = Integer.parseInt(strParallelism);
      if (parallelism <= 0 || parallelism > 1000) {
        isInvalidValue = true;
      }
    } catch (NumberFormatException e) {
      isInvalidValue = true;
    }

    if (isInvalidValue) {
      LOGGER.warn("The specified value for property "
          + CarbonCommonConstants.CARBON_UPDATE_SEGMENT_PARALLELISM
          + " is incorrect. Correct value should be in range of 0 - 1000."
          + " Taking the default value: "
          + CarbonCommonConstants.CARBON_UPDATE_SEGMENT_PARALLELISM_DEFAULT);
      parallelism = Integer.parseInt(
          CarbonCommonConstants.CARBON_UPDATE_SEGMENT_PARALLELISM_DEFAULT);
    }

    return parallelism;
  }

  /**
   * Return valid CARBON_UPDATE_STORAGE_LEVEL
   * @return boolean
   */
  public boolean isPersistUpdateDataset() {
    String isPersistEnabled = getProperty(CarbonCommonConstants.CARBON_UPDATE_PERSIST_ENABLE,
            CarbonCommonConstants.CARBON_UPDATE_PERSIST_ENABLE_DEFAULT);
    boolean validatePersistEnabled = CarbonUtil.validateBoolean(isPersistEnabled);
    if (!validatePersistEnabled) {
      LOGGER.warn("The " + CarbonCommonConstants.CARBON_UPDATE_PERSIST_ENABLE
          + " configuration value is invalid. It will use default value("
          + CarbonCommonConstants.CARBON_UPDATE_PERSIST_ENABLE_DEFAULT
          + ").");
      isPersistEnabled = CarbonCommonConstants.CARBON_UPDATE_PERSIST_ENABLE_DEFAULT;
    }
    return isPersistEnabled.equalsIgnoreCase("true");
  }

  /**
   * Return valid storage level for CARBON_UPDATE_STORAGE_LEVEL
   * @return String
   */
  public String getUpdateDatasetStorageLevel() {
    String storageLevel = getProperty(CarbonCommonConstants.CARBON_UPDATE_STORAGE_LEVEL,
        CarbonCommonConstants.CARBON_UPDATE_STORAGE_LEVEL_DEFAULT);
    boolean validateStorageLevel = CarbonUtil.isValidStorageLevel(storageLevel);
    if (!validateStorageLevel) {
      LOGGER.warn("The " + CarbonCommonConstants.CARBON_UPDATE_STORAGE_LEVEL
          + " configuration value is invalid. It will use default storage level("
          + CarbonCommonConstants.CARBON_UPDATE_STORAGE_LEVEL_DEFAULT
          + ") to persist dataset.");
      storageLevel = CarbonCommonConstants.CARBON_UPDATE_STORAGE_LEVEL_DEFAULT;
    }
    return storageLevel.toUpperCase();
  }

  /**
   * get compressor name for compressing sort temp files
   * @return compressor name
   */
  public String getSortTempCompressor() {
    String compressor = getProperty(CarbonCommonConstants.CARBON_SORT_TEMP_COMPRESSOR,
        CarbonCommonConstants.CARBON_SORT_TEMP_COMPRESSOR_DEFAULT).toUpperCase();
    if (compressor.isEmpty() || "SNAPPY".equals(compressor) || "GZIP".equals(compressor)
        || "BZIP2".equals(compressor) || "LZ4".equals(compressor) || "ZSTD".equals(compressor)) {
      return compressor;
    } else {
      LOGGER.warn("The ".concat(CarbonCommonConstants.CARBON_SORT_TEMP_COMPRESSOR)
          .concat(" configuration value is invalid. Only snappy, gzip, bip2, lz4, zstd and")
          .concat(" empty are allowed. It will not compress the sort temp files by default"));
      return CarbonCommonConstants.CARBON_SORT_TEMP_COMPRESSOR_DEFAULT;
    }
  }

  /**
   * whether optimization for skewed data is enabled
   * @return true, if enabled; false for not enabled.
   */
  public boolean isLoadSkewedDataOptimizationEnabled() {
    String skewedEnabled = getProperty(
        CarbonLoadOptionConstants.ENABLE_CARBON_LOAD_SKEWED_DATA_OPTIMIZATION,
        CarbonLoadOptionConstants.ENABLE_CARBON_LOAD_SKEWED_DATA_OPTIMIZATION_DEFAULT);
    return skewedEnabled.equalsIgnoreCase("true");
  }

  /**
   * returns true if carbon property
   * @param key
   * @return
   */
  public boolean isCarbonProperty(String key) {
    return propertySet.contains(key);
  }

  public Map<String, String> getAddedProperty() {
    return addedProperty;
  }

  /**
   * to add external property
   *
   * @param externalPropertySet
   */
  public void addPropertyToPropertySet(Set<String> externalPropertySet) {
    propertySet.addAll(externalPropertySet);
  }

  private void validateWorkingMemory() {
    try {
      String unsafeWorkingMemoryStr =
          carbonProperties.getProperty(CarbonCommonConstants.UNSAFE_WORKING_MEMORY_IN_MB);
      if (unsafeWorkingMemoryStr == null) {
        return;
      }
      int unsafeWorkingMemory = Integer.parseInt(unsafeWorkingMemoryStr);
      carbonProperties
          .setProperty(CarbonCommonConstants.UNSAFE_WORKING_MEMORY_IN_MB, unsafeWorkingMemory + "");
    } catch (NumberFormatException e) {
      LOGGER.warn(
          "The specified value for property " + CarbonCommonConstants.UNSAFE_WORKING_MEMORY_IN_MB
              + " is invalid.");
    }
  }

  private void validateSortStorageMemory() {
    int unsafeSortStorageMemory;
    try {
      String unsafeSortStorageMemoryStr = carbonProperties
          .getProperty(CarbonCommonConstants.CARBON_SORT_STORAGE_INMEMORY_IN_MB);
      if (unsafeSortStorageMemoryStr != null) {
        unsafeSortStorageMemory = Integer.parseInt(unsafeSortStorageMemoryStr);
      } else {
        unsafeSortStorageMemory = CarbonCommonConstants.CARBON_SORT_STORAGE_INMEMORY_IN_MB_DEFAULT;
      }
    } catch (NumberFormatException e) {
      LOGGER.warn(String.format("The specified value for property %s is invalid."
              + " Taking the default value.%s",
          CarbonCommonConstants.CARBON_SORT_STORAGE_INMEMORY_IN_MB,
          CarbonCommonConstants.CARBON_SORT_STORAGE_INMEMORY_IN_MB_DEFAULT));
      unsafeSortStorageMemory = CarbonCommonConstants.CARBON_SORT_STORAGE_INMEMORY_IN_MB_DEFAULT;
    }
    if (unsafeSortStorageMemory
        < CarbonCommonConstants.CARBON_SORT_STORAGE_INMEMORY_IN_MB_DEFAULT) {
      LOGGER.warn("The specified value for property "
          + CarbonCommonConstants.CARBON_SORT_STORAGE_INMEMORY_IN_MB
          + " is less than the default value." + " Taking the default value: "
          + CarbonCommonConstants.CARBON_SORT_STORAGE_INMEMORY_IN_MB_DEFAULT + ".");
      unsafeSortStorageMemory = CarbonCommonConstants.CARBON_SORT_STORAGE_INMEMORY_IN_MB_DEFAULT;
    }
    carbonProperties.setProperty(CarbonCommonConstants.CARBON_SORT_STORAGE_INMEMORY_IN_MB,
        unsafeSortStorageMemory + "");
  }

  private void validateEnableQueryStatistics() {
    String enableQueryStatistics = carbonProperties.getProperty(
        CarbonCommonConstants.ENABLE_QUERY_STATISTICS,
        CarbonCommonConstants.ENABLE_QUERY_STATISTICS_DEFAULT);
    boolean isValidBooleanValue = CarbonUtil.validateBoolean(enableQueryStatistics);
    if (!isValidBooleanValue) {
      LOGGER.warn(String.format("The enable query statistics value \"%s\" is invalid. " +
                      "Using the default value \"%s\"",
              enableQueryStatistics,
              CarbonCommonConstants.ENABLE_QUERY_STATISTICS_DEFAULT));
      carbonProperties.setProperty(CarbonCommonConstants.ENABLE_QUERY_STATISTICS,
          CarbonCommonConstants.ENABLE_QUERY_STATISTICS_DEFAULT);
    }
  }

  public boolean isEnableQueryStatistics() {
    String enableQueryStatistics = carbonProperties.getProperty(
        CarbonCommonConstants.ENABLE_QUERY_STATISTICS,
        CarbonCommonConstants.ENABLE_QUERY_STATISTICS_DEFAULT);
    return enableQueryStatistics.equalsIgnoreCase("true");
  }

  /**
   * Get the heap memory pooling threshold bytes.
   */
  public int getHeapMemoryPoolingThresholdBytes() {
    int thresholdSize;
    try {
      thresholdSize = Integer.parseInt(CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.CARBON_HEAP_MEMORY_POOLING_THRESHOLD_BYTES,
              CarbonCommonConstants.CARBON_HEAP_MEMORY_POOLING_THRESHOLD_BYTES_DEFAULT));
    } catch (NumberFormatException exc) {
      LOGGER.warn(
          "The heap memory pooling threshold bytes is invalid. Using the default value "
          + CarbonCommonConstants.CARBON_HEAP_MEMORY_POOLING_THRESHOLD_BYTES_DEFAULT);
      thresholdSize = Integer.parseInt(
          CarbonCommonConstants.CARBON_HEAP_MEMORY_POOLING_THRESHOLD_BYTES_DEFAULT);
    }
    return thresholdSize;
  }

  public int getRangeColumnScaleFactor() {
    boolean isValid = true;
    int scaleFactor = 1;
    try {
      scaleFactor = Integer.parseInt(CarbonProperties.getInstance().getProperty(
          CarbonCommonConstants.CARBON_RANGE_COLUMN_SCALE_FACTOR,
          CarbonCommonConstants.CARBON_RANGE_COLUMN_SCALE_FACTOR_DEFAULT));
      if (scaleFactor < 1 || scaleFactor > 300) {
        isValid = false;
      }
    } catch (NumberFormatException ex) {
      LOGGER.warn("Range column scala factor isn't number format");
      isValid = false;
    }

    if (isValid) {
      return scaleFactor;
    } else {
      LOGGER.warn("The scale factor is invalid. Using the default value "
          + CarbonCommonConstants.CARBON_RANGE_COLUMN_SCALE_FACTOR_DEFAULT);
      return Integer.parseInt(CarbonCommonConstants.CARBON_RANGE_COLUMN_SCALE_FACTOR_DEFAULT);
    }
  }

  /**
   * Get the number of hours the segment lock files will be preserved.
   * It will be converted to microseconds to return.
   */
  public long getSegmentLockFilesPreserveHours() {
    long preserveSeconds;
    try {
      int preserveHours = Integer.parseInt(CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.CARBON_SEGMENT_LOCK_FILES_PRESERVE_HOURS,
              CarbonCommonConstants.CARBON_SEGMENT_LOCK_FILES_PRESERVE_HOURS_DEFAULT));
      preserveSeconds = preserveHours * 3600 * 1000L;
    } catch (NumberFormatException exc) {
      LOGGER.warn(
          "The value of '" + CarbonCommonConstants.CARBON_SEGMENT_LOCK_FILES_PRESERVE_HOURS
          + "' is invalid. Using the default value "
          + CarbonCommonConstants.CARBON_SEGMENT_LOCK_FILES_PRESERVE_HOURS_DEFAULT);
      preserveSeconds = Integer.parseInt(
          CarbonCommonConstants.CARBON_SEGMENT_LOCK_FILES_PRESERVE_HOURS_DEFAULT) * 3600 * 1000L;
    }
    return preserveSeconds;
  }

  /**
   * Get the number of invisible segment info which will be preserved in tablestatus file.
   */
  public int getInvisibleSegmentPreserveCount() {
    int preserveCnt;
    try {
      preserveCnt = Integer.parseInt(CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.CARBON_INVISIBLE_SEGMENTS_PRESERVE_COUNT,
              CarbonCommonConstants.CARBON_INVISIBLE_SEGMENTS_PRESERVE_COUNT_DEFAULT));
    } catch (NumberFormatException exc) {
      LOGGER.warn(
          "The value of '" + CarbonCommonConstants.CARBON_INVISIBLE_SEGMENTS_PRESERVE_COUNT
          + "' is invalid. Using the default value "
          + CarbonCommonConstants.CARBON_INVISIBLE_SEGMENTS_PRESERVE_COUNT_DEFAULT);
      preserveCnt = Integer.parseInt(
          CarbonCommonConstants.CARBON_INVISIBLE_SEGMENTS_PRESERVE_COUNT_DEFAULT);
    }
    return preserveCnt;
  }

  /**
   * Get the system folder location based on database location.
   */
  public String getSystemFolderLocationPerDatabase(String databaseLocation) {
    return databaseLocation + CarbonCommonConstants.FILE_SEPARATOR
        + CarbonTablePath.SYSTEM_FOLDER_DIR;
  }

  /**
   * Return valid storage level for CARBON_INSERT_STORAGE_LEVEL
   * @return String
   */
  public int getSortMemorySpillPercentage() {
    int spillPercentage = 0;
    try {
      String spillPercentageStr = getProperty(
          CARBON_LOAD_SORT_MEMORY_SPILL_PERCENTAGE,
          CarbonLoadOptionConstants.CARBON_LOAD_SORT_MEMORY_SPILL_PERCENTAGE_DEFAULT);
      spillPercentage = Integer.parseInt(spillPercentageStr);
    } catch (NumberFormatException e) {
      spillPercentage = Integer.parseInt(
          CarbonLoadOptionConstants.CARBON_LOAD_SORT_MEMORY_SPILL_PERCENTAGE_DEFAULT);
    }
    return spillPercentage;
  }

  public boolean isPushRowFiltersForVector() {
    String pushFilters = getProperty(CarbonCommonConstants.CARBON_PUSH_ROW_FILTERS_FOR_VECTOR,
            CarbonCommonConstants.CARBON_PUSH_ROW_FILTERS_FOR_VECTOR_DEFAULT);
    return Boolean.parseBoolean(pushFilters);
  }

  public boolean isRangeCompactionAllowed() {
    String isRangeCompact = getProperty(CarbonCommonConstants.CARBON_ENABLE_RANGE_COMPACTION,
        CarbonCommonConstants.CARBON_ENABLE_RANGE_COMPACTION_DEFAULT);
    return Boolean.parseBoolean(isRangeCompact);
  }

  private void validateSortMemorySpillPercentage() {
    String spillPercentageStr = carbonProperties.getProperty(
        CARBON_LOAD_SORT_MEMORY_SPILL_PERCENTAGE,
        CarbonLoadOptionConstants.CARBON_LOAD_SORT_MEMORY_SPILL_PERCENTAGE_DEFAULT);

    try {
      int spillPercentage = Integer.parseInt(spillPercentageStr);
      if (spillPercentage > 100 || spillPercentage < 0) {
        LOGGER.info(
            String.format("The sort memory spill percentage value \"%s\" is invalid. " +
                            "Using the default value \"%s\"",
                    spillPercentageStr,
                    CarbonLoadOptionConstants.CARBON_LOAD_SORT_MEMORY_SPILL_PERCENTAGE_DEFAULT));
        carbonProperties.setProperty(
            CARBON_LOAD_SORT_MEMORY_SPILL_PERCENTAGE,
            CarbonLoadOptionConstants.CARBON_LOAD_SORT_MEMORY_SPILL_PERCENTAGE_DEFAULT);
      }
    } catch (NumberFormatException e) {
      LOGGER.info(
          String.format("The sort memory spill percentage value \"%s\" is invalid. " +
                          "Using the default value \"%s\"",
                  spillPercentageStr,
                  CarbonLoadOptionConstants.CARBON_LOAD_SORT_MEMORY_SPILL_PERCENTAGE_DEFAULT));
      carbonProperties.setProperty(
          CARBON_LOAD_SORT_MEMORY_SPILL_PERCENTAGE,
          CarbonLoadOptionConstants.CARBON_LOAD_SORT_MEMORY_SPILL_PERCENTAGE_DEFAULT);
    }
  }

  /**
   * This method validates the allowed character limit for storing min/max for string type
   */
  private void validateStringCharacterLimit() {
    int allowedCharactersLimit = 0;
    try {
      allowedCharactersLimit = Integer.parseInt(carbonProperties
          .getProperty(CARBON_MINMAX_ALLOWED_BYTE_COUNT,
              CarbonCommonConstants.CARBON_MINMAX_ALLOWED_BYTE_COUNT_DEFAULT));
      if (allowedCharactersLimit < CarbonCommonConstants.CARBON_MINMAX_ALLOWED_BYTE_COUNT_MIN
          || allowedCharactersLimit
          > CarbonCommonConstants.CARBON_MINMAX_ALLOWED_BYTE_COUNT_MAX) {
        LOGGER.info(String.format("The min max byte limit for " +
                        "string type value \"%s\" is invalid. " +
                        "Using the default value \"%s\"",
                allowedCharactersLimit,
                CarbonCommonConstants.CARBON_MINMAX_ALLOWED_BYTE_COUNT_DEFAULT));
        carbonProperties.setProperty(CARBON_MINMAX_ALLOWED_BYTE_COUNT,
            CarbonCommonConstants.CARBON_MINMAX_ALLOWED_BYTE_COUNT_DEFAULT);
      } else {
        LOGGER.info(
            "Considered value for min max byte limit for string is: " + allowedCharactersLimit);
        carbonProperties
            .setProperty(CARBON_MINMAX_ALLOWED_BYTE_COUNT, allowedCharactersLimit + "");
      }
    } catch (NumberFormatException e) {
      LOGGER.info(String.format("The min max byte limit for string type value \"%s\" is invalid. " +
                      "Using the default value \"%s\"",
              allowedCharactersLimit,
              CarbonCommonConstants.CARBON_MINMAX_ALLOWED_BYTE_COUNT_DEFAULT));
      carbonProperties.setProperty(CARBON_MINMAX_ALLOWED_BYTE_COUNT,
          CarbonCommonConstants.CARBON_MINMAX_ALLOWED_BYTE_COUNT_DEFAULT);
    }
  }

  /**
   * This method validates the DETAIL_QUERY_BATCH_SIZE. If some invalid input is set, we use the
   * default value for this property
   */
  private void validateDetailQueryBatchSize() {
    String batchSizeString =
        carbonProperties.getProperty(DETAIL_QUERY_BATCH_SIZE);
    if (batchSizeString == null) {
      carbonProperties.setProperty(DETAIL_QUERY_BATCH_SIZE,
          Integer.toString(DETAIL_QUERY_BATCH_SIZE_DEFAULT));
      LOGGER.info(
          "Using default value for carbon.detail.batch.size " + DETAIL_QUERY_BATCH_SIZE_DEFAULT);
    } else {
      int batchSize;
      try {
        batchSize = Integer.parseInt(batchSizeString);
        if (batchSize < DETAIL_QUERY_BATCH_SIZE_MIN || batchSize > DETAIL_QUERY_BATCH_SIZE_MAX) {
          LOGGER.warn("Invalid carbon.detail.batch.size.Using default value "
              + DETAIL_QUERY_BATCH_SIZE_DEFAULT);
          carbonProperties.setProperty(DETAIL_QUERY_BATCH_SIZE,
              Integer.toString(DETAIL_QUERY_BATCH_SIZE_DEFAULT));
        }
      } catch (NumberFormatException ne) {
        LOGGER.info("Invalid carbon.detail.batch.size.Using default value "
            + DETAIL_QUERY_BATCH_SIZE_DEFAULT);
        carbonProperties.setProperty(DETAIL_QUERY_BATCH_SIZE,
            Integer.toString(DETAIL_QUERY_BATCH_SIZE_DEFAULT));
      }
    }
  }

  private void validateDMSchemaStorageProvider() {
    String provider =
        carbonProperties.getProperty(CarbonCommonConstants.CARBON_INDEX_SCHEMA_STORAGE);
    if (provider == null) {
      carbonProperties.setProperty(
          CarbonCommonConstants.CARBON_INDEX_SCHEMA_STORAGE,
          CarbonCommonConstants.CARBON_INDEX_SCHEMA_STORAGE_DEFAULT);
    } else {
      switch (provider.toUpperCase()) {
        case CarbonCommonConstants.CARBON_INDEX_SCHEMA_STORAGE_DISK:
        case  CarbonCommonConstants.CARBON_INDEX_SCHEMA_STORAGE_DATABASE:
          break;
        default:
          LOGGER.warn("The value \"" + provider + "\" configured for key "
              + CarbonCommonConstants.CARBON_INDEX_SCHEMA_STORAGE
              + " is invalid for current file system. Use the default value "
              + CarbonCommonConstants.CARBON_INDEX_SCHEMA_STORAGE_DEFAULT + " instead.");
          carbonProperties.setProperty(
              CarbonCommonConstants.CARBON_INDEX_SCHEMA_STORAGE,
              CarbonCommonConstants.CARBON_INDEX_SCHEMA_STORAGE_DEFAULT);
      }
    }
  }

  /**
   * Check whether the Distributed Pruning is enabled by the user or not.
   */
  public boolean isDistributedPruningEnabled(String dbName, String tableName) {
    // Check if user has enabled/disabled the use of index server for the current session using
    // the set command
    String configuredValue = getSessionPropertyValue(
        CarbonCommonConstants.CARBON_ENABLE_INDEX_SERVER + "." + dbName + "." + tableName);
    if (configuredValue == null) {
      // if not set in session properties then check carbon.properties for the same.
      configuredValue = getProperty(CarbonCommonConstants.CARBON_ENABLE_INDEX_SERVER);
    }
    boolean isServerEnabledByUser = Boolean.parseBoolean(configuredValue);
    if (isServerEnabledByUser) {
      LOGGER.info("Distributed Index server is enabled for " + dbName + "." + tableName);
    }
    return isServerEnabledByUser;
  }

  /**
   * Check whether the MV is enabled by the user or not.
   */
  public boolean isMVEnabled() {
    String mvEnabled = CarbonProperties.getInstance().getProperty(
            CarbonCommonConstants.CARBON_ENABLE_MV);
    if (mvEnabled == null || !CarbonUtil.validateBoolean(mvEnabled)) {
      return Boolean.parseBoolean(CarbonCommonConstants.CARBON_ENABLE_MV_DEFAULT);
    } else {
      return mvEnabled.equalsIgnoreCase("true");
    }
  }

  /**
   * Check if user has enabled/disabled the use of pre-priming for index server
   */
  public boolean isIndexServerPrePrimingEnabled() {
    String configuredValue = carbonProperties.getProperty(
            CarbonCommonConstants.CARBON_INDEXSEVER_ENABLE_PREPRIMING);
    return Boolean.parseBoolean(configuredValue);
  }

  public String getIndexServerIP() {
    return carbonProperties.getProperty(CarbonCommonConstants.CARBON_INDEX_SERVER_IP, "");
  }

  public int getIndexServerPort() {
    String configuredPort =
        carbonProperties.getProperty(CarbonCommonConstants.CARBON_INDEX_SERVER_PORT);
    try {
      return Integer.parseInt(configuredPort);
    } catch (NumberFormatException e) {
      LOGGER.error("Configured port for index server is not a valid number", e);
      throw e;
    }
  }

  /**
   * Whether fallback is disabled by the user or not.
   */
  public boolean isFallBackDisabled() {
    return Boolean.parseBoolean(carbonProperties
        .getProperty(CarbonCommonConstants.CARBON_DISABLE_INDEX_SERVER_FALLBACK, "false"));
  }

  public int getNumberOfHandlersForIndexServer() {
    String configuredValue =
        carbonProperties.getProperty(CarbonCommonConstants.CARBON_INDEX_SERVER_WORKER_THREADS);
    if (configuredValue != null) {
      return Integer.parseInt(configuredValue);
    }
    return CarbonCommonConstants.CARBON_INDEX_SERVER_WORKER_THREADS_DEFAULT;
  }

  public int getNumOfThreadsForExecutorPruning() {
    String configuredValue = CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.CARBON_MAX_EXECUTOR_THREADS_FOR_BLOCK_PRUNING);
    if (configuredValue == null || configuredValue.equalsIgnoreCase("0")) {
      configuredValue = CarbonCommonConstants.CARBON_MAX_EXECUTOR_THREADS_FOR_BLOCK_PRUNING_DEFAULT;
    }
    try {
      int numOfThreads = Integer.parseInt(configuredValue);
      LOGGER.info("Value for "
          + CarbonCommonConstants.CARBON_MAX_EXECUTOR_THREADS_FOR_BLOCK_PRUNING + " is "
          + numOfThreads);
      return numOfThreads;
    } catch (NumberFormatException e) {
      LOGGER.info(configuredValue + " is not a valid input for "
          + CarbonCommonConstants.CARBON_MAX_EXECUTOR_THREADS_FOR_BLOCK_PRUNING + ", taking "
          + CarbonCommonConstants.CARBON_MAX_EXECUTOR_THREADS_FOR_BLOCK_PRUNING_DEFAULT
          + " as default value");
      return Integer
          .parseInt(CarbonCommonConstants.CARBON_MAX_EXECUTOR_THREADS_FOR_BLOCK_PRUNING_DEFAULT);
    }
  }

  /**
   * This method validates the numOfThreadsForPruning
   */
  public static int getNumOfThreadsForPruning() {
    int numOfThreadsForPruning;
    String maxDriverThreadsForBockPruning =
        CarbonCommonConstants.CARBON_MAX_DRIVER_THREADS_FOR_BLOCK_PRUNING;
    int defaultNumberOfThreads =
        Integer.parseInt(CarbonCommonConstants.CARBON_MAX_DRIVER_THREADS_FOR_BLOCK_PRUNING_DEFAULT);
    String logMessage = " is not a valid input for " + maxDriverThreadsForBockPruning
        + ". Using the default number of threads : " + defaultNumberOfThreads;
    try {
      numOfThreadsForPruning = Integer.parseInt(CarbonProperties.getInstance()
          .getProperty(maxDriverThreadsForBockPruning, String.valueOf(defaultNumberOfThreads)));
      if (numOfThreadsForPruning > defaultNumberOfThreads || numOfThreadsForPruning < 1) {
        LOGGER.info(numOfThreadsForPruning + logMessage);
        numOfThreadsForPruning = defaultNumberOfThreads;
      }
    } catch (NumberFormatException e) {
      LOGGER.info(
          CarbonProperties.getInstance().getProperty(maxDriverThreadsForBockPruning + logMessage));
      numOfThreadsForPruning = defaultNumberOfThreads;
    }
    return numOfThreadsForPruning;
  }

  /**
   * This method validates the driverPruningMultiThreadEnableFilesCount
   */
  public static int getDriverPruningMultiThreadEnableFilesCount() {
    int driverPruningMultiThreadEnableFilesCount = 0;
    try {
      driverPruningMultiThreadEnableFilesCount = Integer.parseInt(CarbonProperties.getInstance()
          .getProperty(
              CarbonCommonConstants.CARBON_DRIVER_PRUNING_MULTI_THREAD_ENABLE_FILES_COUNT,
              CarbonCommonConstants.CARBON_DRIVER_PRUNING_MULTI_THREAD_ENABLE_FILES_COUNT_DEFAULT));
      if (driverPruningMultiThreadEnableFilesCount <= 0) {
        LOGGER.info("The driver pruning multi-thread enable files count value \""
            + driverPruningMultiThreadEnableFilesCount
            + "\" is invalid. Using the default value \""
            + CarbonCommonConstants.CARBON_DRIVER_PRUNING_MULTI_THREAD_ENABLE_FILES_COUNT_DEFAULT);
        driverPruningMultiThreadEnableFilesCount = Integer.parseInt(CarbonCommonConstants
            .CARBON_DRIVER_PRUNING_MULTI_THREAD_ENABLE_FILES_COUNT_DEFAULT);
      }
    } catch (NumberFormatException e) {
      LOGGER.info("The driver pruning multi-thread enable files count value " +
              "is invalid. Using the default value \""
          + CarbonCommonConstants.CARBON_DRIVER_PRUNING_MULTI_THREAD_ENABLE_FILES_COUNT_DEFAULT);
      driverPruningMultiThreadEnableFilesCount = Integer.parseInt(CarbonCommonConstants
          .CARBON_DRIVER_PRUNING_MULTI_THREAD_ENABLE_FILES_COUNT_DEFAULT);
    }
    return driverPruningMultiThreadEnableFilesCount;
  }

  /**
   * Validate and get the input metrics interval
   *
   * @return input metrics interval
   */
  public static Long getInputMetricsInterval() {
    String metrics = CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.INPUT_METRICS_UPDATE_INTERVAL);
    if (metrics == null) {
      return CarbonCommonConstants.INPUT_METRICS_UPDATE_INTERVAL_DEFAULT;
    } else {
      try {
        long configuredValue = Long.parseLong(metrics);
        if (configuredValue < 0) {
          return CarbonCommonConstants.INPUT_METRICS_UPDATE_INTERVAL_DEFAULT;
        } else {
          return configuredValue;
        }
      } catch (Exception ex) {
        return CarbonCommonConstants.INPUT_METRICS_UPDATE_INTERVAL_DEFAULT;
      }
    }
  }

  /**
   * Validate and get the input metrics interval
   *
   * @return input metrics interval
   */
  public static Long getInsertStageTimeout() {
    String timeout = CarbonProperties.getInstance()
            .getProperty(CarbonCommonConstants.CARBON_INSERT_STAGE_TIMEOUT);
    if (timeout == null) {
      return CarbonCommonConstants.CARBON_INSERT_STAGE_TIMEOUT_DEFAULT;
    } else {
      try {
        long configuredValue = Long.parseLong(timeout);
        if (configuredValue < 0) {
          LOGGER.warn(String.format("The value \"%s\" configured for key \"%s\" " +
                  "is invalid. Ignoring it. use default value:\"%s\"", timeout,
                  CarbonCommonConstants.CARBON_INSERT_STAGE_TIMEOUT,
                  CarbonCommonConstants.CARBON_INSERT_STAGE_TIMEOUT_DEFAULT));
          return CarbonCommonConstants.CARBON_INSERT_STAGE_TIMEOUT_DEFAULT;
        } else {
          return configuredValue;
        }
      } catch (NumberFormatException e) {
        LOGGER.warn(String.format("The value \"%s\" configured for key \"%s\" " +
                "is invalid. Ignoring it. use default value:\"%s\"", timeout,
                CarbonCommonConstants.CARBON_INSERT_STAGE_TIMEOUT,
                CarbonCommonConstants.CARBON_INSERT_STAGE_TIMEOUT_DEFAULT));
        return CarbonCommonConstants.CARBON_INSERT_STAGE_TIMEOUT_DEFAULT;
      }
    }
  }

  /**
   * Validate and get query prefetch enable
   *
   * @return boolean prefetch value
   */
  public static Boolean getQueryPrefetchEnable() {
    String prefetchEnable = CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.CARBON_QUERY_PREFETCH_ENABLE);
    if (prefetchEnable == null) {
      return Boolean.parseBoolean(CarbonCommonConstants.CARBON_QUERY_PREFETCH_ENABLE_DEFAULT);
    } else {
      // return false only if false is set. any other case return true
      return !prefetchEnable.equalsIgnoreCase("false");
    }
  }

  /**
   * Validate and get unique value check enabled
   *
   * @return boolean
   */
  public static Boolean isUniqueValueCheckEnabled() {
    String needValidate = CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.CARBON_UPDATE_CHECK_UNIQUE_VALUE);
    if (needValidate == null) {
      return Boolean
          .parseBoolean(CarbonCommonConstants.CARBON_UPDATE_CHECK_UNIQUE_VALUE_DEFAULT);
    } else {
      // return false only if false is set. any other case return true
      return !needValidate.equalsIgnoreCase("false");
    }
  }

  /**
   * get local dictionary size threshold in mb.
   */
  private void validateAndGetLocalDictionarySizeThresholdInMB() {
    String sizeStr = carbonProperties
        .getProperty(CarbonCommonConstants.CARBON_LOCAL_DICTIONARY_SIZE_THRESHOLD_IN_MB);
    String defaultValue = Integer
        .toString(CarbonCommonConstants.CARBON_LOCAL_DICTIONARY_SIZE_THRESHOLD_IN_MB_DEFAULT);
    if (sizeStr == null) {
      carbonProperties
          .setProperty(CarbonCommonConstants.CARBON_LOCAL_DICTIONARY_SIZE_THRESHOLD_IN_MB,
              defaultValue);
    } else {
      try {
        int size = Integer.parseInt(sizeStr);
        if (size < 0 || size == 0
            || size > CarbonCommonConstants.CARBON_LOCAL_DICTIONARY_SIZE_THRESHOLD_IN_MB_MAX) {
          LOGGER.info("using default value of carbon.local.dictionary.size.threshold.inmb = "
              + defaultValue);
          carbonProperties
              .setProperty(CarbonCommonConstants.CARBON_LOCAL_DICTIONARY_SIZE_THRESHOLD_IN_MB,
                  defaultValue);
        } else {
          LOGGER.info("using carbon.local.dictionary.size.threshold.inmb = " + size);
          carbonProperties
              .setProperty(CarbonCommonConstants.CARBON_LOCAL_DICTIONARY_SIZE_THRESHOLD_IN_MB,
                  Integer.toString(size));
        }
      } catch (Exception ex) {
        LOGGER.info(
            "using default value of carbon.local.dictionary.size.threshold.inmb = " + defaultValue);
        carbonProperties
            .setProperty(CarbonCommonConstants.CARBON_LOCAL_DICTIONARY_SIZE_THRESHOLD_IN_MB,
                defaultValue);
      }
    }
  }

  public static String getIndexStorageProvider() {
    String provider = CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.CARBON_INDEX_SCHEMA_STORAGE);
    if (provider == null) {
      return CarbonCommonConstants.CARBON_INDEX_SCHEMA_STORAGE_DEFAULT;
    }
    return provider.toUpperCase();
  }

  /**
   * Validate and get bad record handling for insert
   *
   * @return boolean
   */
  public static Boolean isBadRecordHandlingEnabledForInsert() {
    String badRecordHandling = CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.CARBON_ENABLE_BAD_RECORD_HANDLING_FOR_INSERT);
    if (badRecordHandling == null) {
      return Boolean
          .parseBoolean(CarbonCommonConstants.CARBON_ENABLE_BAD_RECORD_HANDLING_FOR_INSERT_DEFAULT);
    } else {
      return badRecordHandling.equalsIgnoreCase("true");
    }
  }

  public String getDefaultCompressor() {
    return getProperty(CarbonCommonConstants.COMPRESSOR, CarbonCommonConstants.DEFAULT_COMPRESSOR);
  }

  public static boolean isQueryStageInputEnabled() {
    return Boolean.parseBoolean(getInstance().getProperty(
        CarbonCommonConstants.CARBON_QUERY_STAGE_INPUT,
        CarbonCommonConstants.CARBON_QUERY_STAGE_INPUT_DEFAULT));
  }

  public static boolean isAuditEnabled() {
    return Boolean.parseBoolean(getInstance().getProperty(
        CarbonCommonConstants.CARBON_ENABLE_AUDIT,
        CarbonCommonConstants.CARBON_ENABLE_AUDIT_DEFAULT
    ));
  }

  public static void setAuditEnabled(boolean enabled) {
    getInstance().addProperty(CarbonCommonConstants.CARBON_ENABLE_AUDIT, String.valueOf(enabled));
  }

  public boolean isSetLenientEnabled() {
    String configuredValue =
        getProperty(CarbonCommonConstants.CARBON_LOAD_DATEFORMAT_SETLENIENT_ENABLE,
            CarbonCommonConstants.CARBON_LOAD_DATEFORMAT_SETLENIENT_ENABLE_DEFAULT);
    return Boolean.parseBoolean(configuredValue);
  }

  public long getMetaCacheExpirationTime() {
    String configuredValue = CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.CARBON_METACACHE_EXPIRATION_TIME_IN_SECONDS);
    if (configuredValue == null || configuredValue.equalsIgnoreCase("0")) {
      return CarbonCommonConstants.CARBON_METACACHE_EXPIRATION_TIME_IN_SECONDS_DEFAULT;
    }
    try {
      long expirationTime = Long.parseLong(configuredValue);
      LOGGER.info("Value for "
          + CarbonCommonConstants.CARBON_METACACHE_EXPIRATION_TIME_IN_SECONDS + " is "
          + expirationTime + ".");
      return expirationTime;
    } catch (NumberFormatException e) {
      LOGGER.warn(configuredValue + " is not a valid input for "
          + CarbonCommonConstants.CARBON_METACACHE_EXPIRATION_TIME_IN_SECONDS + ", taking "
          + CarbonCommonConstants.CARBON_METACACHE_EXPIRATION_TIME_IN_SECONDS_DEFAULT
          + " as default value.");
      return CarbonCommonConstants.CARBON_METACACHE_EXPIRATION_TIME_IN_SECONDS_DEFAULT;
    }
  }

  public boolean isSIRepairEnabled(String dbName, String tableName) {
    // Check if user has enabled/disabled the use of property for the current db and table using
    // the set command
    String configuredValue = getSessionPropertyValue(
            CarbonCommonConstants.CARBON_LOAD_SI_REPAIR + "." + dbName + "." + tableName);
    if (configuredValue == null) {
      // if not set in session properties then check carbon.properties for the same.
      configuredValue = getProperty(CarbonCommonConstants.CARBON_LOAD_SI_REPAIR,
            CarbonCommonConstants.CARBON_LOAD_SI_REPAIR_DEFAULT);
    }
    boolean propertyEnabled =  Boolean.parseBoolean(configuredValue);
    if (propertyEnabled) {
      LOGGER.info("SI Repair is enabled for " + dbName + "." + tableName);
    }
    return propertyEnabled;
  }

  public int getMaxSIRepairLimit(String dbName, String tableName) {
    // Check if user has enabled/disabled the use of property for the current db and table using
    // the set command
    String thresholdValue = getSessionPropertyValue(
        CarbonCommonConstants.CARBON_LOAD_SI_REPAIR + "." + dbName + "." + tableName);
    if (thresholdValue == null) {
      // if not set in session properties then check carbon.properties for the same.
      thresholdValue = getProperty(CarbonCommonConstants.CARBON_SI_REPAIR_LIMIT);
    }
    if (thresholdValue == null) {
      return Integer.MAX_VALUE;
    }
    return Math.abs(Integer.parseInt(thresholdValue));
  }

  /**
   * The below method sets the time(in days) for which timestamp folder retention in trash
   * folder will take place
   */
  private void validateTrashFolderRetentionTime() {
    try {
      int configuredValue = getTrashFolderRetentionTime();
      if (configuredValue < 0 ||
          configuredValue > CarbonCommonConstants.CARBON_TRASH_RETENTION_DAYS_MAXIMUM) {
        LOGGER.warn("Value of " + CarbonCommonConstants.CARBON_TRASH_RETENTION_DAYS + " is" +
            " invalid, taking default value instead");
        carbonProperties.setProperty(CarbonCommonConstants.CARBON_TRASH_RETENTION_DAYS,
            CarbonCommonConstants.CARBON_TRASH_RETENTION_DAYS_DEFAULT);
      } else {
        carbonProperties.setProperty(CarbonCommonConstants.CARBON_TRASH_RETENTION_DAYS,
            configuredValue + "");
      }
    } catch (NumberFormatException e) {
      LOGGER.error("Invalid value configured for " + CarbonCommonConstants
          .CARBON_TRASH_RETENTION_DAYS + ", considering the default value");
      carbonProperties.setProperty(CarbonCommonConstants.CARBON_TRASH_RETENTION_DAYS,
          CarbonCommonConstants.CARBON_TRASH_RETENTION_DAYS_DEFAULT);
    }
  }

  public int getTrashFolderRetentionTime() {
    return Integer.parseInt(carbonProperties.getProperty(
        CarbonCommonConstants.CARBON_TRASH_RETENTION_DAYS,
        CarbonCommonConstants.CARBON_TRASH_RETENTION_DAYS_DEFAULT));
  }

  /**
   * Check if the user has allowed the use of clean files command with force option.
   */
  public boolean isCleanFilesForceAllowed() {
    String configuredValue =
        getProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED,
        CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED_DEFAULT);
    return Boolean.parseBoolean(configuredValue);
  }

  public static boolean isFilterReorderingEnabled() {
    return Boolean.parseBoolean(
        getInstance().getProperty(CarbonCommonConstants.CARBON_REORDER_FILTER,
        CarbonCommonConstants.CARBON_REORDER_FILTER_DEFAULT)
    );
  }

  /**
   * Check whether coarse grain secondary index is enabled or not. If property is not configured,
   * default value {@link CarbonCommonConstants#CARBON_COARSE_GRAIN_SECONDARY_INDEX_DEFAULT} is
   * returned
   */
  public boolean isCoarseGrainSecondaryIndex(String dbName, String tableName) {
    return isCoarseGrainSecondaryIndex(dbName, tableName,
        CarbonCommonConstants.CARBON_COARSE_GRAIN_SECONDARY_INDEX_DEFAULT);
  }

  /**
   * Check whether coarse grain secondary index is enabled or not. If property is not configured,
   * input default value is returned
   */
  public boolean isCoarseGrainSecondaryIndex(String dbName, String tableName, String defaultValue) {
    String configuredValue = getProperty(
        CarbonCommonConstants.CARBON_COARSE_GRAIN_SECONDARY_INDEX + "." + dbName + "." + tableName);
    if (configuredValue == null) {
      configuredValue =
          getProperty(CarbonCommonConstants.CARBON_COARSE_GRAIN_SECONDARY_INDEX, defaultValue);
    }
    boolean isCoarseGrainSecondaryIndex = Boolean.parseBoolean(configuredValue);
    if (isCoarseGrainSecondaryIndex) {
      LOGGER.info("Coarse grain secondary index is enabled for " + dbName + "." + tableName);
    }
    return isCoarseGrainSecondaryIndex;
  }

  /**
   * for test to print current configuration
   */
  @Override
  public String toString() {
    return carbonProperties.toString();
  }
}
