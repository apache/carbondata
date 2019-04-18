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

package org.apache.carbondata.core;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.constants.CarbonLoadOptionConstants;
import org.apache.carbondata.core.util.CarbonProperties;

import junit.framework.TestCase;
import org.junit.Test;

/**
 * Method to test the carbon common constant configurations.
 */
public class CarbonPropertiesValidationTest extends TestCase {

  CarbonProperties carbonProperties;

  @Override public void setUp() throws Exception {
    carbonProperties = CarbonProperties.getInstance();
  }

  @Test public void testValidateLockType()
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    Method validateMethodType = carbonProperties.getClass().getDeclaredMethod("validateLockType");
    validateMethodType.setAccessible(true);
    carbonProperties.addProperty(CarbonCommonConstants.LOCK_TYPE, "xyz");
    String valueBeforeValidation = carbonProperties.getProperty(CarbonCommonConstants.LOCK_TYPE);
    validateMethodType.invoke(carbonProperties);
    String valueAfterValidation = carbonProperties.getProperty(CarbonCommonConstants.LOCK_TYPE);
    assertTrue(!valueBeforeValidation.equals(valueAfterValidation));
    assertTrue(CarbonCommonConstants.CARBON_LOCK_TYPE_LOCAL.equalsIgnoreCase(valueAfterValidation));
  }

  @Test public void testValidateEnableUnsafeSort()
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    Method validateMethodType =
        carbonProperties.getClass().getDeclaredMethod("validateEnableUnsafeSort");
    validateMethodType.setAccessible(true);
    carbonProperties.addProperty(CarbonCommonConstants.ENABLE_UNSAFE_SORT, "xyz");
    String valueBeforeValidation =
        carbonProperties.getProperty(CarbonCommonConstants.ENABLE_UNSAFE_SORT);
    validateMethodType.invoke(carbonProperties);
    String valueAfterValidation =
        carbonProperties.getProperty(CarbonCommonConstants.ENABLE_UNSAFE_SORT);
    assertTrue(valueBeforeValidation.equals(valueAfterValidation));
    assertTrue(
        CarbonCommonConstants.ENABLE_UNSAFE_SORT_DEFAULT.equalsIgnoreCase(valueAfterValidation));
  }

  @Test public void testValidateEnableOffHeapSort()
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    Method validateMethodType =
        carbonProperties.getClass().getDeclaredMethod("validateEnableOffHeapSort");
    validateMethodType.setAccessible(true);
    carbonProperties.addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT, "True");
    assert (carbonProperties.getProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT)
        .equalsIgnoreCase("true"));
    carbonProperties.addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT, "false");
    assert (carbonProperties.getProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT)
        .equalsIgnoreCase("false"));
    carbonProperties.addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT, "xyz");
    assert (carbonProperties.getProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT)
        .equalsIgnoreCase("true"));
    String valueBeforeValidation =
        carbonProperties.getProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT);
    validateMethodType.invoke(carbonProperties);
    String valueAfterValidation =
        carbonProperties.getProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT);
    assertTrue(valueBeforeValidation.equals(valueAfterValidation));
    assertTrue(
        CarbonCommonConstants.ENABLE_OFFHEAP_SORT_DEFAULT.equalsIgnoreCase(valueAfterValidation));
  }

  @Test public void testValidateCustomBlockDistribution()
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    Method validateMethodType =
        carbonProperties.getClass().getDeclaredMethod("validateCustomBlockDistribution");
    validateMethodType.setAccessible(true);
    carbonProperties.addProperty(CarbonCommonConstants.CARBON_CUSTOM_BLOCK_DISTRIBUTION, "xyz");
    String valueBeforeValidation =
        carbonProperties.getProperty(CarbonCommonConstants.CARBON_CUSTOM_BLOCK_DISTRIBUTION);
    validateMethodType.invoke(carbonProperties);
    String valueAfterValidation =
        carbonProperties.getProperty(CarbonCommonConstants.CARBON_CUSTOM_BLOCK_DISTRIBUTION);
    assertTrue(valueBeforeValidation.equals(valueAfterValidation));
    assertTrue("false"
        .equalsIgnoreCase(valueAfterValidation));
  }

  @Test public void testValidateEnableVectorReader()
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    Method validateMethodType =
        carbonProperties.getClass().getDeclaredMethod("validateEnableVectorReader");
    validateMethodType.setAccessible(true);
    carbonProperties.addProperty(CarbonCommonConstants.ENABLE_VECTOR_READER, "xyz");
    String valueBeforeValidation =
        carbonProperties.getProperty(CarbonCommonConstants.ENABLE_VECTOR_READER);
    validateMethodType.invoke(carbonProperties);
    String valueAfterValidation =
        carbonProperties.getProperty(CarbonCommonConstants.ENABLE_VECTOR_READER);
    assertTrue(valueBeforeValidation.equals(valueAfterValidation));
    assertTrue(
        CarbonCommonConstants.ENABLE_VECTOR_READER_DEFAULT.equalsIgnoreCase(valueAfterValidation));
  }

  @Test public void testValidateCarbonCSVReadBufferSizeByte()
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    Method validateMethodType =
        carbonProperties.getClass().getDeclaredMethod("validateCarbonCSVReadBufferSizeByte");
    validateMethodType.setAccessible(true);
    carbonProperties.addProperty(CarbonCommonConstants.CSV_READ_BUFFER_SIZE, "xyz");
    String valueBeforeValidation =
        carbonProperties.getProperty(CarbonCommonConstants.CSV_READ_BUFFER_SIZE);
    validateMethodType.invoke(carbonProperties);
    String valueAfterValidation =
        carbonProperties.getProperty(CarbonCommonConstants.CSV_READ_BUFFER_SIZE);
    assertTrue(valueBeforeValidation.equals(valueAfterValidation));
    assertTrue(
        CarbonCommonConstants.CSV_READ_BUFFER_SIZE_DEFAULT.equalsIgnoreCase(valueAfterValidation));
  }

  @Test public void testValidateCarbonCSVReadBufferSizeByteRange()
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    Method validateMethodType =
        carbonProperties.getClass().getDeclaredMethod("validateCarbonCSVReadBufferSizeByte");
    validateMethodType.setAccessible(true);
    carbonProperties.addProperty(CarbonCommonConstants.CSV_READ_BUFFER_SIZE, "10485761");
    String valueBeforeValidation =
        carbonProperties.getProperty(CarbonCommonConstants.CSV_READ_BUFFER_SIZE);
    validateMethodType.invoke(carbonProperties);
    String valueAfterValidation =
        carbonProperties.getProperty(CarbonCommonConstants.CSV_READ_BUFFER_SIZE);
    assertTrue(valueBeforeValidation.equals(valueAfterValidation));
    assertTrue(
        CarbonCommonConstants.CSV_READ_BUFFER_SIZE_DEFAULT.equalsIgnoreCase(valueAfterValidation));
    carbonProperties.addProperty(CarbonCommonConstants.CSV_READ_BUFFER_SIZE, "10240");
    valueBeforeValidation =
        carbonProperties.getProperty(CarbonCommonConstants.CSV_READ_BUFFER_SIZE);
    validateMethodType.invoke(carbonProperties);
    valueAfterValidation =
        carbonProperties.getProperty(CarbonCommonConstants.CSV_READ_BUFFER_SIZE);
    assertTrue(valueBeforeValidation.equals(valueAfterValidation));
    carbonProperties.addProperty(CarbonCommonConstants.CSV_READ_BUFFER_SIZE, "10239");
    valueBeforeValidation =
        carbonProperties.getProperty(CarbonCommonConstants.CSV_READ_BUFFER_SIZE);
    validateMethodType.invoke(carbonProperties);
    valueAfterValidation =
        carbonProperties.getProperty(CarbonCommonConstants.CSV_READ_BUFFER_SIZE);
    assertTrue(valueBeforeValidation.equals(valueAfterValidation));
    assertTrue(
        CarbonCommonConstants.CSV_READ_BUFFER_SIZE_DEFAULT.equalsIgnoreCase(valueAfterValidation));
  }

  @Test public void testValidateHandoffSize() {
    assertEquals(CarbonCommonConstants.HANDOFF_SIZE_DEFAULT, carbonProperties.getHandoffSize());
    long newSize = 1024L * 1024 * 100;
    carbonProperties.addProperty(CarbonCommonConstants.HANDOFF_SIZE, "" + newSize);
    assertEquals(newSize, carbonProperties.getHandoffSize());
  }

  @Test public void testValidateTimeStampFormat()
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    Method validateMethodType = carbonProperties.getClass()
        .getDeclaredMethod("validateTimeFormatKey", new Class[] { String.class, String.class });
    validateMethodType.setAccessible(true);
    carbonProperties.addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "agdgaJIASDG667");
    String valueBeforeValidation =
        carbonProperties.getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT);
    validateMethodType.invoke(carbonProperties, CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT);
    String valueAfterValidation =
        carbonProperties.getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT);
    assertTrue(valueBeforeValidation.equals(valueAfterValidation));
    assertTrue(CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT
        .equalsIgnoreCase(valueAfterValidation));
    carbonProperties.addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        "yyyy-MM-dd hh:mm:ss");
    validateMethodType.invoke(carbonProperties, CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT);
    assertEquals("yyyy-MM-dd hh:mm:ss",
        carbonProperties.getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT));
  }

  @Test public void testValidateSortFileWriteBufferSize()
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    Method validateMethodType =
        carbonProperties.getClass().getDeclaredMethod("validateSortFileWriteBufferSize");
    validateMethodType.setAccessible(true);
    carbonProperties.addProperty(CarbonCommonConstants.CARBON_SORT_FILE_WRITE_BUFFER_SIZE, "test");
    String valueBeforeValidation =
        carbonProperties.getProperty(CarbonCommonConstants.CARBON_SORT_FILE_WRITE_BUFFER_SIZE);
    validateMethodType.invoke(carbonProperties);
    String valueAfterValidation =
        carbonProperties.getProperty(CarbonCommonConstants.CARBON_SORT_FILE_WRITE_BUFFER_SIZE);
    assertTrue(valueBeforeValidation.equals(valueAfterValidation));
    assertTrue(CarbonCommonConstants.CARBON_SORT_FILE_WRITE_BUFFER_SIZE_DEFAULT_VALUE
        .equalsIgnoreCase(valueAfterValidation));
  }

  @Test public void testValidateSortIntermediateFilesLimit()
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    Method validateMethodType =
        carbonProperties.getClass().getDeclaredMethod("validateSortIntermediateFilesLimit");
    validateMethodType.setAccessible(true);
    carbonProperties.addProperty(CarbonCommonConstants.SORT_INTERMEDIATE_FILES_LIMIT, "test");
    String valueBeforeValidation =
        carbonProperties.getProperty(CarbonCommonConstants.SORT_INTERMEDIATE_FILES_LIMIT);
    validateMethodType.invoke(carbonProperties);
    String valueAfterValidation =
        carbonProperties.getProperty(CarbonCommonConstants.SORT_INTERMEDIATE_FILES_LIMIT);
    assertTrue(valueBeforeValidation.equals(valueAfterValidation));
    assertTrue(CarbonCommonConstants.SORT_INTERMEDIATE_FILES_LIMIT_DEFAULT_VALUE
        .equalsIgnoreCase(valueAfterValidation));
  }

  @Test public void testValidateDynamicSchedulerTimeOut() {
    carbonProperties
        .addProperty(CarbonCommonConstants.CARBON_DYNAMIC_ALLOCATION_SCHEDULER_TIMEOUT, "2");
    String valueAfterValidation = carbonProperties
        .getProperty(CarbonCommonConstants.CARBON_DYNAMIC_ALLOCATION_SCHEDULER_TIMEOUT);
    assertTrue(valueAfterValidation
        .equals(CarbonCommonConstants.CARBON_DYNAMIC_ALLOCATION_SCHEDULER_TIMEOUT_DEFAULT));
    carbonProperties
        .addProperty(CarbonCommonConstants.CARBON_DYNAMIC_ALLOCATION_SCHEDULER_TIMEOUT, "16");
    valueAfterValidation = carbonProperties
        .getProperty(CarbonCommonConstants.CARBON_DYNAMIC_ALLOCATION_SCHEDULER_TIMEOUT);
    assertTrue(valueAfterValidation
        .equals(CarbonCommonConstants.CARBON_DYNAMIC_ALLOCATION_SCHEDULER_TIMEOUT_DEFAULT));
    carbonProperties
        .addProperty(CarbonCommonConstants.CARBON_DYNAMIC_ALLOCATION_SCHEDULER_TIMEOUT, "15");
    valueAfterValidation = carbonProperties
        .getProperty(CarbonCommonConstants.CARBON_DYNAMIC_ALLOCATION_SCHEDULER_TIMEOUT);
    assertTrue(valueAfterValidation
        .equals("15"));

  }

  @Test public void testValidateSchedulerMinRegisteredRatio() {
    carbonProperties
        .addProperty(CarbonCommonConstants.CARBON_SCHEDULER_MIN_REGISTERED_RESOURCES_RATIO, "0.0");
    String valueAfterValidation = carbonProperties
        .getProperty(CarbonCommonConstants.CARBON_SCHEDULER_MIN_REGISTERED_RESOURCES_RATIO);
    assertTrue(valueAfterValidation
        .equals(CarbonCommonConstants.CARBON_SCHEDULER_MIN_REGISTERED_RESOURCES_RATIO_DEFAULT));
    carbonProperties
        .addProperty(CarbonCommonConstants.CARBON_SCHEDULER_MIN_REGISTERED_RESOURCES_RATIO, "-0.1");
    valueAfterValidation = carbonProperties
        .getProperty(CarbonCommonConstants.CARBON_SCHEDULER_MIN_REGISTERED_RESOURCES_RATIO);
    assertTrue(valueAfterValidation
        .equals(CarbonCommonConstants.CARBON_SCHEDULER_MIN_REGISTERED_RESOURCES_RATIO_DEFAULT));
    carbonProperties
        .addProperty(CarbonCommonConstants.CARBON_SCHEDULER_MIN_REGISTERED_RESOURCES_RATIO, "0.1");
    valueAfterValidation = carbonProperties
        .getProperty(CarbonCommonConstants.CARBON_SCHEDULER_MIN_REGISTERED_RESOURCES_RATIO);
    assertTrue(valueAfterValidation.equals("0.1"));
  }

  @Test public void testValidateBooleanProperty()
          throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    Method validateMethodType =
            carbonProperties.getClass().getDeclaredMethod("validateBooleanProperty", new Class[] {String.class});
    validateMethodType.setAccessible(true);
    carbonProperties.addProperty(CarbonCommonConstants.ENABLE_XXHASH, "xxhash");
    String valueBeforeValidation =
            carbonProperties.getProperty(CarbonCommonConstants.ENABLE_XXHASH);
    validateMethodType.invoke(carbonProperties, CarbonCommonConstants.ENABLE_XXHASH);
    String valueAfterValidation =
            carbonProperties.getProperty(CarbonCommonConstants.ENABLE_XXHASH);
    assertEquals(valueBeforeValidation, valueAfterValidation);
    assertTrue(
            CarbonCommonConstants.ENABLE_XXHASH_DEFAULT.equalsIgnoreCase(valueAfterValidation));
    carbonProperties.addProperty(CarbonCommonConstants.LOCAL_DICTIONARY_DECODER_BASED_FALLBACK, "localDictionaryDecoderBasedFallback");
    valueBeforeValidation =
            carbonProperties.getProperty(CarbonCommonConstants.LOCAL_DICTIONARY_DECODER_BASED_FALLBACK);
    validateMethodType.invoke(carbonProperties, CarbonCommonConstants.LOCAL_DICTIONARY_DECODER_BASED_FALLBACK);
    valueAfterValidation =
            carbonProperties.getProperty(CarbonCommonConstants.LOCAL_DICTIONARY_DECODER_BASED_FALLBACK);
    assertEquals(valueBeforeValidation, valueAfterValidation);
    assertTrue(
            CarbonCommonConstants.LOCAL_DICTIONARY_DECODER_BASED_FALLBACK_DEFAULT.equalsIgnoreCase(valueAfterValidation));
    carbonProperties.addProperty(CarbonCommonConstants.DATA_MANAGEMENT_DRIVER, "0.0");
    valueBeforeValidation =
            carbonProperties.getProperty(CarbonCommonConstants.DATA_MANAGEMENT_DRIVER);
    validateMethodType.invoke(carbonProperties, CarbonCommonConstants.DATA_MANAGEMENT_DRIVER);
    valueAfterValidation =
            carbonProperties.getProperty(CarbonCommonConstants.DATA_MANAGEMENT_DRIVER);
    assertEquals(valueBeforeValidation, valueAfterValidation);
    assertTrue(
            CarbonCommonConstants.DATA_MANAGEMENT_DRIVER_DEFAULT.equalsIgnoreCase(valueAfterValidation));
    carbonProperties.addProperty(CarbonCommonConstants.CARBON_SECURE_DICTIONARY_SERVER, "-1.1");
    valueBeforeValidation =
            carbonProperties.getProperty(CarbonCommonConstants.CARBON_SECURE_DICTIONARY_SERVER);
    validateMethodType.invoke(carbonProperties, CarbonCommonConstants.CARBON_SECURE_DICTIONARY_SERVER);
    valueAfterValidation =
            carbonProperties.getProperty(CarbonCommonConstants.CARBON_SECURE_DICTIONARY_SERVER);
    assertEquals(valueBeforeValidation, valueAfterValidation);
    assertTrue(
            CarbonCommonConstants.CARBON_SECURE_DICTIONARY_SERVER_DEFAULT.equalsIgnoreCase(valueAfterValidation));
    carbonProperties.addProperty(CarbonCommonConstants.ENABLE_CALCULATE_SIZE, "enableCalculateSize");
    valueBeforeValidation =
            carbonProperties.getProperty(CarbonCommonConstants.ENABLE_CALCULATE_SIZE);
    validateMethodType.invoke(carbonProperties, CarbonCommonConstants.ENABLE_CALCULATE_SIZE);
    valueAfterValidation =
            carbonProperties.getProperty(CarbonCommonConstants.ENABLE_CALCULATE_SIZE);
    assertEquals(valueBeforeValidation, valueAfterValidation);
    assertTrue(
            CarbonCommonConstants.DEFAULT_ENABLE_CALCULATE_SIZE.equalsIgnoreCase(valueAfterValidation));
    carbonProperties.addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "carbonMergeIndexInSegment");
    valueBeforeValidation =
            carbonProperties.getProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT);
    validateMethodType.invoke(carbonProperties, CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT);
    valueAfterValidation =
            carbonProperties.getProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT);
    assertEquals(valueBeforeValidation, valueAfterValidation);
    assertTrue(
            CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT_DEFAULT.equalsIgnoreCase(valueAfterValidation));
    carbonProperties.addProperty(CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE, "1.1");
    valueBeforeValidation =
            carbonProperties.getProperty(CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE);
    validateMethodType.invoke(carbonProperties, CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE);
    valueAfterValidation =
            carbonProperties.getProperty(CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE);
    assertEquals(valueBeforeValidation, valueAfterValidation);
    assertTrue(
            CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE_DEFAULT.equalsIgnoreCase(valueAfterValidation));
    carbonProperties.addProperty(CarbonCommonConstants.CARBON_MERGE_SORT_PREFETCH, "-2.2");
    valueBeforeValidation =
            carbonProperties.getProperty(CarbonCommonConstants.CARBON_MERGE_SORT_PREFETCH);
    validateMethodType.invoke(carbonProperties, CarbonCommonConstants.CARBON_MERGE_SORT_PREFETCH);
    valueAfterValidation =
            carbonProperties.getProperty(CarbonCommonConstants.CARBON_MERGE_SORT_PREFETCH);
    assertEquals(valueBeforeValidation, valueAfterValidation);
    assertTrue(
            CarbonCommonConstants.CARBON_MERGE_SORT_PREFETCH_DEFAULT.equalsIgnoreCase(valueAfterValidation));
    carbonProperties.addProperty(CarbonCommonConstants.ENABLE_CONCURRENT_COMPACTION, "enableConcurrentCompaction");
    valueBeforeValidation =
            carbonProperties.getProperty(CarbonCommonConstants.ENABLE_CONCURRENT_COMPACTION);
    validateMethodType.invoke(carbonProperties, CarbonCommonConstants.ENABLE_CONCURRENT_COMPACTION);
    valueAfterValidation =
            carbonProperties.getProperty(CarbonCommonConstants.ENABLE_CONCURRENT_COMPACTION);
    assertEquals(valueBeforeValidation, valueAfterValidation);
    assertTrue(
            CarbonCommonConstants.DEFAULT_ENABLE_CONCURRENT_COMPACTION.equalsIgnoreCase(valueAfterValidation));
    carbonProperties.addProperty(CarbonCommonConstants.CARBON_HORIZONTAL_COMPACTION_ENABLE, "carbonHorizontalCompactionEnable");
    valueBeforeValidation =
            carbonProperties.getProperty(CarbonCommonConstants.CARBON_HORIZONTAL_COMPACTION_ENABLE);
    validateMethodType.invoke(carbonProperties, CarbonCommonConstants.CARBON_HORIZONTAL_COMPACTION_ENABLE);
    valueAfterValidation =
            carbonProperties.getProperty(CarbonCommonConstants.CARBON_HORIZONTAL_COMPACTION_ENABLE);
    assertEquals(valueBeforeValidation, valueAfterValidation);
    assertTrue(
            CarbonCommonConstants.CARBON_HORIZONTAL_COMPACTION_ENABLE_DEFAULT.equalsIgnoreCase(valueAfterValidation));
    carbonProperties.addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE, "2.2");
    valueBeforeValidation =
            carbonProperties.getProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE);
    validateMethodType.invoke(carbonProperties, CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE);
    valueAfterValidation =
            carbonProperties.getProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE);
    assertEquals(valueBeforeValidation, valueAfterValidation);
    assertTrue(
            CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE_DEFAULT.equalsIgnoreCase(valueAfterValidation));
    carbonProperties.addProperty(CarbonCommonConstants.CARBON_LOADING_USE_YARN_LOCAL_DIR, "-3.3");
    valueBeforeValidation =
            carbonProperties.getProperty(CarbonCommonConstants.CARBON_LOADING_USE_YARN_LOCAL_DIR);
    validateMethodType.invoke(carbonProperties, CarbonCommonConstants.CARBON_LOADING_USE_YARN_LOCAL_DIR);
    valueAfterValidation =
            carbonProperties.getProperty(CarbonCommonConstants.CARBON_LOADING_USE_YARN_LOCAL_DIR);
    assertEquals(valueBeforeValidation, valueAfterValidation);
    assertTrue(
            CarbonCommonConstants.CARBON_LOADING_USE_YARN_LOCAL_DIR_DEFAULT.equalsIgnoreCase(valueAfterValidation));
    carbonProperties.addProperty(CarbonCommonConstants.CARBON_QUERY_MIN_MAX_ENABLED, "carbonQueryMinMaxEnabled");
    valueBeforeValidation =
            carbonProperties.getProperty(CarbonCommonConstants.CARBON_QUERY_MIN_MAX_ENABLED);
    validateMethodType.invoke(carbonProperties, CarbonCommonConstants.CARBON_QUERY_MIN_MAX_ENABLED);
    valueAfterValidation =
            carbonProperties.getProperty(CarbonCommonConstants.CARBON_QUERY_MIN_MAX_ENABLED);
    assertEquals(valueBeforeValidation, valueAfterValidation);
    assertTrue(
            CarbonCommonConstants.MIN_MAX_DEFAULT_VALUE.equalsIgnoreCase(valueAfterValidation));
    carbonProperties.addProperty(CarbonCommonConstants.BITSET_PIPE_LINE, "bitsetPipeLine");
    valueBeforeValidation =
            carbonProperties.getProperty(CarbonCommonConstants.BITSET_PIPE_LINE);
    validateMethodType.invoke(carbonProperties, CarbonCommonConstants.BITSET_PIPE_LINE);
    valueAfterValidation =
            carbonProperties.getProperty(CarbonCommonConstants.BITSET_PIPE_LINE);
    assertEquals(valueBeforeValidation, valueAfterValidation);
    assertTrue(
            CarbonCommonConstants.BITSET_PIPE_LINE_DEFAULT.equalsIgnoreCase(valueAfterValidation));
    carbonProperties.addProperty(CarbonCommonConstants.CARBON_READ_PARTITION_HIVE_DIRECT, "3.3");
    valueBeforeValidation =
            carbonProperties.getProperty(CarbonCommonConstants.CARBON_READ_PARTITION_HIVE_DIRECT);
    validateMethodType.invoke(carbonProperties, CarbonCommonConstants.CARBON_READ_PARTITION_HIVE_DIRECT);
    valueAfterValidation =
            carbonProperties.getProperty(CarbonCommonConstants.CARBON_READ_PARTITION_HIVE_DIRECT);
    assertEquals(valueBeforeValidation, valueAfterValidation);
    assertTrue(
            CarbonCommonConstants.CARBON_READ_PARTITION_HIVE_DIRECT_DEFAULT.equalsIgnoreCase(valueAfterValidation));
    carbonProperties.addProperty(CarbonCommonConstants.CARBON_SHOW_DATAMAPS, "-4.4");
    valueBeforeValidation =
            carbonProperties.getProperty(CarbonCommonConstants.CARBON_SHOW_DATAMAPS);
    validateMethodType.invoke(carbonProperties, CarbonCommonConstants.CARBON_SHOW_DATAMAPS);
    valueAfterValidation =
            carbonProperties.getProperty(CarbonCommonConstants.CARBON_SHOW_DATAMAPS);
    assertEquals(valueBeforeValidation, valueAfterValidation);
    assertTrue(
            CarbonCommonConstants.CARBON_SHOW_DATAMAPS_DEFAULT.equalsIgnoreCase(valueAfterValidation));
    carbonProperties.addProperty(CarbonCommonConstants.ENABLE_HIVE_SCHEMA_META_STORE, "enableHiveSchemaMetaStore");
    valueBeforeValidation =
            carbonProperties.getProperty(CarbonCommonConstants.ENABLE_HIVE_SCHEMA_META_STORE);
    validateMethodType.invoke(carbonProperties, CarbonCommonConstants.ENABLE_HIVE_SCHEMA_META_STORE);
    valueAfterValidation =
            carbonProperties.getProperty(CarbonCommonConstants.ENABLE_HIVE_SCHEMA_META_STORE);
    assertEquals(valueBeforeValidation, valueAfterValidation);
    assertTrue(
            CarbonCommonConstants.ENABLE_HIVE_SCHEMA_META_STORE_DEFAULT.equalsIgnoreCase(valueAfterValidation));
    carbonProperties.addProperty(CarbonCommonConstants.CARBON_SKIP_EMPTY_LINE, "carbonSkipEmptyLine");
    valueBeforeValidation =
            carbonProperties.getProperty(CarbonCommonConstants.CARBON_SKIP_EMPTY_LINE);
    validateMethodType.invoke(carbonProperties, CarbonCommonConstants.CARBON_SKIP_EMPTY_LINE);
    valueAfterValidation =
            carbonProperties.getProperty(CarbonCommonConstants.CARBON_SKIP_EMPTY_LINE);
    assertEquals(valueBeforeValidation, valueAfterValidation);
    assertTrue(
            CarbonCommonConstants.CARBON_SKIP_EMPTY_LINE_DEFAULT.equalsIgnoreCase(valueAfterValidation));
    carbonProperties.addProperty(CarbonCommonConstants.ENABLE_DATA_LOADING_STATISTICS, "4.4");
    valueBeforeValidation =
            carbonProperties.getProperty(CarbonCommonConstants.ENABLE_DATA_LOADING_STATISTICS);
    validateMethodType.invoke(carbonProperties, CarbonCommonConstants.ENABLE_DATA_LOADING_STATISTICS);
    valueAfterValidation =
            carbonProperties.getProperty(CarbonCommonConstants.ENABLE_DATA_LOADING_STATISTICS);
    assertEquals(valueBeforeValidation, valueAfterValidation);
    assertTrue(
            CarbonCommonConstants.ENABLE_DATA_LOADING_STATISTICS_DEFAULT.equalsIgnoreCase(valueAfterValidation));
    carbonProperties.addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE, "-5.5");
    valueBeforeValidation =
            carbonProperties.getProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE);
    validateMethodType.invoke(carbonProperties, CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE);
    valueAfterValidation =
            carbonProperties.getProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE);
    assertEquals(valueBeforeValidation, valueAfterValidation);
    assertTrue(
            CarbonCommonConstants.DEFAULT_ENABLE_AUTO_LOAD_MERGE.equalsIgnoreCase(valueAfterValidation));
    carbonProperties.addProperty(CarbonCommonConstants.CARBON_INSERT_PERSIST_ENABLED, "carbonInsertPersistEnabled");
    valueBeforeValidation =
            carbonProperties.getProperty(CarbonCommonConstants.CARBON_INSERT_PERSIST_ENABLED);
    validateMethodType.invoke(carbonProperties, CarbonCommonConstants.CARBON_INSERT_PERSIST_ENABLED);
    valueAfterValidation =
            carbonProperties.getProperty(CarbonCommonConstants.CARBON_INSERT_PERSIST_ENABLED);
    assertEquals(valueBeforeValidation, valueAfterValidation);
    assertTrue(
            CarbonCommonConstants.CARBON_INSERT_PERSIST_ENABLED_DEFAULT.equalsIgnoreCase(valueAfterValidation));
    carbonProperties.addProperty(CarbonCommonConstants.ENABLE_INMEMORY_MERGE_SORT, "enableInmemoryMergeSort");
    valueBeforeValidation =
            carbonProperties.getProperty(CarbonCommonConstants.ENABLE_INMEMORY_MERGE_SORT);
    validateMethodType.invoke(carbonProperties, CarbonCommonConstants.ENABLE_INMEMORY_MERGE_SORT);
    valueAfterValidation =
            carbonProperties.getProperty(CarbonCommonConstants.ENABLE_INMEMORY_MERGE_SORT);
    assertEquals(valueBeforeValidation, valueAfterValidation);
    assertTrue(
            CarbonCommonConstants.ENABLE_INMEMORY_MERGE_SORT_DEFAULT.equalsIgnoreCase(valueAfterValidation));
    carbonProperties.addProperty(CarbonCommonConstants.ENABLE_DATA_LOADING_STATISTICS, "5.5");
    valueBeforeValidation =
            carbonProperties.getProperty(CarbonCommonConstants.ENABLE_DATA_LOADING_STATISTICS);
    validateMethodType.invoke(carbonProperties, CarbonCommonConstants.ENABLE_DATA_LOADING_STATISTICS);
    valueAfterValidation =
            carbonProperties.getProperty(CarbonCommonConstants.ENABLE_DATA_LOADING_STATISTICS);
    assertEquals(valueBeforeValidation, valueAfterValidation);
    assertTrue(
            CarbonCommonConstants.ENABLE_DATA_LOADING_STATISTICS_DEFAULT.equalsIgnoreCase(valueAfterValidation));
    carbonProperties.addProperty(CarbonCommonConstants.USE_PREFETCH_WHILE_LOADING, "-6.6");
    valueBeforeValidation =
            carbonProperties.getProperty(CarbonCommonConstants.USE_PREFETCH_WHILE_LOADING);
    validateMethodType.invoke(carbonProperties, CarbonCommonConstants.USE_PREFETCH_WHILE_LOADING);
    valueAfterValidation =
            carbonProperties.getProperty(CarbonCommonConstants.USE_PREFETCH_WHILE_LOADING);
    assertEquals(valueBeforeValidation, valueAfterValidation);
    assertTrue(
            CarbonCommonConstants.USE_PREFETCH_WHILE_LOADING_DEFAULT.equalsIgnoreCase(valueAfterValidation));
    carbonProperties.addProperty(CarbonCommonConstants.CARBON_ENABLE_PAGE_LEVEL_READER_IN_COMPACTION, "carbonEnablePageLevelReaderInCompaction");
    valueBeforeValidation =
            carbonProperties.getProperty(CarbonCommonConstants.CARBON_ENABLE_PAGE_LEVEL_READER_IN_COMPACTION);
    validateMethodType.invoke(carbonProperties, CarbonCommonConstants.CARBON_ENABLE_PAGE_LEVEL_READER_IN_COMPACTION);
    valueAfterValidation =
            carbonProperties.getProperty(CarbonCommonConstants.CARBON_ENABLE_PAGE_LEVEL_READER_IN_COMPACTION);
    assertEquals(valueBeforeValidation, valueAfterValidation);
    assertTrue(
            CarbonCommonConstants.CARBON_ENABLE_PAGE_LEVEL_READER_IN_COMPACTION_DEFAULT.equalsIgnoreCase(valueAfterValidation));
    carbonProperties.addProperty(CarbonCommonConstants.CARBON_COMPACTION_PREFETCH_ENABLE, "carbonCompactionPrefetchEnable");
    valueBeforeValidation =
            carbonProperties.getProperty(CarbonCommonConstants.CARBON_COMPACTION_PREFETCH_ENABLE);
    validateMethodType.invoke(carbonProperties, CarbonCommonConstants.CARBON_COMPACTION_PREFETCH_ENABLE);
    valueAfterValidation =
            carbonProperties.getProperty(CarbonCommonConstants.CARBON_COMPACTION_PREFETCH_ENABLE);
    assertEquals(valueBeforeValidation, valueAfterValidation);
    assertTrue(
            CarbonCommonConstants.CARBON_COMPACTION_PREFETCH_ENABLE_DEFAULT.equalsIgnoreCase(valueAfterValidation));
    carbonProperties.addProperty(CarbonCommonConstants.ENABLE_QUERY_STATISTICS, "6.6");
    valueBeforeValidation =
            carbonProperties.getProperty(CarbonCommonConstants.ENABLE_QUERY_STATISTICS);
    validateMethodType.invoke(carbonProperties, CarbonCommonConstants.ENABLE_QUERY_STATISTICS);
    valueAfterValidation =
            carbonProperties.getProperty(CarbonCommonConstants.ENABLE_QUERY_STATISTICS);
    assertEquals(valueBeforeValidation, valueAfterValidation);
    assertTrue(
            CarbonCommonConstants.ENABLE_QUERY_STATISTICS_DEFAULT.equalsIgnoreCase(valueAfterValidation));
    carbonProperties.addProperty(CarbonCommonConstants.IS_DRIVER_INSTANCE, "-7.7");
    valueBeforeValidation =
            carbonProperties.getProperty(CarbonCommonConstants.IS_DRIVER_INSTANCE);
    validateMethodType.invoke(carbonProperties, CarbonCommonConstants.IS_DRIVER_INSTANCE);
    valueAfterValidation =
            carbonProperties.getProperty(CarbonCommonConstants.IS_DRIVER_INSTANCE);
    assertEquals(valueBeforeValidation, valueAfterValidation);
    assertTrue(
            CarbonCommonConstants.IS_DRIVER_INSTANCE_DEFAULT.equalsIgnoreCase(valueAfterValidation));
    carbonProperties.addProperty(CarbonCommonConstants.ENABLE_UNSAFE_IN_QUERY_EXECUTION, "enableUnsafeInQueryExecution");
    valueBeforeValidation =
            carbonProperties.getProperty(CarbonCommonConstants.ENABLE_UNSAFE_IN_QUERY_EXECUTION);
    validateMethodType.invoke(carbonProperties, CarbonCommonConstants.ENABLE_UNSAFE_IN_QUERY_EXECUTION);
    valueAfterValidation =
            carbonProperties.getProperty(CarbonCommonConstants.ENABLE_UNSAFE_IN_QUERY_EXECUTION);
    assertEquals(valueBeforeValidation, valueAfterValidation);
    assertTrue(
            CarbonCommonConstants.ENABLE_UNSAFE_IN_QUERY_EXECUTION_DEFAULTVALUE.equalsIgnoreCase(valueAfterValidation));
    carbonProperties.addProperty(CarbonCommonConstants.CARBON_PUSH_ROW_FILTERS_FOR_VECTOR, "carbonPushRowFiltersForVector");
    valueBeforeValidation =
            carbonProperties.getProperty(CarbonCommonConstants.CARBON_PUSH_ROW_FILTERS_FOR_VECTOR);
    validateMethodType.invoke(carbonProperties, CarbonCommonConstants.CARBON_PUSH_ROW_FILTERS_FOR_VECTOR);
    valueAfterValidation =
            carbonProperties.getProperty(CarbonCommonConstants.CARBON_PUSH_ROW_FILTERS_FOR_VECTOR);
    assertEquals(valueBeforeValidation, valueAfterValidation);
    assertTrue(
            CarbonCommonConstants.CARBON_PUSH_ROW_FILTERS_FOR_VECTOR_DEFAULT.equalsIgnoreCase(valueAfterValidation));
    carbonProperties.addProperty(CarbonCommonConstants.IS_INTERNAL_LOAD_CALL, "7.7");
    valueBeforeValidation =
            carbonProperties.getProperty(CarbonCommonConstants.IS_INTERNAL_LOAD_CALL);
    validateMethodType.invoke(carbonProperties, CarbonCommonConstants.IS_INTERNAL_LOAD_CALL);
    valueAfterValidation =
            carbonProperties.getProperty(CarbonCommonConstants.IS_INTERNAL_LOAD_CALL);
    assertEquals(valueBeforeValidation, valueAfterValidation);
    assertTrue(
            CarbonCommonConstants.IS_INTERNAL_LOAD_CALL_DEFAULT.equalsIgnoreCase(valueAfterValidation));
    carbonProperties.addProperty(CarbonCommonConstants.USE_DISTRIBUTED_DATAMAP, "-8.8");
    valueBeforeValidation =
            carbonProperties.getProperty(CarbonCommonConstants.USE_DISTRIBUTED_DATAMAP);
    validateMethodType.invoke(carbonProperties, CarbonCommonConstants.USE_DISTRIBUTED_DATAMAP);
    valueAfterValidation =
            carbonProperties.getProperty(CarbonCommonConstants.USE_DISTRIBUTED_DATAMAP);
    assertEquals(valueBeforeValidation, valueAfterValidation);
    assertTrue(
            CarbonCommonConstants.USE_DISTRIBUTED_DATAMAP_DEFAULT.equalsIgnoreCase(valueAfterValidation));
    carbonProperties.addProperty(CarbonCommonConstants.SUPPORT_DIRECT_QUERY_ON_DATAMAP, "supportDirectQueryOnDataMap");
    valueBeforeValidation =
            carbonProperties.getProperty(CarbonCommonConstants.SUPPORT_DIRECT_QUERY_ON_DATAMAP);
    validateMethodType.invoke(carbonProperties, CarbonCommonConstants.SUPPORT_DIRECT_QUERY_ON_DATAMAP);
    valueAfterValidation =
            carbonProperties.getProperty(CarbonCommonConstants.SUPPORT_DIRECT_QUERY_ON_DATAMAP);
    assertEquals(valueBeforeValidation, valueAfterValidation);
    assertTrue(
            CarbonCommonConstants.SUPPORT_DIRECT_QUERY_ON_DATAMAP_DEFAULTVALUE.equalsIgnoreCase(valueAfterValidation));
    carbonProperties.addProperty(CarbonCommonConstants.CARBON_LUCENE_INDEX_STOP_WORDS, "supportDirectQueryOnDataMap");
    valueBeforeValidation =
            carbonProperties.getProperty(CarbonCommonConstants.CARBON_LUCENE_INDEX_STOP_WORDS);
    validateMethodType.invoke(carbonProperties, CarbonCommonConstants.CARBON_LUCENE_INDEX_STOP_WORDS);
    valueAfterValidation =
            carbonProperties.getProperty(CarbonCommonConstants.CARBON_LUCENE_INDEX_STOP_WORDS);
    assertEquals(valueBeforeValidation, valueAfterValidation);
    assertTrue(
            CarbonCommonConstants.CARBON_LUCENE_INDEX_STOP_WORDS_DEFAULT.equalsIgnoreCase(valueAfterValidation));
    carbonProperties.addProperty(CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORDS_LOGGER_ENABLE, "8.8");
    valueBeforeValidation =
            carbonProperties.getProperty(CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORDS_LOGGER_ENABLE);
    validateMethodType.invoke(carbonProperties, CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORDS_LOGGER_ENABLE);
    valueAfterValidation =
            carbonProperties.getProperty(CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORDS_LOGGER_ENABLE);
    assertEquals(valueBeforeValidation, valueAfterValidation);
    assertTrue(
            CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORDS_LOGGER_ENABLE_DEFAULT.equalsIgnoreCase(valueAfterValidation));
    carbonProperties.addProperty(CarbonLoadOptionConstants.CARBON_OPTIONS_IS_EMPTY_DATA_BAD_RECORD, "-9.9");
    valueBeforeValidation =
            carbonProperties.getProperty(CarbonLoadOptionConstants.CARBON_OPTIONS_IS_EMPTY_DATA_BAD_RECORD);
    validateMethodType.invoke(carbonProperties, CarbonLoadOptionConstants.CARBON_OPTIONS_IS_EMPTY_DATA_BAD_RECORD);
    valueAfterValidation =
            carbonProperties.getProperty(CarbonLoadOptionConstants.CARBON_OPTIONS_IS_EMPTY_DATA_BAD_RECORD);
    assertEquals(valueBeforeValidation, valueAfterValidation);
    assertTrue(
            CarbonLoadOptionConstants.CARBON_OPTIONS_IS_EMPTY_DATA_BAD_RECORD_DEFAULT.equalsIgnoreCase(valueAfterValidation));
    carbonProperties.addProperty(CarbonLoadOptionConstants.CARBON_OPTIONS_SINGLE_PASS, "carbonOptionsSinglePass");
    valueBeforeValidation =
            carbonProperties.getProperty(CarbonLoadOptionConstants.CARBON_OPTIONS_SINGLE_PASS);
    validateMethodType.invoke(carbonProperties, CarbonLoadOptionConstants.CARBON_OPTIONS_SINGLE_PASS);
    valueAfterValidation =
            carbonProperties.getProperty(CarbonLoadOptionConstants.CARBON_OPTIONS_SINGLE_PASS);
    assertEquals(valueBeforeValidation, valueAfterValidation);
    assertTrue(
            CarbonLoadOptionConstants.CARBON_OPTIONS_SINGLE_PASS_DEFAULT.equalsIgnoreCase(valueAfterValidation));
    carbonProperties.addProperty(CarbonLoadOptionConstants.ENABLE_CARBON_LOAD_DIRECT_WRITE_TO_STORE_PATH, "enableCarbonLoadDirectWriteToStorePath");
    valueBeforeValidation =
            carbonProperties.getProperty(CarbonLoadOptionConstants.ENABLE_CARBON_LOAD_DIRECT_WRITE_TO_STORE_PATH);
    validateMethodType.invoke(carbonProperties, CarbonLoadOptionConstants.ENABLE_CARBON_LOAD_DIRECT_WRITE_TO_STORE_PATH);
    valueAfterValidation =
            carbonProperties.getProperty(CarbonLoadOptionConstants.ENABLE_CARBON_LOAD_DIRECT_WRITE_TO_STORE_PATH);
    assertEquals(valueBeforeValidation, valueAfterValidation);
    assertTrue(
            CarbonLoadOptionConstants.ENABLE_CARBON_LOAD_DIRECT_WRITE_TO_STORE_PATH_DEFAULT.equalsIgnoreCase(valueAfterValidation));
  }
}
