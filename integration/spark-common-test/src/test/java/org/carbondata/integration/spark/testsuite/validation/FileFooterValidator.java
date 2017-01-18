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
package org.apache.carbondata.integration.spark.testsuite.validation;

import org.apache.spark.sql.common.util.CarbonHiveContext;
import org.apache.carbondata.core.carbon.CarbonTableIdentifier;
import org.apache.carbondata.core.util.path.CarbonStorePath;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastorage.store.FileHolder;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.reader.CarbonFooterReader;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.format.BlockletIndex;
import org.apache.carbondata.format.BlockletInfo;
import org.apache.carbondata.format.DataChunk;
import org.apache.carbondata.format.Encoding;
import org.apache.carbondata.format.FileFooter;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class FileFooterValidator {

  private static FileFooter fileFooter;

  private static boolean setUpIsDone;

  @Before public void setUp() throws Exception {

    if (setUpIsDone) {
      return;
    }
    CarbonHiveContext.sql(
            "CREATE CUBE validatefooter DIMENSIONS (empno Integer, empname String,"
            + " designation String,"
            + " doj Timestamp, workgroupcategory Integer, workgroupcategoryname String, "
            + "deptno Integer, deptname String, projectcode Integer, projectjoindate Timestamp,"
            + " projectenddate Timestamp) MEASURES (attendance Integer,utilization Integer,"
            + "salary Integer) OPTIONS (PARTITIONER [PARTITION_COUNT=1])");
    CarbonHiveContext.sql(
            "LOAD DATA fact from './src/test/resources/data.csv' INTO CUBE validatefooter "
                + "PARTITIONDATA(DELIMITER ',', QUOTECHAR '\"')");
    String storePath =
        CarbonProperties.getInstance().getProperty(CarbonCommonConstants.STORE_LOCATION);
    CarbonTableIdentifier tableIdentifier =
            new CarbonTableIdentifier(CarbonCommonConstants.DATABASE_DEFAULT_NAME, "validatefooter", "1");
    String segmentPath = CarbonStorePath.getCarbonTablePath(storePath, tableIdentifier)
        .getCarbonDataDirectoryPath("0", "0");
    CarbonFile carbonFile =
        FileFactory.getCarbonFile(segmentPath, FileFactory.getFileType(segmentPath));
    CarbonFile[] list = carbonFile.listFiles(new CarbonFileFilter() {
      @Override public boolean accept(CarbonFile file) {
        if (file.getName().endsWith(CarbonCommonConstants.FACT_FILE_EXT)) {
          return true;
        }
        return false;
      }
    });

    for (CarbonFile file : list) {
      String fileLocation = file.getAbsolutePath();
      CarbonFile factFile =
          FileFactory.getCarbonFile(fileLocation, FileFactory.getFileType(fileLocation));
      long offset = factFile.getSize() - CarbonCommonConstants.LONG_SIZE_IN_BYTE;
      FileHolder fileHolder = FileFactory.getFileHolder(FileFactory.getFileType(fileLocation));
      offset = fileHolder.readLong(fileLocation, offset);
      CarbonFooterReader metaDataReader = new CarbonFooterReader(fileLocation, offset);
      fileFooter = metaDataReader.readFooter();
    }
    setUpIsDone = true;
  }

  @AfterClass public static void tearDownAfterClass() {
    CarbonHiveContext.sql("drop CUBE validatefooter");
  }

  @Test public void testFileFooterExist() {
    assertTrue(fileFooter != null);
  }

  @Test public void testFileFooterVersion() {
    assertTrue(fileFooter.getVersion() >= 0);
  }

  @Test public void testFileFooterNumRows() {
    assertTrue(fileFooter.getNum_rows() > 0);
  }

  @Test public void testFileFooterTableColumns() {
    assertTrue(fileFooter.getTable_columns() != null && fileFooter.getTable_columns().size() > 0);
  }

  @Test public void testFileFooterSegmentInfo() {
    assertTrue(
        fileFooter.getSegment_info() != null && fileFooter.getSegment_info().getNum_cols() > 0
            && fileFooter.getSegment_info().getColumn_cardinalities().size() > 0);
  }

  @Test public void testFileFooterBlockletIndex() {
    assertTrue(fileFooter.getBlocklet_index_list() != null
        && fileFooter.getBlocklet_index_list().size() > 0);
    for (BlockletIndex blockletIndex : fileFooter.getBlocklet_index_list()) {
      assertTrue(blockletIndex.getMin_max_index().getMin_values() != null
          && blockletIndex.getMin_max_index().getMin_values().size() > 0
          && blockletIndex.getMin_max_index().getMax_values() != null
          && blockletIndex.getMin_max_index().getMax_values().size() > 0
          && blockletIndex.getMin_max_index().getMin_values().size() == blockletIndex
          .getMin_max_index().getMax_values().size());
      assertTrue(blockletIndex.getB_tree_index().getStart_key() != null
          && blockletIndex.getB_tree_index().getEnd_key() != null);
    }
  }

  @Test public void testFileFooterBlockletInfo() {
    assertTrue(fileFooter.getBlocklet_info_list() != null
        && fileFooter.getBlocklet_info_list().size() > 0);
    for (BlockletInfo blockletInfo : fileFooter.getBlocklet_info_list()) {
      assertTrue(blockletInfo.getNum_rows() > 0 && blockletInfo.getColumn_data_chunks() != null
          && blockletInfo.getColumn_data_chunks().size() > 0);
      for (DataChunk columnDataChunk : blockletInfo.getColumn_data_chunks()) {
        testColumnDataChunk(columnDataChunk);
      }
    }
  }

  private void testColumnDataChunk(DataChunk columnDatachunk) {
    assertTrue(columnDatachunk.getEncoders() != null && columnDatachunk.getChunk_meta() != null
        && columnDatachunk.getChunk_meta().getCompression_codec() != null);
    // For Measure
    if (columnDatachunk.getEncoders().contains(Encoding.DELTA)) {
      assertTrue(
          columnDatachunk.getPresence() != null && columnDatachunk.getEncoder_meta() != null);
    } else {
      assertTrue(columnDatachunk.getSort_state() != null);
    }
  }
}