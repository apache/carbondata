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
package org.apache.carbondata.processing.newflow;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.core.carbon.CarbonDataLoadSchema;
import org.apache.carbondata.core.carbon.metadata.CarbonMetadata;
import org.apache.carbondata.core.carbon.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.apache.carbondata.core.datastorage.store.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastorage.store.filesystem.LocalCarbonFile;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.processing.model.CarbonLoadModel;
import org.apache.carbondata.processing.newflow.constants.DataLoadProcessorConstants;
import org.apache.carbondata.processing.newflow.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;
import org.apache.carbondata.scan.result.BatchResult;

import mockit.Mock;
import mockit.MockUp;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DataLoadProcessBuilderTest {

  DataLoadProcessBuilder dataLoadProcessBuilder = new DataLoadProcessBuilder();
  static CarbonLoadModel carbonLoadModel = null;
  static CarbonTable carbonTable = null;
  static List<CarbonDimension> carbonDimensions = null;
  static CarbonIterator carbonIterator = null;
  static CarbonIterator[] carbonIterators = null;

  @BeforeClass public static void setUp() {

    carbonLoadModel = new CarbonLoadModel();
    carbonTable = new CarbonTable();
    carbonLoadModel.setCarbonDataLoadSchema(new CarbonDataLoadSchema(carbonTable));
    carbonDimensions = new ArrayList<>();
    carbonDimensions.add(new CarbonDimension(new ColumnSchema(), 1, 1, 1, 1));
    carbonIterator = new BatchResult();
    carbonIterators = new CarbonIterator[] { carbonIterator };
  }

  @Test public void buildTest() throws Exception {

    new MockUp<CarbonDataProcessorUtil>() {
      @SuppressWarnings("unused") @Mock public CarbonFile getCsvFileToRead(String csvFilePath) {
        return new LocalCarbonFile("/path/file");
      }

      @Mock public String getFileHeader(CarbonFile csvFile) {
        return "header";
      }

      @Mock public String[] getColumnFields(String header, String delimiter) {
        String[] columns = { "col1", "col2" };
        return columns;
      }

      @Mock
      public boolean isHeaderValid(String tableName, String header, CarbonDataLoadSchema schema,
          String delimiter) {
        return true;
      }
    };

    new MockUp<File>() {

      @Mock public boolean mkdirs() {
        return true;
      }
    };

    new MockUp<CarbonLoadModel>() {
      @Mock public String getStorePath() {
        return "/store/path";
      }

      @Mock public List<String> getFactFilesToProcess() {
        List<String> factFiles = new ArrayList();
        factFiles.add("file1");
        return factFiles;
      }

      @Mock public String getSerializationNullFormat() {
        return "value1,value2,value3";
      }

      @Mock public String getBadRecordsLoggerEnable() {
        return "badRecord1,badRecord2";
      }

      @Mock public String getBadRecordsAction() {
        return "writeRecordInFile,writeRecordInFile";
      }

      @Mock public String getCsvHeader() {
        return "header";
      }
    };
    new MockUp<CarbonMetadata>() {
      @Mock public CarbonTable getCarbonTable(String tableUniqueName) {
        return carbonTable;
      }
    };

    new MockUp<CarbonTable>() {
      @Mock public List<CarbonDimension> getDimensionByTableName(String tableName) {
        return carbonDimensions;
      }

      @Mock public String getFactTableName() {
        return "tableName";
      }

      @Mock public List<CarbonMeasure> getMeasureByTableName(String tableName) {
        List<CarbonMeasure> carbonMeasures = new ArrayList<>();
        carbonMeasures.add(new CarbonMeasure(new ColumnSchema(), 1));
        return carbonMeasures;
      }
    };

    new MockUp<CarbonColumn>() {
      @Mock public String getColName() {
        return "columnName";
      }
    };
    AbstractDataLoadProcessorStep actualResult =
        dataLoadProcessBuilder.build(carbonLoadModel, "/storeLocation", carbonIterators);
    int datafieldLen = 2;
    String propertyValue = "value2";
    assertEquals(propertyValue, actualResult.configuration
        .getDataLoadProperty(DataLoadProcessorConstants.SERIALIZATION_NULL_FORMAT));
    assertEquals(datafieldLen, actualResult.configuration.getDataFields().length);
  }

  @Test public void buildTestWhenCsvHeaderIsNull() throws Exception {

    new MockUp<CarbonDataProcessorUtil>() {
      @SuppressWarnings("unused") @Mock public CarbonFile getCsvFileToRead(String csvFilePath) {
        return new LocalCarbonFile("/path/file");
      }

      @Mock public String getFileHeader(CarbonFile csvFile) {
        return "header";
      }

      @Mock public String[] getColumnFields(String header, String delimiter) {
        String[] columns = { "col1", "col2" };
        return columns;
      }

      @Mock
      public boolean isHeaderValid(String tableName, String header, CarbonDataLoadSchema schema,
          String delimiter) {
        return true;
      }
    };

    new MockUp<File>() {

      @Mock public boolean mkdirs() {
        return true;
      }
    };

    new MockUp<CarbonLoadModel>() {
      @Mock public String getStorePath() {
        return "/store/path";
      }

      @Mock public List<String> getFactFilesToProcess() {
        List<String> factFiles = new ArrayList();
        factFiles.add("file1");
        return factFiles;
      }

      @Mock public String getSerializationNullFormat() {
        return "value1,value2,value3";
      }

      @Mock public String getBadRecordsLoggerEnable() {
        return "badRecord1,badRecord2";
      }

      @Mock public String getBadRecordsAction() {
        return "writeRecordInFile,writeRecordInFile";
      }

    };

    new MockUp<CarbonMetadata>() {
      @Mock public CarbonTable getCarbonTable(String tableUniqueName) {
        return carbonTable;
      }
    };

    new MockUp<CarbonTable>() {
      @Mock public List<CarbonDimension> getDimensionByTableName(String tableName) {
        return carbonDimensions;
      }

      @Mock public String getFactTableName() {
        return "tableName";
      }

      @Mock public List<CarbonMeasure> getMeasureByTableName(String tableName) {
        List<CarbonMeasure> carbonMeasures = new ArrayList<>();
        carbonMeasures.add(new CarbonMeasure(new ColumnSchema(), 1));
        return carbonMeasures;
      }
    };

    new MockUp<CarbonColumn>() {
      @Mock public String getColName() {
        return "columnName";
      }
    };
    AbstractDataLoadProcessorStep actualResult =
        dataLoadProcessBuilder.build(carbonLoadModel, "/storeLocation", carbonIterators);
    int datafieldLen = 2;
    String propertyValue = "value2";
    assertEquals(propertyValue, actualResult.configuration
        .getDataLoadProperty(DataLoadProcessorConstants.SERIALIZATION_NULL_FORMAT));
    assertEquals(datafieldLen, actualResult.configuration.getDataFields().length);
  }

  @Test(expected = CarbonDataLoadingException.class)
  public void buildTestWhenCsvHeaderIsInvalidAndCsvFileNameIsNull() throws Exception {

    new MockUp<CarbonDataProcessorUtil>() {
      @SuppressWarnings("unused") @Mock public CarbonFile getCsvFileToRead(String csvFilePath) {
        return new LocalCarbonFile("/path/");
      }

      @Mock public String getFileHeader(CarbonFile csvFile) {
        return "header";
      }

      @Mock public String[] getColumnFields(String header, String delimiter) {
        String[] columns = { "col1", "col2" };
        return columns;
      }

      @Mock
      public boolean isHeaderValid(String tableName, String header, CarbonDataLoadSchema schema,
          String delimiter) {
        return false;
      }
    };

    new MockUp<File>() {

      @Mock public boolean mkdirs() {
        return true;
      }
    };

    new MockUp<CarbonLoadModel>() {
      @Mock public String getStorePath() {
        return "/store/path";
      }

      @Mock public List<String> getFactFilesToProcess() {
        List<String> factFiles = new ArrayList();
        factFiles.add("file1");
        return factFiles;
      }

      @Mock public String getSerializationNullFormat() {
        return "value1,value2,value3";
      }

      @Mock public String getBadRecordsLoggerEnable() {
        return "badRecord1,badRecord2";
      }

      @Mock public String getBadRecordsAction() {
        return "writeRecordInFile,writeRecordInFile";
      }

      @Mock public String getCsvHeader() {
        return "header";
      }
    };
    new MockUp<CarbonMetadata>() {
      @Mock public CarbonTable getCarbonTable(String tableUniqueName) {
        return carbonTable;
      }
    };

    new MockUp<CarbonTable>() {
      @Mock public List<CarbonDimension> getDimensionByTableName(String tableName) {
        return carbonDimensions;
      }

      @Mock public String getFactTableName() {
        return "tableName";
      }

      @Mock public List<CarbonMeasure> getMeasureByTableName(String tableName) {
        List<CarbonMeasure> carbonMeasures = new ArrayList<>();
        carbonMeasures.add(new CarbonMeasure(new ColumnSchema(), 1));
        return carbonMeasures;
      }
    };

    new MockUp<CarbonColumn>() {
      @Mock public String getColName() {
        return "columnName";
      }
    };

    dataLoadProcessBuilder.build(carbonLoadModel, "/storeLocation", carbonIterators);
  }

  @Test(expected = CarbonDataLoadingException.class)
  public void buildTestWhenCsvHeaderIsInvalidAndCsvFileNameIsNotNull() throws Exception {

    new MockUp<CarbonDataProcessorUtil>() {
      @SuppressWarnings("unused") @Mock public CarbonFile getCsvFileToRead(String csvFilePath) {
        return new LocalCarbonFile("/path/fileName");
      }

      @Mock public String getFileHeader(CarbonFile csvFile) {
        return "header";
      }

      @Mock public String[] getColumnFields(String header, String delimiter) {
        String[] columns = { "col1", "col2" };
        return columns;
      }

      @Mock
      public boolean isHeaderValid(String tableName, String header, CarbonDataLoadSchema schema,
          String delimiter) {
        return false;
      }
    };

    new MockUp<File>() {

      @Mock public boolean mkdirs() {
        return true;
      }
    };

    new MockUp<CarbonLoadModel>() {
      @Mock public String getStorePath() {
        return "/store/path";
      }

      @Mock public List<String> getFactFilesToProcess() {
        List<String> factFiles = new ArrayList();
        factFiles.add("file1");
        return factFiles;
      }

      @Mock public String getSerializationNullFormat() {
        return "value1,value2,value3";
      }

      @Mock public String getBadRecordsLoggerEnable() {
        return "badRecord1,badRecord2";
      }

      @Mock public String getBadRecordsAction() {
        return "writeRecordInFile,writeRecordInFile";
      }

      @Mock public String getCsvHeader() {
        return null;
      }
    };
    new MockUp<CarbonMetadata>() {
      @Mock public CarbonTable getCarbonTable(String tableUniqueName) {
        return carbonTable;
      }
    };

    new MockUp<CarbonTable>() {
      @Mock public List<CarbonDimension> getDimensionByTableName(String tableName) {
        return carbonDimensions;
      }

      @Mock public String getFactTableName() {
        return "tableName";
      }

      @Mock public List<CarbonMeasure> getMeasureByTableName(String tableName) {
        List<CarbonMeasure> carbonMeasures = new ArrayList<>();
        carbonMeasures.add(new CarbonMeasure(new ColumnSchema(), 1));
        return carbonMeasures;
      }
    };

    new MockUp<CarbonColumn>() {
      @Mock public String getColName() {
        return "columnName";
      }
    };
    dataLoadProcessBuilder.build(carbonLoadModel, "/storeLocation", carbonIterators);
  }

  @Test public void buildTestWhenCarbonTableOfCarbonMetadataIsNull() throws Exception {

    new MockUp<CarbonDataProcessorUtil>() {
      @SuppressWarnings("unused") @Mock public CarbonFile getCsvFileToRead(String csvFilePath) {
        return new LocalCarbonFile("/path/file");
      }

      @Mock public String getFileHeader(CarbonFile csvFile) {
        return "header";
      }

      @Mock public String[] getColumnFields(String header, String delimiter) {
        String[] columns = { "col1", "col2" };
        return columns;
      }

      @Mock
      public boolean isHeaderValid(String tableName, String header, CarbonDataLoadSchema schema,
          String delimiter) {
        return true;
      }
    };

    new MockUp<File>() {

      @Mock public boolean mkdirs() {
        return true;
      }
    };

    new MockUp<CarbonLoadModel>() {
      @Mock public String getStorePath() {
        return "/store/path";
      }

      @Mock public List<String> getFactFilesToProcess() {
        List<String> factFiles = new ArrayList();
        factFiles.add("file1");
        return factFiles;
      }

      @Mock public String getSerializationNullFormat() {
        return "value1,value2,value3";
      }

      @Mock public String getBadRecordsLoggerEnable() {
        return "badRecord1,badRecord2";
      }

      @Mock public String getBadRecordsAction() {
        return "writeRecordInFile,writeRecordInFile";
      }

      @Mock public String getCsvHeader() {
        return "header";
      }
    };
    new MockUp<CarbonMetadata>() {
      @Mock public CarbonTable getCarbonTable(String tableUniqueName) {
        return null;
      }
    };

    new MockUp<CarbonTable>() {
      @Mock public List<CarbonDimension> getDimensionByTableName(String tableName) {
        return carbonDimensions;
      }

      @Mock public String getFactTableName() {
        return "tableName";
      }

      @Mock public List<CarbonMeasure> getMeasureByTableName(String tableName) {
        List<CarbonMeasure> carbonMeasures = new ArrayList<>();
        carbonMeasures.add(new CarbonMeasure(new ColumnSchema(), 1));
        return carbonMeasures;
      }

      @Mock public String getTableUniqueName() {
        return "tableName";
      }
    };

    new MockUp<CarbonColumn>() {
      @Mock public String getColName() {
        return "columnName";
      }
    };
    AbstractDataLoadProcessorStep actualResult =
        dataLoadProcessBuilder.build(carbonLoadModel, "/storeLocation", carbonIterators);
    int datafieldLen = 2;
    String propertyValue = "value2";
    assertEquals(propertyValue, actualResult.configuration
        .getDataLoadProperty(DataLoadProcessorConstants.SERIALIZATION_NULL_FORMAT));
    assertEquals(datafieldLen, actualResult.configuration.getDataFields().length);
  }

  @Test public void buildTestWhenColumnIsOfComplexType() throws Exception {

    new MockUp<CarbonDataProcessorUtil>() {
      @SuppressWarnings("unused") @Mock public CarbonFile getCsvFileToRead(String csvFilePath) {
        return new LocalCarbonFile("/path/file");
      }

      @Mock public String getFileHeader(CarbonFile csvFile) {
        return "header";
      }

      @Mock public String[] getColumnFields(String header, String delimiter) {
        String[] columns = { "col1", "col2" };
        return columns;
      }

      @Mock
      public boolean isHeaderValid(String tableName, String header, CarbonDataLoadSchema schema,
          String delimiter) {
        return true;
      }
    };

    new MockUp<File>() {

      @Mock public boolean mkdirs() {
        return true;
      }
    };

    new MockUp<CarbonLoadModel>() {
      @Mock public String getStorePath() {
        return "/store/path";
      }

      @Mock public List<String> getFactFilesToProcess() {
        List<String> factFiles = new ArrayList();
        factFiles.add("file1");
        return factFiles;
      }

      @Mock public String getSerializationNullFormat() {
        return "value1,value2,value3";
      }

      @Mock public String getBadRecordsLoggerEnable() {
        return "badRecord1,badRecord2";
      }

      @Mock public String getBadRecordsAction() {
        return "writeRecordInFile,writeRecordInFile";
      }

      @Mock public String getCsvHeader() {
        return "header";
      }
    };
    new MockUp<CarbonMetadata>() {
      @Mock public CarbonTable getCarbonTable(String tableUniqueName) {
        return carbonTable;
      }
    };

    new MockUp<CarbonTable>() {
      @Mock public List<CarbonDimension> getDimensionByTableName(String tableName) {
        return carbonDimensions;
      }

      @Mock public String getFactTableName() {
        return "tableName";
      }

      @Mock public List<CarbonMeasure> getMeasureByTableName(String tableName) {
        List<CarbonMeasure> carbonMeasures = new ArrayList<>();
        carbonMeasures.add(new CarbonMeasure(new ColumnSchema(), 1));
        return carbonMeasures;
      }
    };

    new MockUp<CarbonColumn>() {
      @Mock public String getColName() {
        return "columnName";
      }

      @Mock public Boolean isComplex() {
        return true;
      }
    };
    AbstractDataLoadProcessorStep actualResult =
        dataLoadProcessBuilder.build(carbonLoadModel, "/storeLocation", carbonIterators);
    int datafieldLen = 2;
    String propertyValue = "value2";
    assertEquals(propertyValue, actualResult.configuration
        .getDataLoadProperty(DataLoadProcessorConstants.SERIALIZATION_NULL_FORMAT));
    assertEquals(datafieldLen, actualResult.configuration.getDataFields().length);
  }

  @Test public void buildTestWhenDirectoryCouldNotBeCreated() throws Exception {

    new MockUp<CarbonDataProcessorUtil>() {
      @SuppressWarnings("unused") @Mock public CarbonFile getCsvFileToRead(String csvFilePath) {
        return new LocalCarbonFile("/path/file");
      }

      @Mock public String getFileHeader(CarbonFile csvFile) {
        return "header";
      }

      @Mock public String[] getColumnFields(String header, String delimiter) {
        String[] columns = { "col1", "col2" };
        return columns;
      }

      @Mock
      public boolean isHeaderValid(String tableName, String header, CarbonDataLoadSchema schema,
          String delimiter) {
        return true;
      }
    };

    new MockUp<File>() {

      @Mock public boolean mkdirs() {
        return false;
      }
    };

    new MockUp<CarbonLoadModel>() {
      @Mock public String getStorePath() {
        return "/store/path";
      }

      @Mock public List<String> getFactFilesToProcess() {
        List<String> factFiles = new ArrayList();
        factFiles.add("file1");
        return factFiles;
      }

      @Mock public String getSerializationNullFormat() {
        return "value1,value2,value3";
      }

      @Mock public String getBadRecordsLoggerEnable() {
        return "badRecord1,badRecord2";
      }

      @Mock public String getBadRecordsAction() {
        return "writeRecordInFile,writeRecordInFile";
      }

      @Mock public String getCsvHeader() {
        return "header";
      }
    };
    new MockUp<CarbonMetadata>() {
      @Mock public CarbonTable getCarbonTable(String tableUniqueName) {
        return carbonTable;
      }
    };

    new MockUp<CarbonTable>() {
      @Mock public List<CarbonDimension> getDimensionByTableName(String tableName) {
        return carbonDimensions;
      }

      @Mock public String getFactTableName() {
        return "tableName";
      }

      @Mock public List<CarbonMeasure> getMeasureByTableName(String tableName) {
        List<CarbonMeasure> carbonMeasures = new ArrayList<>();
        carbonMeasures.add(new CarbonMeasure(new ColumnSchema(), 1));
        return carbonMeasures;
      }
    };

    new MockUp<CarbonColumn>() {
      @Mock public String getColName() {
        return "columnName";
      }
    };
    AbstractDataLoadProcessorStep actualResult =
        dataLoadProcessBuilder.build(carbonLoadModel, "/storeLocation", carbonIterators);
    int datafieldLen = 2;
    String propertyValue = "value2";
    assertEquals(propertyValue, actualResult.configuration
        .getDataLoadProperty(DataLoadProcessorConstants.SERIALIZATION_NULL_FORMAT));
    assertEquals(datafieldLen, actualResult.configuration.getDataFields().length);
  }

}

