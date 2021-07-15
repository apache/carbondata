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
package org.apache.carbondata.processing.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.carbon.CarbonDataLoadSchema;
import org.apache.carbondata.core.carbon.metadata.datatype.DataType;
import org.apache.carbondata.core.carbon.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.apache.carbondata.core.datastorage.store.filesystem.LocalCarbonFile;
import org.apache.carbondata.core.load.LoadMetadataDetails;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.processing.datatypes.GenericDataType;
import org.apache.carbondata.processing.etl.DataLoadingException;
import org.apache.carbondata.processing.newflow.DataField;
import org.apache.carbondata.processing.sortandgroupby.exception.CarbonSortKeyAndGroupByException;

import mockit.Mock;
import mockit.MockUp;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.TestCase.assertNotNull;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.pentaho.di.core.util.Assert.assertNull;

public class CarbonDataProcessorUtilTest {

  private static CarbonDataLoadSchema carbonDataLoadSchema;
  private static CarbonTable carbonTable;
  private static CarbonDimension carbonDimension;
  private static ColumnSchema columnSchema;
  private static CarbonMeasure carbonMeasure;
  private static DataField dataField;
  private static CarbonColumn carbonColumn;
  private static CarbonFile carbonFile;
  private static LoadMetadataDetails loadMetadataDetails;
  private static List<LoadMetadataDetails> loadMetadataDetailsList;
  private static DataField[] dataFields;
  private static List<String> stringList;
  private static List<CarbonDimension> carbonDimensionList;
  private static List<CarbonMeasure> carbonMeasures;

  @BeforeClass public static void setUp() {
    carbonTable = new CarbonTable();
    columnSchema = new ColumnSchema();
    carbonFile = new LocalCarbonFile("testFile");
    loadMetadataDetails = new LoadMetadataDetails();
    carbonMeasure = new CarbonMeasure(columnSchema, 1);
    carbonDataLoadSchema = new CarbonDataLoadSchema(carbonTable);
    loadMetadataDetailsList = new ArrayList<LoadMetadataDetails>();
    loadMetadataDetailsList.add(loadMetadataDetails);

    new MockUp<CarbonDimension>() {
      @Mock public String getColName() {
        return "header";
      }
    };
    new MockUp<CarbonMeasure>() {
      @Mock public String getColName() {
        return "header";
      }
    };
    new MockUp<CarbonDataLoadSchema.DimensionRelation>() {
      @Mock public List<String> getColumns() {
        stringList = new ArrayList<>();
        stringList.add("header");
        stringList.add("colName");
        return stringList;
      }
    };
    new MockUp<File>() {
      @Mock public boolean isDirectory() {
        return true;
      }
    };
    carbonColumn = new CarbonColumn(columnSchema, 1, 2);
    dataField = new DataField(carbonColumn);
    dataFields = new DataField[] { dataField };

  }

  @Test public void testIsHeaderValid() throws DataLoadingException {
    new MockUp<CarbonDataLoadSchema>() {
      @Mock public CarbonTable getCarbonTable() {
        return carbonTable;
      }
    };
    new MockUp<CarbonTable>() {
      @Mock public String getFactTableName() {
        return "carbonTable";
      }
    };
    new MockUp<CarbonTable>() {
      @Mock public List<CarbonDimension> getDimensionByTableName(String tableName) {
        columnSchema = new ColumnSchema();
        carbonDimension = new CarbonDimension(columnSchema, 1, 2, 3, 4);
        carbonDimensionList = new ArrayList<CarbonDimension>();
        carbonDimensionList.add(carbonDimension);
        return carbonDimensionList;
      }
    };
    new MockUp<CarbonTable>() {
      @Mock public List<CarbonMeasure> getMeasureByTableName(String tableName) {
        carbonMeasures = new ArrayList<CarbonMeasure>();
        carbonMeasures.add(carbonMeasure);
        return carbonMeasures;
      }
    };

    assertTrue(
        CarbonDataProcessorUtil.isHeaderValid("tableName", "header", carbonDataLoadSchema, ","));
  }

  @Test public void renameBadRecordsFromInProgressToNormalForException() throws IOException {
    new MockUp<File>() {

      @Mock public boolean exists() throws IOException {
        throw new IOException();
      }

    };
    CarbonDataProcessorUtil.renameBadRecordsFromInProgressToNormal("temp location");
  }

  @Test public void testGetDataFormat() {
    Map<String, String> expectedDateFormatMap = new HashMap<>();
    expectedDateFormatMap.put("dateformat", "dateFormat");
    Map<String, String> actualDateFormatMap =
        CarbonDataProcessorUtil.getDateFormatMap("dateFormat:dateFormat");
    assertThat(expectedDateFormatMap, is(equalTo(actualDateFormatMap)));

  }

  @Test public void testGetColumnFields() {
    String expectedResult = "[header, header]";
    String actualResult =
        Arrays.toString(CarbonDataProcessorUtil.getColumnFields("header,header", ","));
    assertThat(expectedResult, is(equalTo(actualResult)));

  }
@Test public void testFetchTaskContext(){
Object object = new Object();
  CarbonDataProcessorUtil.configureTaskContext(object);
  assertNull(CarbonDataProcessorUtil.fetchTaskContext());
}

  @Test public void testGetFileHeaderForIOException() throws DataLoadingException {
    String expected = "Not able to read CSV input File ";
    new MockUp<File>() {

      @Mock boolean isInvalid() {
        return false;
      }

      @Mock public boolean exists() {
        return true;
      }
    };
    new MockUp<FileInputStream>() {

      @Mock private void open(String name) throws FileNotFoundException {

      }

    };
    try {

      CarbonDataProcessorUtil.getFileHeader(carbonFile);
    } catch (Exception exception) {
      String actual = exception.getMessage();
      assertThat(expected, is(equalTo(actual)));

    }
  }

  @Test public void testGetFileHeaderForFileNotFoundException() throws DataLoadingException {
    String expected = "CSV Input File not found ";
    new MockUp<File>() {

      @Mock boolean isInvalid() throws FileNotFoundException {
        throw new FileNotFoundException();
      }

      @Mock public boolean exists() {
        return true;
      }
    };
    new MockUp<FileInputStream>() {

      @Mock private void open(String name) throws FileNotFoundException {

      }

    };
    try {

      CarbonDataProcessorUtil.getFileHeader(carbonFile);
    } catch (Exception exception) {
      String actual = exception.getMessage();
      assertThat(expected, is(equalTo(actual)));

    }
  }

  @Test public void testGetCsvFileToRead() {
    new MockUp<LocalCarbonFile>() {

      @Mock boolean isDirectory() {
        return true;
      }

    };
    new MockUp<File>() {
      @Mock public File[] listFiles(FileFilter filter) {
        File fileName = new File("dummyFile.txt");
        File[] file = new File[] { fileName };
        return file;
      }

    };

    assertNotNull(CarbonDataProcessorUtil.getCsvFileToRead("csvFile"));
  }

  @Test public void testGetNoDictionaryMapping() {
    new MockUp<DataField>() {
      @Mock public CarbonColumn getColumn() {
        return carbonColumn;
      }
    };
    new MockUp<CarbonColumn>() {
      @Mock public Boolean isComplex() {
        return false;
      }

      @Mock public Boolean isDimesion() {
        return false;
      }

    };

    boolean[] expected = new boolean[0];
    carbonColumn = new CarbonColumn(columnSchema, 1, 2);
    dataField = new DataField(carbonColumn);
    dataFields = new DataField[] { dataField };
    boolean[] actual = CarbonDataProcessorUtil.getNoDictionaryMapping(dataFields);
    assertThat(expected, is(equalTo(actual)));

  }

  @Test public void testGetNoDictionaryMappingWithIsComplex() {
    boolean[] expected = new boolean[0];
    carbonColumn = new CarbonColumn(columnSchema, 1, 2);
    dataField = new DataField(carbonColumn);
    new MockUp<DataField>() {
      @Mock public CarbonColumn getColumn() {
        return carbonColumn;
      }
    };
    new MockUp<CarbonColumn>() {
      @Mock public Boolean isComplex() {
        return true;
      }
    };

    dataFields = new DataField[] { dataField };

    boolean[] actual = CarbonDataProcessorUtil.getNoDictionaryMapping(dataFields);
    assertThat(expected, is(equalTo(actual)));

  }

  @Test public void testGetNoDictionaryMappingWithIsComplexAndWithOutHasEncodingType() {
    boolean[] expected = new boolean[1];
    expected[0] = true;
    carbonColumn = new CarbonColumn(columnSchema, 1, 2);
    dataField = new DataField(carbonColumn);
    new MockUp<DataField>() {
      @Mock public CarbonColumn getColumn() {
        return carbonColumn;
      }

      @Mock public boolean hasDictionaryEncoding() {
        return false;
      }
    };
    new MockUp<CarbonColumn>() {
      @Mock public Boolean isComplex() {
        return false;
      }

      @Mock public Boolean isDimesion() {
        return true;
      }
    };

    dataFields = new DataField[] { dataField };

    boolean[] actual = CarbonDataProcessorUtil.getNoDictionaryMapping(dataFields);
    assertThat(expected, is(equalTo(actual)));

  }

  @Test public void testGetNoDictionaryMappingWithIsComplexAndWithHasEncodingType() {
    boolean[] expected = new boolean[1];
    expected[0] = false;
    carbonColumn = new CarbonColumn(columnSchema, 1, 2);
    dataField = new DataField(carbonColumn);
    new MockUp<DataField>() {
      @Mock public CarbonColumn getColumn() {
        return carbonColumn;
      }

      @Mock public boolean hasDictionaryEncoding() {
        return true;
      }
    };
    new MockUp<CarbonColumn>() {
      @Mock public Boolean isComplex() {
        return false;
      }

      @Mock public Boolean isDimesion() {
        return true;
      }
    };

    dataFields = new DataField[] { dataField };

    boolean[] actual = CarbonDataProcessorUtil.getNoDictionaryMapping(dataFields);
    assertThat(expected, is(equalTo(actual)));

  }

  @Test public void testGetIsUseInvertedIndex() {
    boolean[] expected = new boolean[1];
    expected[0] = true;
    dataField = new DataField(carbonColumn);
    new MockUp<DataField>() {
      @Mock public CarbonColumn getColumn() {
        return carbonColumn;
      }

    };

    new MockUp<CarbonColumn>() {

      @Mock public Boolean isDimesion() {
        return true;
      }

      @Mock public Boolean isUseInvertedIndex() {
        return true;
      }
    };
    dataFields = new DataField[] { dataField };
    boolean[] actual = CarbonDataProcessorUtil.getIsUseInvertedIndex(dataFields);
    assertThat(expected, is(equalTo(actual)));
  }

  @Test public void testGetIsUseInvertedIndexIsFalse() {
    boolean[] expected = new boolean[1];
    expected[0] = false;
    dataField = new DataField(carbonColumn);
    new MockUp<DataField>() {
      @Mock public CarbonColumn getColumn() {
        return carbonColumn;
      }

    };
    new MockUp<CarbonColumn>() {
      @Mock public Boolean isUseInvertedIndex() {
        return false;
      }

      @Mock public Boolean isDimesion() {
        return true;
      }
    };
    dataFields = new DataField[] { dataField };
    boolean[] actual = CarbonDataProcessorUtil.getIsUseInvertedIndex(dataFields);
    assertThat(expected, is(equalTo(actual)));
  }

  @Test public void testGetLoadNameFromLoadMetaDataDetails() {
    String expected = "Segment_loadName";
    List<LoadMetadataDetails> loadMetadataDetailsList = new ArrayList<LoadMetadataDetails>();
    loadMetadataDetailsList.add(loadMetadataDetails);
    new MockUp<LoadMetadataDetails>() {
      @Mock public String getLoadName() {
        return "loadName";
      }
    };

    String actual =
        CarbonDataProcessorUtil.getLoadNameFromLoadMetaDataDetails(loadMetadataDetailsList);
    assertThat(expected, is(equalTo(actual)));

  }

  @Test public void testGetModificationOrDeletionTimesFromLoadMetadataDetails() {
    String expected = "ModificationOrdeletionTimesStamp";
    new MockUp<LoadMetadataDetails>() {
      @Mock public String getModificationOrdeletionTimesStamp() {
        return "ModificationOrdeletionTimesStamp";
      }
    };

    String actual = CarbonDataProcessorUtil
        .getModificationOrDeletionTimesFromLoadMetadataDetails(loadMetadataDetailsList);
    assertThat(expected, is(equalTo(actual)));

  }

  @Test public void testGetComplexTypeMap() {

    new MockUp<DataField>() {
      @Mock public CarbonColumn getColumn() {
        carbonColumn = new CarbonDimension(columnSchema, 1, 2, 3, 4);
        return carbonColumn;
      }

    };
    new MockUp<CarbonColumn>() {
      @Mock public DataType getDataType() {
        return DataType.ARRAY;
      }

    };

    Map<String, GenericDataType> actual = CarbonDataProcessorUtil.getComplexTypesMap(dataFields);
    assertNotNull(actual);

  }

  @Test public void testGetComplexTypeMapForNUll() {
    Map<String, GenericDataType> expected = new LinkedHashMap<>();
    new MockUp<DataField>() {
      @Mock public CarbonColumn getColumn() {
        carbonColumn = new CarbonDimension(columnSchema, 1, 2, 3, 4);
        return carbonColumn;
      }

    };
    new MockUp<CarbonColumn>() {
      @Mock public DataType getDataType() {
        return DataType.BOOLEAN;
      }

    };
    Map<String, GenericDataType> actual = CarbonDataProcessorUtil.getComplexTypesMap(dataFields);
    assertThat(expected, is(equalTo(actual)));

  }

  @Test public void testGetFileBufferSize() throws CarbonSortKeyAndGroupByException {
    int expected = 1024;
    new MockUp<CarbonProperties>() {
      @Mock public String getProperty(String key) {
        return "0";
      }

    };
    int actual = CarbonDataProcessorUtil.getFileBufferSize(1, CarbonProperties.getInstance(), 1);
    assertThat(expected, is(equalTo(actual)));
  }

}

