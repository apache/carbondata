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
package org.apache.carbondata.processing.newflow.steps;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.apache.carbondata.core.carbon.CarbonTableIdentifier;
import org.apache.carbondata.core.carbon.datastore.block.SegmentProperties;
import org.apache.carbondata.core.carbon.metadata.CarbonMetadata;
import org.apache.carbondata.core.carbon.metadata.datatype.DataType;
import org.apache.carbondata.core.carbon.metadata.encoder.Encoding;
import org.apache.carbondata.core.carbon.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.carbon.path.CarbonStorePath;
import org.apache.carbondata.core.carbon.path.CarbonTablePath;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.keygenerator.KeyGenerator;
import org.apache.carbondata.core.keygenerator.mdkey.MultiDimKeyVarLengthGenerator;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.processing.datatypes.ArrayDataType;
import org.apache.carbondata.processing.datatypes.GenericDataType;
import org.apache.carbondata.processing.datatypes.StructDataType;
import org.apache.carbondata.processing.newflow.AbstractDataLoadProcessorStep;
import org.apache.carbondata.processing.newflow.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.newflow.DataField;
import org.apache.carbondata.processing.newflow.constants.DataLoadProcessorConstants;
import org.apache.carbondata.processing.newflow.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.newflow.row.CarbonRow;
import org.apache.carbondata.processing.newflow.row.CarbonRowBatch;
import org.apache.carbondata.processing.store.CarbonFactDataHandlerColumnar;
import org.apache.carbondata.processing.store.CarbonFactDataHandlerModel;
import org.apache.carbondata.processing.store.CarbonFactHandler;
import org.apache.carbondata.processing.store.CarbonFactHandlerFactory;
import org.apache.carbondata.processing.store.writer.exception.CarbonDataWriterException;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

import mockit.Mock;
import mockit.MockUp;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNull;

public class DataWriterProcessorStepImplTest {

  private static CarbonTableIdentifier tIdentifier;
  private static CarbonDataLoadConfiguration config;
  private static CarbonIterator iterator[];
  private static DataField dataField, dataField2;
  private static ColumnSchema columnSchema = new ColumnSchema();
  private static ColumnSchema columnSchema2 = new ColumnSchema();
  private static AbstractDataLoadProcessorStep dataLoadProcessorStep;
  private static DataWriterProcessorStepImpl dataWriterProcessorStep;
  private static Object inputData[] = { "harsh", "sharma", 26 };
  private static Iterator<CarbonRowBatch>[] iterators = new Iterator[2];
  private static Iterator<CarbonRowBatch>[] resultIterators = new Iterator[2];
  private static CarbonRowBatch batch1 = new CarbonRowBatch();
  private static CarbonRowBatch batch2 = new CarbonRowBatch();
  private static CarbonFactDataHandlerModel carbonFactDataHandlerModel;

  @BeforeClass public static void setUp() {
    columnSchema.setColumnName("name");
    columnSchema.setDataType(DataType.ARRAY);
    columnSchema.setDimensionColumn(true);

    columnSchema2.setColumnName("name");
    columnSchema2.setDataType(DataType.STRING);
    columnSchema2.setDimensionColumn(true);

    new MockUp<CarbonMetadata>() {
      @Mock CarbonTable getCarbonTable(String tableUniqueName) {
        return new CarbonTable();
      }
    };
    new MockUp<CarbonTable>() {
      @Mock List<CarbonDimension> getDimensionByTableName(String tableName) {
        return Arrays.asList(new CarbonDimension(columnSchema, 0, 0, 0, 0));
      }

      @Mock public CarbonTableIdentifier getCarbonTableIdentifier() {
        return tIdentifier;
      }
    };
    new MockUp<CarbonUtil>() {
      @Mock List<ColumnSchema> getColumnSchemaList(List<CarbonDimension> carbonDimensionsList,
          List<CarbonMeasure> carbonMeasureList) {
        return Arrays.asList(columnSchema, columnSchema2);
      }

      @Mock int[] getFormattedCardinality(int[] dictionaryColumnCardinality,
          List<ColumnSchema> wrapperColumnSchemaList) {
        return new int[] { 10, 10 };
      }

      @Mock boolean hasEncoding(List<Encoding> encodings, Encoding encoding) {
        return true;
      }
    };
    new MockUp<CarbonDataLoadConfiguration>() {
      @Mock int getDimensionCount() {
        return 2;
      }

      @Mock int getNoDictionaryCount() {
        return 1;
      }

      @Mock int getComplexDimensionCount() {
        return 1;
      }

      @Mock String getTaskNo() {
        return "10";
      }
    };
    new MockUp<CarbonDimension>() {
      @Mock boolean hasEncoding(Encoding encoding) {
        return false;
      }

      @Mock List<Encoding> getEncoder() {
        return Arrays.asList(Encoding.DICTIONARY);
      }
    };
    new MockUp<CarbonDataProcessorUtil>() {
      @Mock Map<String, GenericDataType> getComplexTypesMap(DataField[] dataFields) {
        return getGenericDataTypeMap();
      }
    };
    new MockUp<ArrayDataType>() {
      @Mock public void setOutputArrayIndex(int outputArrayIndex) {
      }

      @Mock public void getAllPrimitiveChildren(List<GenericDataType> primitiveChild) {
      }
    };
    new MockUp<CarbonStorePath>() {
      @Mock CarbonTablePath getCarbonTablePath(String storePath,
          CarbonTableIdentifier tableIdentifier) {
        return new CarbonTablePath(tIdentifier,
            "/tmp" + File.separator + tIdentifier.getDatabaseName() + File.separator + tIdentifier
                .getTableName());
      }
    };

    config = new CarbonDataLoadConfiguration();
    tIdentifier = new CarbonTableIdentifier("derby", "test", "TB100");
    config.setTableIdentifier(new AbsoluteTableIdentifier("/tmp", tIdentifier));
    dataLoadProcessorStep = new InputProcessorStepImpl(new CarbonDataLoadConfiguration(), iterator);
    dataWriterProcessorStep = new DataWriterProcessorStepImpl(config, dataLoadProcessorStep);
    dataField = new DataField(new CarbonColumn(columnSchema, 0, 0));
    dataField2 = new DataField(new CarbonColumn(columnSchema2, 0, 0));
    batch1.addRow(new CarbonRow(inputData));
    batch1.addRow(new CarbonRow(inputData));
    batch2.addRow(new CarbonRow(inputData));
    batch2.addRow(new CarbonRow(inputData));
    iterators[0] = new ArrayList<CarbonRowBatch>(Arrays.asList(batch1)).iterator();
    iterators[1] = new ArrayList<CarbonRowBatch>(Arrays.asList(batch2)).iterator();
    config.setDataFields(new DataField[] { dataField, dataField2 });
    config.setDataLoadProperty(DataLoadProcessorConstants.DIMENSION_LENGTHS, new int[] { 10, 10 });
  }

  public static Map<String, GenericDataType> getGenericDataTypeMap() {
    String complexTypeString =
        "'':ARRAY:name:111#'':ARRAY:name:112;'':ARRAY:name:113#'':ARRAY:name:114";
    Map<String, GenericDataType> complexTypesMap = new LinkedHashMap<String, GenericDataType>();
    String[] hierarchies = complexTypeString.split(";");
    for (int i = 0; i < hierarchies.length; i++) {
      String[] levels = hierarchies[i].split("#");
      String[] levelInfo = levels[0].split(":");
      GenericDataType g = levelInfo[1].equals(CarbonCommonConstants.ARRAY) ?
          new ArrayDataType(levelInfo[0], "", levelInfo[3]) :
          new StructDataType(levelInfo[0], "", levelInfo[3]);
      complexTypesMap.put(levelInfo[0], g);
    }
    return complexTypesMap;
  }

  @Test public void testExecuteWithoutException() throws CarbonDataLoadingException {
    setupCommonObjects();
    new MockUp<CarbonRow>() {
      @Mock public Object[] getObjectArray(int ordinal) {
        return new Object[2];
      }

      @Mock public int[] getIntArray(int ordinal) {
        return new int[2];
      }
    };

    dataWriterProcessorStep.initialize();
    Iterator<CarbonRowBatch> result[] = dataWriterProcessorStep.execute();
    assertNull(result);
  }

  @Test(expected = CarbonDataLoadingException.class) public void testExecuteWithException()
      throws CarbonDataLoadingException {
    setupCommonObjects();
    new MockUp<CarbonRow>() {
      @Mock public Object[] getObjectArray(int ordinal) {
        return new Object[2];
      }

      @Mock public int[] getIntArray(int ordinal) {
        return new int[0];
      }
    };

    dataWriterProcessorStep.initialize();
    Iterator<CarbonRowBatch> result[] = dataWriterProcessorStep.execute();
    assertNull(result);
  }

  private void setupCommonObjects() {
    carbonFactDataHandlerModel =
        CarbonFactDataHandlerModel.createCarbonFactDataHandlerModel(config, "/tmp");
    carbonFactDataHandlerModel.setTableName("test");
    carbonFactDataHandlerModel.setDatabaseName("derby");

    new MockUp<CarbonFactDataHandlerModel>() {
      @Mock CarbonFactDataHandlerModel createCarbonFactDataHandlerModel(
          CarbonDataLoadConfiguration configuration, String storeLocation) {
        return carbonFactDataHandlerModel;
      }

      @Mock SegmentProperties getSegmentProperties() {
        return new SegmentProperties(Arrays.asList(columnSchema, columnSchema2),
            new int[] { 10, 10 });
      }
    };
    new MockUp<InputProcessorStepImpl>() {
      @Mock public Iterator<CarbonRowBatch>[] execute() {
        return iterators;
      }

      @Mock public void initialize() {
      }
    };
    new MockUp<CarbonDataProcessorUtil>() {
      @Mock String getLocalDataFolderLocation(String databaseName, String tableName, String taskId,
          String partitionId, String segmentId, boolean isCompactionFlow) {
        return "/tmp";
      }
    };

    new MockUp<CarbonDataLoadConfiguration>() {
      @Mock int getComplexDimensionCount() {
        return 0;
      }
    };
    new MockUp<SegmentProperties>() {
      @Mock public KeyGenerator getDimensionKeyGenerator() {
        return new MultiDimKeyVarLengthGenerator(new int[] { 5 });
      }
    };
    new MockUp<CarbonFactHandlerFactory>() {
      @Mock CarbonFactHandler createCarbonFactHandler(CarbonFactDataHandlerModel model,
          CarbonFactHandlerFactory.FactHandlerType handlerType) {
        return new CarbonFactDataHandlerColumnar(carbonFactDataHandlerModel);
      }
    };
    new MockUp<CarbonFactDataHandlerColumnar>() {
      @Mock int getComplexColsCount() {
        return 1;
      }

      @Mock int getColsCount(int columnSplit) {
        return 1;
      }

      @Mock public void addDataToStore(Object[] row) throws CarbonDataWriterException {
      }
    };
    new MockUp<ArrayDataType>() {
      @Mock
      public void fillAggKeyBlock(List<Boolean> aggKeyBlockWithComplex, boolean[] aggKeyBlock) {
        aggKeyBlockWithComplex.add(false);
      }
    };
    new MockUp<CarbonFactDataHandlerColumnar>() {
      @Mock public void initialise() throws CarbonDataWriterException {
      }
    };
  }

  @Test public void testGetOutput() {
    new MockUp<InputProcessorStepImpl>() {
      @Mock public DataField[] getOutput() {
        return new DataField[] { dataField };
      }
    };

    DataField result[] = dataWriterProcessorStep.getOutput();
    assertEquals(result.length, 1);
  }

  @Test public void testProcessRow() {
    assertNull(dataWriterProcessorStep.processRow(new CarbonRow(inputData)));
  }

  @Test public void close() {
    dataWriterProcessorStep.close();
  }

}
