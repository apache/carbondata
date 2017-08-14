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

package org.apache.carbondata.hive;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.carbondata.core.cache.dictionary.AbstractColumnDictionaryInfo;
import org.apache.carbondata.core.cache.dictionary.ColumnDictionaryInfo;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.apache.carbondata.core.cache.dictionary.ForwardDictionaryCache;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;

import mockit.Mock;
import mockit.MockUp;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.types.ArrayType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class CarbonDictionaryDecodeReadSupportTest {

  private static Date date = new Date();
  private static DataType dataTypes[] =
      new DataType[] { DataType.INT, DataType.STRING, DataType.NULL, DataType.DOUBLE, DataType.LONG,
          DataType.SHORT, DataType.DATE, DataType.TIMESTAMP, DataType.DECIMAL, DataType.STRUCT,
          DataType.ARRAY, DataType.BYTE_ARRAY };
  private static Encoding encodings[] =
      new Encoding[] { Encoding.DICTIONARY, Encoding.DIRECT_DICTIONARY, Encoding.BIT_PACKED };
  private static CarbonColumn carbonColumnsArray[] = new CarbonColumn[12];
  private static ColumnSchema columnSchemas[] = new ColumnSchema[12];
  private CarbonDictionaryDecodeReadSupport carbonDictionaryDecodeReadSupportObj =
      new CarbonDictionaryDecodeReadSupport();
  private static AbsoluteTableIdentifier absoluteTableIdentifier;
  private Dictionary dictionary = new ColumnDictionaryInfo(DataType.BOOLEAN);
  private String name[] = new String[] { "FirstName", "LastName" };
  private Object objects[];
  private static String dateFormat = new SimpleDateFormat("yyyy/MM/dd").format(date);
  private static String timeStamp = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(date);

  @BeforeClass public static void setUp() {

    for (int i = 0; i < carbonColumnsArray.length; i++) {
      List<Encoding> encodingList = new ArrayList<>();
      columnSchemas[i] = new ColumnSchema();
      columnSchemas[i].setDataType(dataTypes[i]);

      DataType datatype = columnSchemas[i].getDataType();
      if (datatype == DataType.STRING) {
        encodingList.add(encodings[0]);
      } else if (datatype.isComplexType()) {
        encodingList.add(encodings[0]);
        columnSchemas[i].setNumberOfChild(2);
        columnSchemas[i].setDimensionColumn(true);
      } else {
        encodingList.add(encodings[((i % 2) + 1)]);
      }
      columnSchemas[i].setEncodingList(encodingList);
      carbonColumnsArray[i] = new CarbonDimension(columnSchemas[i], 10, 20, 30, 40);
    }

    absoluteTableIdentifier = new AbsoluteTableIdentifier(
        "/incubator-carbondata/examples/spark2/target/store",
        new CarbonTableIdentifier("default", "t5", "TBLID"));

    new MockUp<AbstractColumnDictionaryInfo>() {
      @Mock public String getDictionaryValueForKey(int surrogateKey) {
        switch (surrogateKey) {
          case 1:
            return "China";
          case 2:
            return "Africa";
          case 3:
            return dateFormat;
          case 4:
            return timeStamp;
          case 5:
            return "Struct Or Array";
          case 6:
            return "20L";
          default:
            return "";
        }
      }
    };

  }

  @Test public void testInitialize() throws IOException {
    new MockUp<ForwardDictionaryCache>() {
      @Mock public Dictionary get(DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier)
          throws IOException {
        return dictionary;
      }
    };

    carbonDictionaryDecodeReadSupportObj.initialize(carbonColumnsArray, absoluteTableIdentifier);
    Assert.assertEquals(carbonDictionaryDecodeReadSupportObj.carbonColumns.length,
        carbonColumnsArray.length);
  }

  @Test public void testReadRowNullPrimitive() throws IOException {
    new MockUp<ForwardDictionaryCache>() {
      @Mock public Dictionary get(DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier)
          throws IOException {
        return dictionary;
      }
    };

    CarbonColumn carbonColumns[] = new CarbonColumn[1];
    for (CarbonColumn column : carbonColumnsArray) {
      if (column.getDataType() == DataType.NULL) {
        carbonColumns[0] = column;
      }
    }
    objects = new Object[] { null };
    carbonDictionaryDecodeReadSupportObj.initialize(carbonColumns, absoluteTableIdentifier);
    carbonDictionaryDecodeReadSupportObj.readRow(objects);
    Assert.assertEquals(carbonDictionaryDecodeReadSupportObj.writableArr[0], objects[0]);
  }

  public void testReadRowIOException() throws IOException {
    new MockUp<ForwardDictionaryCache>() {
      @Mock public Dictionary get(DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier)
          throws IOException {
        return dictionary;
      }
    };

    CarbonColumn carbonColumns[] = new CarbonColumn[1];
    for (CarbonColumn column : carbonColumnsArray) {
      if (column.getDataType() == DataType.BYTE_ARRAY) {
        carbonColumns[0] = column;
      }
    }
    objects = new Object[] { 1 };
    try {
      carbonDictionaryDecodeReadSupportObj.initialize(carbonColumns, absoluteTableIdentifier);
      carbonDictionaryDecodeReadSupportObj.readRow(objects);
      Assert.assertTrue(false);
    } catch (IOException | RuntimeException ex) {
      Assert.assertTrue(true);
    }
  }

  @Test public void testReadRowPrimitive() throws IOException {

    new MockUp<ForwardDictionaryCache>() {
      @Mock public Dictionary get(DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier)
          throws IOException {
        return dictionary;
      }
    };

    CarbonColumn primitiveCarbonColumns[] = new CarbonColumn[9];
    int i = 0;
    for (CarbonColumn column : carbonColumnsArray) {
      if (!column.getDataType().isComplexType() && column.getDataType() != DataType.BYTE_ARRAY) {
        primitiveCarbonColumns[i] = column;
        i++;
      }
    }
    objects = new Object[] { 0, 1, 0, 1.23, 12L, (short) 1, 3, 4L, 1.23 };
    carbonDictionaryDecodeReadSupportObj
        .initialize(primitiveCarbonColumns, absoluteTableIdentifier);
    carbonDictionaryDecodeReadSupportObj.readRow(objects);
    Assert.assertEquals(carbonDictionaryDecodeReadSupportObj.writableArr[1].toString(), "China");
  }

  @Test public void testReadRowStructIf() throws IOException {
    new MockUp<ForwardDictionaryCache>() {
      @Mock public Dictionary get(DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier)
          throws IOException {
        return null;
      }
    };

    CarbonColumn structCarbonColumns[] = new CarbonColumn[1];
    for (CarbonColumn column : carbonColumnsArray) {
      if (column.getDataType() == DataType.STRUCT) {
        structCarbonColumns[0] = column;
      }
    }
    objects = new Object[] { new GenericInternalRow(name) };

    new MockUp<CarbonDimension>() {
      @Mock public List<CarbonDimension> getListOfChildDimensions() {
        ColumnSchema schema1 = new ColumnSchema();
        schema1.setColumnName("FirstName");
        schema1.setDataType(DataType.STRING);

        ColumnSchema schema2 = new ColumnSchema();
        schema2.setColumnName("LastName");
        schema2.setDataType(DataType.STRING);

        CarbonDimension carbonDimension1 = new CarbonDimension(schema1, 1, 2, 3, 4);
        CarbonDimension carbonDimension2 = new CarbonDimension(schema2, 1, 2, 3, 4);

        List<CarbonDimension> list = new ArrayList<>();
        list.add(carbonDimension1);
        list.add(carbonDimension2);
        return list;
      }
    };
    carbonDictionaryDecodeReadSupportObj.initialize(structCarbonColumns, absoluteTableIdentifier);
    carbonDictionaryDecodeReadSupportObj.readRow(objects);
    Assert.assertEquals(carbonDictionaryDecodeReadSupportObj.writableArr[0].getClass(),
        ArrayWritable.class);
  }

  public void testReadRowStructElse() throws IOException {
    new MockUp<ForwardDictionaryCache>() {
      @Mock public Dictionary get(DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier)
          throws IOException {
        return null;
      }
    };

    CarbonColumn structCarbonColumns[] = new CarbonColumn[1];
    for (CarbonColumn column : carbonColumnsArray) {
      if (column.getDataType() == DataType.STRUCT) {
        structCarbonColumns[0] = column;
      }
    }
    objects = new Object[] { "" };
    try {
      carbonDictionaryDecodeReadSupportObj.initialize(structCarbonColumns, absoluteTableIdentifier);
      carbonDictionaryDecodeReadSupportObj.readRow(objects);
      Assert.assertTrue(false);
    } catch (IOException | RuntimeException ex) {
      Assert.assertTrue(true);
    }

  }

  @Test public void testReadRowArrayIf() throws IOException {
    new MockUp<ForwardDictionaryCache>() {
      @Mock public Dictionary get(DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier)
          throws IOException {
        return null;
      }
    };

    CarbonColumn arrayCarbonColumns[] = new CarbonColumn[1];
    for (CarbonColumn column : carbonColumnsArray) {
      if (column.getDataType() == DataType.ARRAY) {
        arrayCarbonColumns[0] = column;
      }
    }
    objects = new Object[] { new GenericArrayData(name) };

    new MockUp<CarbonDimension>() {
      @Mock public List<CarbonDimension> getListOfChildDimensions() {
        ColumnSchema schema1 = new ColumnSchema();
        schema1.setColumnName("FirstName");
        schema1.setDataType(DataType.STRING);

        ColumnSchema schema2 = new ColumnSchema();
        schema2.setColumnName("LastName");
        schema2.setDataType(DataType.STRING);

        CarbonDimension carbonDimension1 = new CarbonDimension(schema1, 1, 2, 3, 4);
        CarbonDimension carbonDimension2 = new CarbonDimension(schema2, 1, 2, 3, 4);

        List<CarbonDimension> list = new ArrayList<>();
        list.add(carbonDimension1);
        list.add(carbonDimension2);
        return list;
      }
    };
    carbonDictionaryDecodeReadSupportObj.initialize(arrayCarbonColumns, absoluteTableIdentifier);
    carbonDictionaryDecodeReadSupportObj.readRow(objects);
    Assert.assertEquals(carbonDictionaryDecodeReadSupportObj.writableArr[0].getClass(),
        ArrayWritable.class);
  }

  @Test public void testReadRowArrayElse() throws IOException {
    new MockUp<ForwardDictionaryCache>() {
      @Mock public Dictionary get(DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier)
          throws IOException {
        return null;
      }
    };
    CarbonColumn arrayCarbonColumns[] = new CarbonColumn[1];
    for (CarbonColumn column : carbonColumnsArray) {
      if (column.getDataType() == DataType.ARRAY) {
        arrayCarbonColumns[0] = column;
      }
    }
    objects = new Object[] { new ArrayType() };

    carbonDictionaryDecodeReadSupportObj.initialize(arrayCarbonColumns, absoluteTableIdentifier);
    carbonDictionaryDecodeReadSupportObj.readRow(objects);
    Assert.assertEquals(carbonDictionaryDecodeReadSupportObj.writableArr[0], null);
  }

}
