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

import junit.framework.TestCase;
import mockit.Deencapsulation;
import mockit.Mock;
import mockit.MockUp;
import org.apache.commons.lang.reflect.FieldUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.parquet.serde.ArrayWritableObjectInspector;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableIntObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.*;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.STRUCT;

public class CarbonHiveSerdeTest {

  CarbonHiveSerDe carbonHiveSerDe = new CarbonHiveSerDe();
  private Configuration configuration;
  private Properties properties;
  private Writable[] writables = null;
  private int propertyCounter = 0;

  @After public void tearDown() {
    writables = null;
  }

  private StructTypeInfo createSampleStructTypeInfo() {
    StructTypeInfo structTypeInfo = new StructTypeInfo();
    structTypeInfo.setAllStructFieldNames(new ArrayList<>(Arrays.asList("id", "name")));
    PrimitiveTypeInfo col1TypeInfo = new PrimitiveTypeInfo();
    col1TypeInfo.setTypeName("int");
    PrimitiveTypeInfo col2TypeInfo = new PrimitiveTypeInfo();
    col2TypeInfo.setTypeName("string");
    structTypeInfo.setAllStructFieldTypeInfos(
        new ArrayList<TypeInfo>(Arrays.asList(col1TypeInfo, col2TypeInfo)));
    return structTypeInfo;
  }

  private void getClassField(ObjectInspector objectInspector) {
    try {
      Field field = carbonHiveSerDe.getClass().getDeclaredField("objInspector");
      field.setAccessible(true);
      FieldUtils.writeDeclaredField(carbonHiveSerDe, "objInspector", objectInspector, true);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      e.printStackTrace();
    }
  }

  @Test public void testInitialize() {
    carbonHiveSerDe = null;
    carbonHiveSerDe = new CarbonHiveSerDe();
    configuration = new Configuration();
    properties = new Properties();

    new MockUp<Properties>() {
      @Mock public String getProperty(String key) {
        propertyCounter = 0;
        if (propertyCounter == 0) {
          propertyCounter++;
          return "id,name";
        } else {
          propertyCounter++;
          return "int,string";
        }
      }
    };

    new MockUp<TypeInfoUtils>() {
      @Mock ArrayList<PrimitiveTypeInfo> getTypeInfosFromTypeString(String typeString) {
        PrimitiveTypeInfo intTypeInfo = new PrimitiveTypeInfo();
        intTypeInfo.setTypeName("int");
        PrimitiveTypeInfo stringTypeInfo = new PrimitiveTypeInfo();
        stringTypeInfo.setTypeName("string");
        return new ArrayList<>(Arrays.asList(intTypeInfo, stringTypeInfo));
      }
    };

    new MockUp<TypeInfoFactory>() {
      @Mock TypeInfo getStructTypeInfo(List<String> names, List<TypeInfo> typeInfos) {
        return createSampleStructTypeInfo();
      }
    };

    new MockUp<StructTypeInfo>() {
      @Mock List<String> getAllStructFieldNames() {
        return new ArrayList<>(Arrays.asList("id", "name"));
      }

      @Mock
      public java.util.ArrayList<org.apache.hadoop.hive.serde2.typeinfo.TypeInfo> getAllStructFieldTypeInfos() {
        PrimitiveTypeInfo intTypeInfo = new PrimitiveTypeInfo();
        intTypeInfo.setTypeName("int");
        PrimitiveTypeInfo stringTypeInfo = new PrimitiveTypeInfo();
        stringTypeInfo.setTypeName("string");
        return new ArrayList<TypeInfo>(Arrays.asList(intTypeInfo, stringTypeInfo));
      }
    };

    try {
      carbonHiveSerDe.initialize(configuration, properties);
      Assert.assertEquals(carbonHiveSerDe.getObjectInspector().getCategory(), STRUCT);
      Field field = carbonHiveSerDe.getClass().getDeclaredField("serializedSize");
      field.setAccessible(true);
      Assert.assertEquals(0, field.getLong(carbonHiveSerDe));
    } catch (SerDeException | NoSuchFieldException | IllegalAccessException exception) {
      Assert.assertTrue(false);
    }
  }

  @Test public void testInitializeForEmptyColumn() {
    configuration = new Configuration();
    properties = new Properties();

    new MockUp<Properties>() {
      @Mock public String getProperty(String key) {
        return "";
      }
    };

    new MockUp<TypeInfoUtils>() {
      @Mock ArrayList<PrimitiveTypeInfo> getTypeInfosFromTypeString(String typeString) {
        return new ArrayList<>();
      }
    };

    new MockUp<TypeInfoFactory>() {
      @Mock TypeInfo getStructTypeInfo(List<String> names, List<TypeInfo> typeInfos) {
        return createSampleStructTypeInfo();
      }
    };

    new MockUp<StructTypeInfo>() {
      @Mock List<String> getAllStructFieldNames() {
        return new ArrayList<>();
      }

      @Mock public String toString() {
        return "struct<>";
      }
    };

    try {
      carbonHiveSerDe.initialize(configuration, properties);
      Assert.assertEquals(carbonHiveSerDe.getObjectInspector().getCategory(), STRUCT);
    } catch (SerDeException serdeException) {
      Assert.assertTrue(false);
    }
  }

  @Test public void testGetSerializedClass() {
    Assert.assertTrue(carbonHiveSerDe.getSerializedClass().equals(ArrayWritable.class));
  }

  @Test(expected = SerDeException.class) public void testSerializeForException()
      throws SerDeException {

    new MockUp<AbstractPrimitiveObjectInspector>() {
      @Mock public ObjectInspector.Category getCategory() {
        return ObjectInspector.Category.PRIMITIVE;
      }
    };

    ObjectInspector objInspector = PrimitiveObjectInspectorFactory.writableIntObjectInspector;
    getClassField(objInspector);
    ObjectInspector objectInspector =
        new ArrayWritableObjectInspector(createSampleStructTypeInfo());
    carbonHiveSerDe.serialize(new Object(), objectInspector);
  }

  @Test public void testSerializeForPrimitiveCategory() {
    writables = getWritable("int");
    final StructTypeInfo structTypeInfo = getTypeInfo("int");

    new MockUp<CarbonObjectInspector>() {
      @Mock public ObjectInspector.Category getCategory() {
        return ObjectInspector.Category.STRUCT;
      }
    };

    new MockUp<CarbonObjectInspector>() {
      @Mock List<? extends StructField> getAllStructFieldRefs() {
        CarbonObjectInspector carbonObjectInspector = new CarbonObjectInspector(structTypeInfo);
        return Deencapsulation.getField(carbonObjectInspector, "fields");
      }

      @Mock Object getStructFieldData(Object var1, StructField var2) {
        return new IntWritable(10000);
      }
    };

    new MockUp<AbstractPrimitiveObjectInspector>() {
      @Mock public ObjectInspector.Category getCategory() {
        return ObjectInspector.Category.PRIMITIVE;
      }

      @Mock public PrimitiveObjectInspector.PrimitiveCategory getPrimitiveCategory() {
        return PrimitiveObjectInspector.PrimitiveCategory.INT;
      }
    };

    try {
      Class<?> innerClass =
          Class.forName(CarbonObjectInspector.class.getName() + "$StructFieldImpl");
      new MockUp(innerClass) {
        @Mock ObjectInspector getFieldObjectInspector() {
          return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
        }
      };
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }

    new MockUp<WritableIntObjectInspector>() {
      @Mock int get(Object o) {
        return 1000;
      }
    };

    new MockUp<StructTypeInfo>() {
      @Mock List<String> getAllStructFieldNames() {
        return new ArrayList<>(Arrays.asList("id"));
      }

      @Mock public String toString() {
        return "struct<id:int>";
      }
    };

    ObjectInspector objectInspector = new CarbonObjectInspector(structTypeInfo);
    getClassField(objectInspector);
    testWritableInstance(writables, objectInspector);
  }

  @Test public void testSerializeForListCategory() {
    final CarbonArrayInspector carbonArrayInspector =
        new CarbonArrayInspector(PrimitiveObjectInspectorFactory.writableIntObjectInspector);
    final ArrayWritable writables = new ArrayWritable(ArrayWritable.class, getWritable("list"));
    final StructTypeInfo structTypeInfo = getTypeInfo("int");

    new MockUp<CarbonObjectInspector>() {
      @Mock public ObjectInspector.Category getCategory() {
        return ObjectInspector.Category.STRUCT;
      }
    };

    new MockUp<CarbonObjectInspector>() {
      @Mock List<? extends StructField> getAllStructFieldRefs() {
        CarbonObjectInspector carbonObjectInspector = new CarbonObjectInspector(structTypeInfo);
        return Deencapsulation.getField(carbonObjectInspector, "fields");
      }

      @Mock Object getStructFieldData(Object var1, StructField var2) {
        return writables;
      }
    };

    new MockUp<CarbonArrayInspector>() {
      @Mock public ObjectInspector.Category getCategory() {
        return ObjectInspector.Category.LIST;
      }

      @Mock public List<?> getList(final Object data) {
        return new ArrayList<>(Arrays.asList(1000));
      }

      @Mock ObjectInspector getListElementObjectInspector() {
        return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
      }

    };

    new MockUp<AbstractPrimitiveObjectInspector>() {
      @Mock public ObjectInspector.Category getCategory() {
        return ObjectInspector.Category.PRIMITIVE;
      }

      @Mock public PrimitiveObjectInspector.PrimitiveCategory getPrimitiveCategory() {
        return PrimitiveObjectInspector.PrimitiveCategory.INT;
      }
    };

    try {
      Class<?> innerClass =
          Class.forName(CarbonObjectInspector.class.getName() + "$StructFieldImpl");
      new MockUp(innerClass) {
        @Mock ObjectInspector getFieldObjectInspector() {
          return carbonArrayInspector;
        }
      };
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }

    new MockUp<WritableIntObjectInspector>() {
      @Mock int get(Object o) {
        return 1000;
      }
    };

    new MockUp<StructTypeInfo>() {
      @Mock List<String> getAllStructFieldNames() {
        return new ArrayList<>(Arrays.asList("id"));
      }

      @Mock public String toString() {
        return "struct<id:int>";
      }
    };

    ObjectInspector objectInspector = new CarbonObjectInspector(structTypeInfo);
    getClassField(objectInspector);
    try {
      ArrayWritable writable =
          (ArrayWritable) carbonHiveSerDe.serialize(writables, objectInspector);
      Assert.assertTrue(writable instanceof ArrayWritable);
      Assert.assertEquals(objectInspector.getCategory(), ObjectInspector.Category.STRUCT);
      Assert.assertEquals(
          ((IntWritable) (((ArrayWritable) (((ArrayWritable) (writable.get()[0])).get()[0]))
              .get()[0])).get(), 1000);
    } catch (SerDeException e) {
      Assert.assertTrue(false);
      e.printStackTrace();
    }
  }

  @Test public void testSerializeForSerDeException() {

    final CarbonArrayInspector carbonArrayInspector =
        new CarbonArrayInspector(PrimitiveObjectInspectorFactory.writableIntObjectInspector);
    final ArrayWritable writables = new ArrayWritable(IntWritable.class, getWritable("int"));
    final StructTypeInfo structTypeInfo = getTypeInfo("int");

    new MockUp<CarbonObjectInspector>() {
      @Mock public ObjectInspector.Category getCategory() {
        return ObjectInspector.Category.STRUCT;
      }
    };

    new MockUp<CarbonObjectInspector>() {

      @Mock List<? extends StructField> getAllStructFieldRefs() {
        CarbonObjectInspector carbonObjectInspector = new CarbonObjectInspector(structTypeInfo);
        return Deencapsulation.getField(carbonObjectInspector, "fields");
      }

      @Mock Object getStructFieldData(Object var1, StructField var2) {
        return writables;
      }
    };

    new MockUp<CarbonArrayInspector>() {
      @Mock public ObjectInspector.Category getCategory() {
        return ObjectInspector.Category.UNION;
      }

      @Mock public List<?> getList(final Object data) {
        return new ArrayList<>(Arrays.asList(1000));
      }

      @Mock ObjectInspector getListElementObjectInspector() {
        return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
      }
    };

    new MockUp<AbstractPrimitiveObjectInspector>() {
      @Mock public ObjectInspector.Category getCategory() throws SerDeException {
        throw new SerDeException("Unable to get Category For PrimitiveObjectInspector");
      }

      @Mock public PrimitiveObjectInspector.PrimitiveCategory getPrimitiveCategory() {
        return PrimitiveObjectInspector.PrimitiveCategory.INT;
      }
    };

    try {
      Class<?> innerClass =
          Class.forName(CarbonObjectInspector.class.getName() + "$StructFieldImpl");
      new MockUp(innerClass) {
        @Mock ObjectInspector getFieldObjectInspector() {
          return carbonArrayInspector;
        }
      };
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }

    new MockUp<WritableIntObjectInspector>() {
      @Mock int get(Object o) {
        return 1000;
      }
    };

    new MockUp<StructTypeInfo>() {
      @Mock List<String> getAllStructFieldNames() {
        List<String> fieldNames = new ArrayList<String>(Arrays.asList("id"));
        return fieldNames;
      }

      @Mock public String toString() {
        return "struct<id:int>";
      }
    };

    // Invocation of method
    ObjectInspector objectInspector = new CarbonObjectInspector(structTypeInfo);
    getClassField(objectInspector);
    try {
      carbonHiveSerDe.serialize(writables, objectInspector);
      Assert.assertTrue(false);
    } catch (SerDeException e) {
      Assert.assertTrue(true);
    }
  }

  @Test public void testGetObjectInspector() {
    ObjectInspector objectInspector = new CarbonObjectInspector(createSampleStructTypeInfo());
    getClassField(objectInspector);
    try {
      Assert.assertEquals(carbonHiveSerDe.getObjectInspector(), objectInspector);
    } catch (SerDeException exception) {
      Assert.assertTrue(false);
    }
  }

  @Test public void testGetSerDeStatsForDeSerialize() {
    Assert.assertEquals(carbonHiveSerDe.getSerDeStats().getRawDataSize(), 0);
  }

  @Test public void testDeserializeForNullWritable() {
    final StructTypeInfo structTypeInfo = getTypeInfo("int");
    getClassField(new CarbonObjectInspector(structTypeInfo));

    try {
      Object writable = carbonHiveSerDe.deserialize(new IntWritable(10000));
      Assert.assertTrue(writable == null);
    } catch (SerDeException e) {
      Assert.assertTrue(false);
    }
  }

  @Test public void testDeserializeForArrayWritable() {
    final StructTypeInfo structTypeInfo = getTypeInfo("int");

    new MockUp<CarbonObjectInspector>() {
      @Mock List<? extends StructField> getAllStructFieldRefs() {
        CarbonObjectInspector carbonObjectInspector = new CarbonObjectInspector(structTypeInfo);
        return Deencapsulation.getField(carbonObjectInspector, "fields");
      }
    };

    new MockUp<StructTypeInfo>() {
      @Mock List<String> getAllStructFieldNames() {
        return new ArrayList<>(Arrays.asList("salary"));
      }

      @Mock public String toString() {
        return "struct<salary:int>";
      }
    };

    try {
      Class<?> innerClass =
          Class.forName(CarbonObjectInspector.class.getName() + "$StructFieldImpl");
      new MockUp(innerClass) {
        @Mock ObjectInspector getFieldObjectInspector() {
          return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
        }
      };
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }

    ArrayWritable writables =
        new ArrayWritable(IntWritable.class, new Writable[] { new IntWritable(10000) });

    ObjectInspector objectInspector = new CarbonObjectInspector(structTypeInfo);
    getClassField(objectInspector);

    try {
      Object writable = carbonHiveSerDe.deserialize(writables);
      Field field = carbonHiveSerDe.getClass().getDeclaredField("deserializedSize");
      field.setAccessible(true);
      Assert.assertTrue(field.getLong(carbonHiveSerDe) == 1);
      Assert.assertEquals(writable, writables);
    } catch (SerDeException | NoSuchFieldException | IllegalAccessException e) {
      Assert.assertTrue(false);
    }
  }

  @Test public void testSerializeForLongCategory() {
    writables = getWritable("bigint");
    final StructTypeInfo structTypeInfo = getTypeInfo("bigint");

    new MockUp<CarbonObjectInspector>() {
      @Mock public ObjectInspector.Category getCategory() {
        return ObjectInspector.Category.STRUCT;
      }
    };

    new MockUp<CarbonObjectInspector>() {
      @Mock List<? extends StructField> getAllStructFieldRefs() {
        CarbonObjectInspector carbonObjectInspector = new CarbonObjectInspector(structTypeInfo);
        return Deencapsulation.getField(carbonObjectInspector, "fields");
      }

      @Mock Object getStructFieldData(Object var1, StructField var2) {
        return new LongWritable(10000);
      }
    };

    new MockUp<AbstractPrimitiveObjectInspector>() {
      @Mock public ObjectInspector.Category getCategory() {
        return ObjectInspector.Category.PRIMITIVE;
      }

      @Mock public PrimitiveObjectInspector.PrimitiveCategory getPrimitiveCategory() {
        return PrimitiveObjectInspector.PrimitiveCategory.LONG;
      }
    };

    try {
      Class<?> innerClass =
          Class.forName(CarbonObjectInspector.class.getName() + "$StructFieldImpl");
      new MockUp(innerClass) {
        @Mock ObjectInspector getFieldObjectInspector() {
          return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
        }
      };
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }

    new MockUp<WritableIntObjectInspector>() {
      @Mock int get(Object o) {
        return 1000;
      }
    };

    new MockUp<StructTypeInfo>() {
      @Mock List<String> getAllStructFieldNames() {
        List<String> fieldNames = new ArrayList<String>(Arrays.asList("salary"));
        return fieldNames;
      }

      @Mock public String toString() {
        return "struct<salary:bigint>";
      }
    };

    // Invocation of method
    ObjectInspector objectInspector = new CarbonObjectInspector(structTypeInfo);
    getClassField(objectInspector);

    testWritableInstance(writables, objectInspector);
  }

  @Test public void testSerializeForDoubleCategory() {
    writables = getWritable("double");
    final StructTypeInfo structTypeInfo = getTypeInfo("double");

    new MockUp<CarbonObjectInspector>() {
      @Mock public ObjectInspector.Category getCategory() {
        return ObjectInspector.Category.STRUCT;
      }
    };

    new MockUp<CarbonObjectInspector>() {
      @Mock List<? extends StructField> getAllStructFieldRefs() {
        CarbonObjectInspector carbonObjectInspector = new CarbonObjectInspector(structTypeInfo);
        return Deencapsulation.getField(carbonObjectInspector, "fields");
      }

      @Mock Object getStructFieldData(Object var1, StructField var2) {
        return new DoubleWritable(10000);
      }
    };

    new MockUp<AbstractPrimitiveObjectInspector>() {
      @Mock public ObjectInspector.Category getCategory() {
        return ObjectInspector.Category.PRIMITIVE;
      }

      @Mock public PrimitiveObjectInspector.PrimitiveCategory getPrimitiveCategory() {
        return PrimitiveObjectInspector.PrimitiveCategory.DOUBLE;
      }
    };

    try {
      Class<?> innerClass =
          Class.forName(CarbonObjectInspector.class.getName() + "$StructFieldImpl");
      new MockUp(innerClass) {
        @Mock ObjectInspector getFieldObjectInspector() {
          return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
        }
      };
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }

    new MockUp<WritableIntObjectInspector>() {
      @Mock int get(Object o) {
        return 1000;
      }
    };

    new MockUp<StructTypeInfo>() {
      @Mock List<String> getAllStructFieldNames() {
        return new ArrayList<String>(Arrays.asList("salary"));
      }

      @Mock public String toString() {
        return "struct<salary:double>";
      }
    };

    // Invocation of method
    ObjectInspector objectInspector = new CarbonObjectInspector(structTypeInfo);
    getClassField(objectInspector);

    testWritableInstance(writables, objectInspector);
  }

  private void testWritableInstance(Writable[] writables, ObjectInspector objectInspector) {
    try {
      Writable writable = carbonHiveSerDe.serialize(writables, objectInspector);
      Assert.assertTrue(writable instanceof ArrayWritable);
      Assert.assertEquals(objectInspector.getCategory(), STRUCT);
      Assert.assertEquals(((ArrayWritable) writable).getValueClass(), Writable.class);
    } catch (SerDeException e) {
      Assert.assertTrue(false);
      e.printStackTrace();
    }
  }

  private StructTypeInfo getTypeInfo(String type) {
    final StructTypeInfo structTypeInfo = new StructTypeInfo();
    structTypeInfo.setAllStructFieldNames(new ArrayList<>(Arrays.asList("salary")));
    PrimitiveTypeInfo col1TypeInfo = new PrimitiveTypeInfo();
    col1TypeInfo.setTypeName(type);
    structTypeInfo.setAllStructFieldTypeInfos(new ArrayList<TypeInfo>(Arrays.asList(col1TypeInfo)));
    return structTypeInfo;
  }

  private Writable[] getWritable(String type) {
    switch (type) {
      case "double":
        return new Writable[] { new DoubleWritable(10000) };
      case "int":
        return new Writable[] { new IntWritable(10000) };
      case "list":
        IntWritable intWritable1 = new IntWritable(10000);
        Writable[] intWritables = new Writable[] { intWritable1 };
        ArrayWritable arrayWritable1 = new ArrayWritable(IntWritable.class, intWritables);
        ArrayWritable arrayWritable2 = new ArrayWritable(IntWritable.class, intWritables);
        return new Writable[] { arrayWritable1, arrayWritable2 };
      case "bigint":
        return new Writable[] { new LongWritable(10000) };
      default:
        return null;
    }
  }
}