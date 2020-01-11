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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.ArrayWritable;

class CarbonObjectInspector extends SettableStructObjectInspector {
  private final TypeInfo typeInfo;
  private final List<StructField> fields;
  private final HashMap<String, StructFieldImpl> fieldsByName;

  public CarbonObjectInspector(final StructTypeInfo rowTypeInfo) {

    typeInfo = rowTypeInfo;
    List<String> fieldNames = rowTypeInfo.getAllStructFieldNames();
    List<TypeInfo> fieldInfos = rowTypeInfo.getAllStructFieldTypeInfos();
    fields = new ArrayList<StructField>(fieldNames.size());
    fieldsByName = new HashMap<String, StructFieldImpl>();

    for (int i = 0; i < fieldNames.size(); ++i) {
      final String name = fieldNames.get(i);
      final TypeInfo fieldInfo = fieldInfos.get(i);

      final StructFieldImpl field = new StructFieldImpl(name, getObjectInspector(fieldInfo), i);
      fields.add(field);
      fieldsByName.put(name, field);
    }
  }

  private ObjectInspector getObjectInspector(final TypeInfo typeInfo) {
    if (typeInfo instanceof PrimitiveTypeInfo) {
      return PrimitiveObjectInspectorFactory
          .getPrimitiveWritableObjectInspector((PrimitiveTypeInfo) typeInfo);
    } else if (typeInfo.getCategory().equals(Category.STRUCT)) {
      return new CarbonObjectInspector((StructTypeInfo) typeInfo);
    } else if (typeInfo.getCategory().equals(Category.LIST)) {
      final TypeInfo subTypeInfo = ((ListTypeInfo) typeInfo).getListElementTypeInfo();
      return new CarbonArrayInspector(getObjectInspector(subTypeInfo));
    } else if (typeInfo instanceof MapTypeInfo) {
      MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;
      ObjectInspector mapKeyObjectIns = getObjectInspector(mapTypeInfo.getMapKeyTypeInfo());
      ObjectInspector mapValObjectIns = getObjectInspector(mapTypeInfo.getMapValueTypeInfo());
      return new CarbonMapInspector(mapKeyObjectIns, mapValObjectIns);
    } else {
      throw new UnsupportedOperationException("Unknown field type: " + typeInfo);
    }
  }

  @Override
  public Category getCategory() {
    return Category.STRUCT;
  }

  @Override
  public String getTypeName() {
    return typeInfo.getTypeName();
  }

  @Override
  public List<? extends StructField> getAllStructFieldRefs() {
    return fields;
  }

  @Override
  public Object getStructFieldData(final Object data, final StructField fieldRef) {
    if (data == null) {
      return null;
    }

    if (data instanceof ArrayWritable) {
      final ArrayWritable arr = (ArrayWritable) data;
      return arr.get()[((StructFieldImpl) fieldRef).getIndex()];
    }

    boolean isArray = !(data instanceof List);
    int listSize = isArray ? ((Object[]) data).length : ((List) data).size();
    int fieldID = fieldRef.getFieldID();
    return fieldID >= listSize ?
        null :
        (isArray ? ((Object[]) data)[fieldID] : ((List) data).get(fieldID));
  }

  @Override
  public StructField getStructFieldRef(final String name) {
    return fieldsByName.get(name);
  }

  @Override
  public List<Object> getStructFieldsDataAsList(final Object data) {
    if (data == null) {
      return null;
    }

    if (data instanceof ArrayWritable) {
      final ArrayWritable arr = (ArrayWritable) data;
      final Object[] arrWritable = arr.get();
      return new ArrayList<Object>(Arrays.asList(arrWritable));
    }

    throw new UnsupportedOperationException("Cannot inspect " + data.getClass().getCanonicalName());
  }

  @Override
  public Object create() {
    final ArrayList<Object> list = new ArrayList<Object>(fields.size());
    for (int i = 0; i < fields.size(); ++i) {
      list.add(null);
    }
    return list;
  }

  @Override
  public Object setStructFieldData(Object struct, StructField field, Object fieldValue) {
    final ArrayList<Object> list = (ArrayList<Object>) struct;
    list.set(((StructFieldImpl) field).getIndex(), fieldValue);
    return list;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final CarbonObjectInspector other = (CarbonObjectInspector) obj;
    return !(!Objects.equals(this.typeInfo, other.typeInfo));
  }

  @Override
  public int hashCode() {
    int hash = 5;
    hash = 29 * hash + (this.typeInfo != null ? this.typeInfo.hashCode() : 0);
    return hash;
  }

  private static class StructFieldImpl implements StructField {

    private final String name;
    private final ObjectInspector inspector;
    private final int index;

    public StructFieldImpl(final String name, final ObjectInspector inspector, final int index) {
      this.name = name;
      this.inspector = inspector;
      this.index = index;
    }

    @Override
    public String getFieldComment() {
      return "";
    }

    @Override
    public String getFieldName() {
      return name;
    }

    public int getIndex() {
      return index;
    }

    @Override
    public ObjectInspector getFieldObjectInspector() {
      return inspector;
    }

    @Override
    public int getFieldID() {
      return index;
    }
  }
}
