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

package org.apache.carbondata.processing.loading.converter.impl;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.processing.datatypes.ArrayDataType;
import org.apache.carbondata.processing.datatypes.GenericDataType;
import org.apache.carbondata.processing.datatypes.PrimitiveDataType;
import org.apache.carbondata.processing.datatypes.StructDataType;
import org.apache.carbondata.processing.loading.complexobjects.ArrayObject;
import org.apache.carbondata.processing.loading.complexobjects.StructObject;
import org.apache.carbondata.processing.loading.converter.BadRecordLogHolder;
import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;


public class ComplexFieldConverterImpl extends AbstractDictionaryFieldConverterImpl {

  private GenericDataType genericDataType;

  private int index;

  private boolean isEmptyBadRecord;

  public ComplexFieldConverterImpl(GenericDataType genericDataType, int index,
      boolean isEmptyBadRecord) {
    this.genericDataType = genericDataType;
    this.index = index;
    this.isEmptyBadRecord = isEmptyBadRecord;
  }

  @Override
  public void convert(CarbonRow row, BadRecordLogHolder logHolder) {
    Object object = row.getObject(index);
    checkBadRecord(object, genericDataType, logHolder);
    // TODO Its temporary, needs refactor here.
    ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArray);
    try {
      genericDataType.writeByteArray(object, dataOutputStream);
      dataOutputStream.close();
      row.update(byteArray.toByteArray(), index);
    } catch (Exception e) {
      throw new CarbonDataLoadingException(object + "", e);
    }
  }

  @Override public void fillColumnCardinality(List<Integer> cardinality) {
    genericDataType.fillCardinality(cardinality);
  }

  private BadRecordLogHolder checkBadRecord(Object object, GenericDataType genericDataTypeCheck,
      BadRecordLogHolder logHolder) {
    if (StructObject.class.isInstance(object)) {
      List<GenericDataType> genericDataTypeList =
          ((StructDataType) genericDataTypeCheck).getChildren();
      List<PrimitiveDataType> primitiveDataTypeList = new ArrayList<>();
      genericDataTypeCheck.getAllPrimitiveChildren(primitiveDataTypeList);
      int i = 0;
      for (Object o : ((StructObject) object).getData()) {
        if (StructObject.class.isInstance(o) || ArrayObject.class.isInstance(o)) {
          checkBadRecord(o, genericDataTypeList.get(i++), logHolder);
        } else if (null == o) {
          logHolder.setReason(CarbonDataProcessorUtil
              .prepareFailureReason(genericDataTypeCheck.getName(),
                  ((PrimitiveDataType) (genericDataTypeList.get(i++))).getCarbonDimension()
                      .getColumnSchema().getDataType()));
        } else {
          if ((DataTypeUtil.getDataBasedOnDataType((String) o,
              ((PrimitiveDataType) (genericDataTypeList.get(i))).getCarbonDimension()
                  .getColumnSchema().getDataType()) == null) || (((String) o).length() == 0
              && isEmptyBadRecord)) {
            logHolder.setReason(CarbonDataProcessorUtil
                .prepareFailureReason(genericDataTypeCheck.getName(),
                    ((PrimitiveDataType) (genericDataTypeList.get(i))).getCarbonDimension()
                        .getColumnSchema().getDataType()));
          }
          i++;
        }
      }
    } else if (ArrayObject.class.isInstance(object)) {
      List<PrimitiveDataType> primitiveDataTypeList = new ArrayList<>();
      genericDataTypeCheck.getAllPrimitiveChildren(primitiveDataTypeList);
      for (Object o : ((ArrayObject) object).getData()) {
        if (StructObject.class.isInstance(o) || ArrayObject.class.isInstance(o)) {
          checkBadRecord(o, ((ArrayDataType) genericDataTypeCheck).getChildren().get(0), logHolder);
        } else if (null == o) {
          logHolder.setReason(CarbonDataProcessorUtil
              .prepareFailureReason(genericDataTypeCheck.getName(),
                  primitiveDataTypeList.get(0).getCarbonDimension().getColumnSchema()
                      .getDataType()));
        } else {
          if ((DataTypeUtil.getDataBasedOnDataType((String) o,
              primitiveDataTypeList.get(0).getCarbonDimension().getColumnSchema().getDataType())
              == null) || (((String) o).length() == 0 && isEmptyBadRecord)) {
            logHolder.setReason(CarbonDataProcessorUtil
                .prepareFailureReason(genericDataTypeCheck.getName(),
                    primitiveDataTypeList.get(0).getCarbonDimension().getColumnSchema()
                        .getDataType()));
          }
        }
      }
    } else if ((object == null) || (((String) object).length() == 0 && isEmptyBadRecord)) {
      logHolder.setReason(CarbonDataProcessorUtil
          .prepareFailureReason(genericDataTypeCheck.getName(), DataTypes.STRING));
    }
    return logHolder;
  }
}
