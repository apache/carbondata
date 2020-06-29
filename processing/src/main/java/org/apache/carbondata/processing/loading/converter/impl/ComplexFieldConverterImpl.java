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

import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.processing.datatypes.GenericDataType;
import org.apache.carbondata.processing.loading.DataField;
import org.apache.carbondata.processing.loading.converter.BadRecordLogHolder;
import org.apache.carbondata.processing.loading.converter.FieldConverter;
import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException;

public class ComplexFieldConverterImpl implements FieldConverter {

  private GenericDataType genericDataType;

  private int index;
  private DataField dataField;

  public ComplexFieldConverterImpl(DataField dataField, GenericDataType genericDataType,
      int index) {
    this.genericDataType = genericDataType;
    this.index = index;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3548
    this.dataField = dataField;
  }

  @Override
  public void convert(CarbonRow row, BadRecordLogHolder logHolder) {
    Object object = row.getObject(index);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2633
    row.update(convert(object, logHolder), index);
  }

  @Override
  public Object convert(Object value, BadRecordLogHolder logHolder) throws RuntimeException {
    // TODO Its temporary, needs refactor here.
    ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArray);
    try {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3761
      genericDataType.writeByteArray(value, dataOutputStream, logHolder, false);
      dataOutputStream.close();
      return byteArray.toByteArray();
    } catch (Exception e) {
      throw new CarbonDataLoadingException(value + "", e);
    }
  }

  @Override
  public DataField getDataField() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3548
    return dataField;
  }

  /**
   * Method to clear out the dictionary caches. In this instance nothing to clear.
   */
  @Override
  public void clear() {
  }

}
