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

package org.apache.carbondata.processing.newflow.converter.impl;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.List;

import org.apache.carbondata.core.datastore.GenericDataType;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.processing.newflow.converter.BadRecordLogHolder;
import org.apache.carbondata.processing.newflow.exception.CarbonDataLoadingException;

public class ComplexFieldConverterImpl extends AbstractDictionaryFieldConverterImpl {

  private GenericDataType genericDataType;

  private int index;

  public ComplexFieldConverterImpl(GenericDataType genericDataType, int index) {
    this.genericDataType = genericDataType;
    this.index = index;
  }

  @Override
  public void convert(CarbonRow row, BadRecordLogHolder logHolder) {
    Object object = row.getObject(index);
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
}
