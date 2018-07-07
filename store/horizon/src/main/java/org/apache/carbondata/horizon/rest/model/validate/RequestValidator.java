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

package org.apache.carbondata.horizon.rest.model.validate;

import org.apache.carbondata.horizon.rest.model.view.CreateTableRequest;
import org.apache.carbondata.horizon.rest.model.view.DropTableRequest;
import org.apache.carbondata.horizon.rest.model.view.LoadRequest;
import org.apache.carbondata.horizon.rest.model.view.SelectRequest;
import org.apache.carbondata.store.api.exception.StoreException;

import org.apache.commons.lang.StringUtils;

public class RequestValidator {

  public static void validateSelect(SelectRequest request) throws StoreException {
    if (request == null) {
      throw new StoreException("Select should not be null");
    }
    if (StringUtils.isEmpty(request.getDatabaseName())) {
      throw new StoreException("database name is invalid");
    }
    if (StringUtils.isEmpty(request.getTableName())) {
      throw new StoreException("table name is invalid");
    }
  }

  public static void validateTable(CreateTableRequest request) throws StoreException {
    if (request == null) {
      throw new StoreException("TableDescriptor should not be null");
    }
    if (StringUtils.isEmpty(request.getDatabaseName())) {
      throw new StoreException("database name is invalid");
    }
    if (StringUtils.isEmpty(request.getTableName())) {
      throw new StoreException("table name is invalid");
    }
    if (request.getFields() == null || request.getFields().length == 0) {
      throw new StoreException("fields should not be empty");
    }
  }

  public static void validateLoad(LoadRequest request)  throws StoreException {
    if (request == null) {
      throw new StoreException("LoadDescriptor should not be null");
    }
    if (StringUtils.isEmpty(request.getDatabaseName())) {
      throw new StoreException("database name is invalid");
    }
    if (StringUtils.isEmpty(request.getTableName())) {
      throw new StoreException("table name is invalid");
    }
    if (StringUtils.isEmpty(request.getInputPath())) {
      throw new StoreException("input path is invalid");
    }
  }

  public static void validateDrop(DropTableRequest request)  throws StoreException {
    if (request == null) {
      throw new StoreException("DropTableRequest should not be null");
    }
    if (StringUtils.isEmpty(request.getDatabaseName())) {
      throw new StoreException("database name is invalid");
    }
    if (StringUtils.isEmpty(request.getTableName())) {
      throw new StoreException("table name is invalid");
    }
  }
}
