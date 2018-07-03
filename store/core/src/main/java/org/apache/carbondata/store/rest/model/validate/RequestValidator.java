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

package org.apache.carbondata.store.rest.model.validate;

import org.apache.carbondata.store.exception.StoreException;
import org.apache.carbondata.store.rest.model.vo.LoadRequest;
import org.apache.carbondata.store.rest.model.vo.SelectRequest;
import org.apache.carbondata.store.rest.model.vo.TableRequest;

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

  public static void validateTable(TableRequest request) throws StoreException {
    if (request == null) {
      throw new StoreException("Table should not be null");
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
      throw new StoreException("Load should not be null");
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
}
