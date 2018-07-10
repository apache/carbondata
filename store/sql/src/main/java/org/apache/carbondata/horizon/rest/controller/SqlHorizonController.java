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

package org.apache.carbondata.horizon.rest.controller;

import java.util.List;

import org.apache.carbondata.horizon.rest.model.validate.RequestValidator;
import org.apache.carbondata.horizon.rest.model.view.SqlRequest;
import org.apache.carbondata.horizon.rest.model.view.SqlResponse;
import org.apache.carbondata.store.api.exception.StoreException;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Row;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class SqlHorizonController {

  @RequestMapping(value = "/table/sql", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<SqlResponse> sql(@RequestBody SqlRequest request) throws StoreException {
    RequestValidator.validateSql(request);
    List<Row> rows;
    try {
      rows = SqlHorizon.getSession().sql(request.getSqlStatement()).collectAsList();
    } catch (Exception e) {
      if (e instanceof AnalysisException) {
        throw new StoreException(((AnalysisException) e).getSimpleMessage());
      }
      throw new StoreException(e);
    }
    Object[][] result = new Object[rows.size()][];
    for (int i = 0; i < rows.size(); i++) {
      Row row = rows.get(i);
      result[i] = new Object[row.size()];
      for (int j = 0; j < row.size(); j++) {
        result[i][j] = row.get(j);
      }
    }

    return new ResponseEntity<>(
        new SqlResponse(request, "SUCCESS", result), HttpStatus.OK);
  }

  @RequestMapping(value = "echosql")
  public ResponseEntity<String> echosql(@RequestParam(name = "name") String name) {
    return new ResponseEntity<>(String.valueOf("Welcome to SQL, " + name), HttpStatus.OK);
  }
}
