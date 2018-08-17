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
import java.util.stream.IntStream;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.horizon.rest.model.validate.RequestValidator;
import org.apache.carbondata.horizon.rest.model.view.SqlRequest;
import org.apache.carbondata.horizon.rest.model.view.SqlResponse;
import org.apache.carbondata.horizon.rest.sql.SparkSqlWrapper;
import org.apache.carbondata.sdk.store.exception.CarbonException;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
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

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(SqlHorizonController.class.getName());

  @RequestMapping(value = "/table/sql", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<SqlResponse> sql(@RequestBody SqlRequest request) throws CarbonException {
    RequestValidator.validateSql(request);
    List<Row> rows;
    Dataset<Row> sqlDataFrame = null;
    try {
      sqlDataFrame = SparkSqlWrapper.sql(SqlHorizon.getSession(),
              request.getSqlStatement());
      rows = sqlDataFrame.collectAsList();
    } catch (AnalysisException e) {
      LOGGER.error(e);
      throw new CarbonException(e.getSimpleMessage());
    } catch (Exception e) {
      LOGGER.error(e);
      throw new CarbonException(e.getMessage());
    }
    final String[] fieldNames = sqlDataFrame.schema().fieldNames();
    Object[][] responseData = new Object[0][];
    if (rows.size() > 0) {
      final Object[][] result = new Object[rows.size() + 1][fieldNames.length];
      System.arraycopy(fieldNames, 0, result[0], 0, fieldNames.length);
      IntStream.range(0, rows.size()).forEach(index ->
          IntStream.range(0, fieldNames.length).forEach(col ->
                result[index + 1][col] = rows.get(index).get(col)));
      responseData = result;
    }

    return new ResponseEntity<>(
        new SqlResponse(request, "SUCCESS", responseData), HttpStatus.OK);
  }

  @RequestMapping(value = "echosql")
  public ResponseEntity<String> echosql(@RequestParam(name = "name") String name) {
    return new ResponseEntity<>(String.valueOf("Welcome to SQL, " + name), HttpStatus.OK);
  }
}
