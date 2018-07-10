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

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.horizon.antlr.Parser;
import org.apache.carbondata.horizon.rest.model.validate.RequestValidator;
import org.apache.carbondata.horizon.rest.model.view.CreateTableRequest;
import org.apache.carbondata.horizon.rest.model.view.DropTableRequest;
import org.apache.carbondata.horizon.rest.model.view.LoadRequest;
import org.apache.carbondata.horizon.rest.model.view.SelectRequest;
import org.apache.carbondata.horizon.rest.model.view.SelectResponse;
import org.apache.carbondata.store.api.CarbonStore;
import org.apache.carbondata.store.api.CarbonStoreFactory;
import org.apache.carbondata.store.api.conf.StoreConf;
import org.apache.carbondata.store.api.descriptor.LoadDescriptor;
import org.apache.carbondata.store.api.descriptor.SelectDescriptor;
import org.apache.carbondata.store.api.descriptor.TableDescriptor;
import org.apache.carbondata.store.api.descriptor.TableIdentifier;
import org.apache.carbondata.store.api.exception.StoreException;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HorizonController {

  private static LogService LOGGER =
      LogServiceFactory.getLogService(HorizonController.class.getName());

  private CarbonStore store;

  public HorizonController() throws StoreException {
    String storeFile = System.getProperty("carbonstore.conf.file");
    store = CarbonStoreFactory.getDistributedStore("GlobalStore", new StoreConf(storeFile));
  }

  @RequestMapping(value = "echo")
  public ResponseEntity<String> echo(@RequestParam(name = "name") String name) {
    return new ResponseEntity<>(String.valueOf("Hello " + name), HttpStatus.OK);
  }

  @RequestMapping(value = "/table/create", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String> createTable(
      @RequestBody CreateTableRequest request) throws StoreException, IOException {
    RequestValidator.validateTable(request);
    TableDescriptor tableDescriptor = request.convertToDto();
    store.createTable(tableDescriptor);
    return new ResponseEntity<>(String.valueOf(true), HttpStatus.OK);
  }

  @RequestMapping(value = "/table/drop", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String> dropTable(
      @RequestBody DropTableRequest request) throws StoreException, IOException {
    RequestValidator.validateDrop(request);
    store.dropTable(new TableIdentifier(request.getTableName(), request.getDatabaseName()));
    return new ResponseEntity<>(String.valueOf(true), HttpStatus.OK);
  }

  @RequestMapping(value = "/table/load", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String> load(@RequestBody LoadRequest request)
      throws StoreException, IOException {
    RequestValidator.validateLoad(request);
    LoadDescriptor loadDescriptor = request.convertToDto();
    store.loadData(loadDescriptor);
    return new ResponseEntity<>(String.valueOf(true), HttpStatus.OK);
  }

  @RequestMapping(value = "/table/select", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<SelectResponse> select(@RequestBody SelectRequest request)
      throws StoreException, IOException {
    long start = System.currentTimeMillis();
    RequestValidator.validateSelect(request);
    TableIdentifier table = new TableIdentifier(request.getTableName(), request.getDatabaseName());
    CarbonTable carbonTable = store.getTable(table);
    Expression expression = Parser.parseFilter(request.getFilter(), carbonTable);
    SelectDescriptor selectDescriptor = new SelectDescriptor(
        table, request.getSelect(), expression, request.getLimit());
    List<CarbonRow> result = store.select(selectDescriptor);
    Iterator<CarbonRow> iterator = result.iterator();
    Object[][] output = new Object[result.size()][];
    int i = 0;
    while (iterator.hasNext()) {
      output[i] = (iterator.next().getData());
      i++;
    }
    long end = System.currentTimeMillis();
    LOGGER.audit("[" + request.getRequestId() +  "] HorizonController select " +
        request.getDatabaseName() + "." + request.getTableName() +
        ", take time: " + (end - start) + " ms");

    return new ResponseEntity<>(
        new SelectResponse(request, "SUCCESS", output), HttpStatus.OK);
  }

}
