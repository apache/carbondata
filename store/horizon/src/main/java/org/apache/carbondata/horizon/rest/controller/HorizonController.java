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
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.hadoop.readsupport.impl.CarbonRowReadSupport;
import org.apache.carbondata.horizon.antlr.Parser;
import org.apache.carbondata.horizon.rest.model.validate.RequestValidator;
import org.apache.carbondata.horizon.rest.model.view.CreateTableRequest;
import org.apache.carbondata.horizon.rest.model.view.DropTableRequest;
import org.apache.carbondata.horizon.rest.model.view.LoadRequest;
import org.apache.carbondata.horizon.rest.model.view.SelectRequest;
import org.apache.carbondata.horizon.rest.model.view.SelectResponse;
import org.apache.carbondata.sdk.store.conf.StoreConf;
import org.apache.carbondata.sdk.store.descriptor.LoadDescriptor;
import org.apache.carbondata.sdk.store.descriptor.ScanDescriptor;
import org.apache.carbondata.sdk.store.descriptor.TableDescriptor;
import org.apache.carbondata.sdk.store.descriptor.TableIdentifier;
import org.apache.carbondata.sdk.store.exception.CarbonException;
import org.apache.carbondata.store.devapi.InternalCarbonStore;
import org.apache.carbondata.store.devapi.InternalCarbonStoreFactory;
import org.apache.carbondata.store.devapi.ResultBatch;
import org.apache.carbondata.store.devapi.ScanUnit;
import org.apache.carbondata.store.devapi.Scanner;

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

  private InternalCarbonStore store;

  public HorizonController() throws CarbonException {
    String storeFile = System.getProperty("carbonstore.conf.file");
    StoreConf storeConf = new StoreConf();
    try {
      storeConf.conf(StoreConf.STORE_LOCATION, CarbonProperties.getStorePath())
          .conf(StoreConf.MASTER_HOST, InetAddress.getLocalHost().getHostAddress())
          .conf(StoreConf.STORE_PORT, CarbonProperties.getSearchMasterPort())
          .conf(StoreConf.WORKER_HOST, InetAddress.getLocalHost().getHostAddress())
          .conf(StoreConf.WORKER_PORT, CarbonProperties.getSearchWorkerPort())
          .conf(StoreConf.WORKER_CORE_NUM, 2);

      if (storeFile != null && FileFactory.isFileExist(storeFile)) {
        storeConf.load(storeFile);
      }

      store = InternalCarbonStoreFactory.getStore("HorizonController", storeConf);
    } catch (IOException e) {
      throw new CarbonException(e);
    }
  }

  @RequestMapping(value = "echo")
  public ResponseEntity<String> echo(@RequestParam(name = "name") String name) {
    return new ResponseEntity<>(String.valueOf("Hello " + name), HttpStatus.OK);
  }

  @RequestMapping(value = "/table/create", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String> createTable(
      @RequestBody CreateTableRequest request) throws CarbonException {
    RequestValidator.validateTable(request);
    TableDescriptor tableDescriptor = request.convertToDto();
    store.createTable(tableDescriptor);
    return new ResponseEntity<>(String.valueOf(true), HttpStatus.OK);
  }

  @RequestMapping(value = "/table/drop", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String> dropTable(
      @RequestBody DropTableRequest request) throws CarbonException {
    RequestValidator.validateDrop(request);
    store.dropTable(new TableIdentifier(request.getTableName(), request.getDatabaseName()));
    return new ResponseEntity<>(String.valueOf(true), HttpStatus.OK);
  }

  @RequestMapping(value = "/table/load", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String> load(@RequestBody LoadRequest request)
      throws CarbonException, IOException {
    RequestValidator.validateLoad(request);
    LoadDescriptor loadDescriptor = request.convertToDto();
    store.loadData(loadDescriptor);
    return new ResponseEntity<>(String.valueOf(true), HttpStatus.OK);
  }

  @RequestMapping(value = "/table/select", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<SelectResponse> select(@RequestBody SelectRequest request)
      throws CarbonException {
    long start = System.currentTimeMillis();
    RequestValidator.validateSelect(request);
    TableIdentifier table = new TableIdentifier(request.getTableName(), request.getDatabaseName());
    CarbonTable carbonTable = store.getCarbonTable(table);
    Expression expression = Parser.parseFilter(request.getFilter(), carbonTable);
    ScanDescriptor scanDescriptor = new ScanDescriptor(
        table, request.getSelect(), expression, request.getLimit());
    Scanner<CarbonRow> scanner = store.newScanner(table, scanDescriptor, null,
        CarbonRowReadSupport.class);
    List<ScanUnit> scanUnits = scanner.prune(table, expression);
    ArrayList<Object[]> output = new ArrayList<>();
    for (ScanUnit scanUnit : scanUnits) {
      Iterator<? extends ResultBatch<CarbonRow>> iterator = scanner.scan(scanUnit);
      while (iterator.hasNext()) {
        ResultBatch<CarbonRow> rows = iterator.next();
        while (rows.hasNext()) {
          output.add(rows.next().getData());
        }
      }
    }

    long end = System.currentTimeMillis();
    LOGGER.audit("[" + request.getRequestId() + "] HorizonController select " +
        request.getDatabaseName() + "." + request.getTableName() +
        ", take time: " + (end - start) + " ms");

    return new ResponseEntity<>(
        new SelectResponse(request, "SUCCESS", output), HttpStatus.OK);
  }

}
