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

import java.util.UUID;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.horizon.rest.model.descriptor.LoadDescriptor;
import org.apache.carbondata.horizon.rest.model.descriptor.SelectDescriptor;
import org.apache.carbondata.horizon.rest.model.descriptor.TableDescriptor;
import org.apache.carbondata.horizon.rest.model.validate.RequestValidator;
import org.apache.carbondata.horizon.rest.model.view.CreateTableRequest;
import org.apache.carbondata.horizon.rest.model.view.LoadRequest;
import org.apache.carbondata.horizon.rest.model.view.SelectRequest;
import org.apache.carbondata.horizon.rest.model.view.SelectResponse;
import org.apache.carbondata.horizon.rest.service.HorizonService;
import org.apache.carbondata.store.exception.StoreException;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HorizonController {

  private static LogService LOGGER =
      LogServiceFactory.getLogService(HorizonController.class.getName());

  private HorizonService service;

  public HorizonController() {
    service = HorizonService.getInstance();
  }

  @RequestMapping(value = "/table/create", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String> createTable(
      @RequestBody CreateTableRequest request) throws StoreException {
    RequestValidator.validateTable(request);
    TableDescriptor tableDescriptor = request.convertToDto();
    boolean result = service.createTable(tableDescriptor);
    return new ResponseEntity<>(String.valueOf(result), HttpStatus.OK);
  }

  @RequestMapping(value = "/table/load", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String> load(@RequestBody LoadRequest request) throws StoreException {
    RequestValidator.validateLoad(request);
    LoadDescriptor loadDescriptor = request.convertToDto();
    boolean result = service.loadData(loadDescriptor);
    return new ResponseEntity<>(String.valueOf(result), HttpStatus.OK);
  }


  @RequestMapping(value = "/table/select", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<SelectResponse> select(@RequestBody SelectRequest request)
      throws StoreException {
    long start = System.currentTimeMillis();
    RequestValidator.validateSelect(request);
    SelectDescriptor selectDescriptor = request.convertToDto();
    selectDescriptor.setId(UUID.randomUUID().toString());
    CarbonRow[] result = service.select(selectDescriptor);
    Object[][] newResult = new Object[result.length][];
    for (int i = newResult.length - 1; i >= 0; i--) {
      newResult[i] = result[i].getData();
    }
    long end = System.currentTimeMillis();
    LOGGER.audit("[" + selectDescriptor.getId() +  "] HorizonController select " +
        request.getDatabaseName() + "." + request.getTableName() +
        ", take time: " + (end - start) + " ms");
    return new ResponseEntity<>(
        new SelectResponse(selectDescriptor.getId(), newResult), HttpStatus.OK);
  }

}
