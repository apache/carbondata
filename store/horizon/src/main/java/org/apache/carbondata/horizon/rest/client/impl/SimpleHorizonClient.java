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

package org.apache.carbondata.horizon.rest.client.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.horizon.rest.client.HorizonClient;
import org.apache.carbondata.horizon.rest.model.view.CreateTableRequest;
import org.apache.carbondata.horizon.rest.model.view.DropTableRequest;
import org.apache.carbondata.horizon.rest.model.view.LoadRequest;
import org.apache.carbondata.horizon.rest.model.view.SelectRequest;
import org.apache.carbondata.horizon.rest.model.view.SelectResponse;
import org.apache.carbondata.store.api.exception.StoreException;

import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

public class SimpleHorizonClient implements HorizonClient {

  private RestTemplate restTemplate;
  private String serviceUri;

  public SimpleHorizonClient(String serviceUri) {
    this.serviceUri = serviceUri;
    this.restTemplate = new RestTemplate();
  }

  @Override
  public void createTable(CreateTableRequest create) throws IOException, StoreException {
    Objects.requireNonNull(create);
    restTemplate.postForEntity(serviceUri + "/table/create", create, String.class);
  }

  @Override
  public void dropTable(DropTableRequest drop) throws IOException {
    Objects.requireNonNull(drop);
    restTemplate.postForEntity(serviceUri + "/table/drop", drop, String.class);
  }

  @Override
  public void loadData(LoadRequest load) throws IOException, StoreException {
    Objects.requireNonNull(load);
    restTemplate.postForEntity(serviceUri + "/table/load", load, String.class);
  }

  @Override
  public List<CarbonRow> select(SelectRequest select) throws IOException, StoreException {
    Objects.requireNonNull(select);
    ResponseEntity<SelectResponse> response =
        restTemplate.postForEntity(serviceUri + "/table/select", select, SelectResponse.class);
    Object[][] rows = Objects.requireNonNull(response.getBody()).getRows();
    List<CarbonRow> output = new ArrayList<>(rows.length);
    for (Object[] row : rows) {
      output.add(new CarbonRow(row));
    }
    return output;
  }

  @Override
  public List<CarbonRow> sql(String sqlString) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws IOException {

  }
}
