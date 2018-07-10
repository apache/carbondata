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

package org.apache.carbondata.horizon.rest.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.horizon.rest.client.impl.SimpleHorizonClient;
import org.apache.carbondata.horizon.rest.model.view.SqlResponse;

import org.springframework.http.ResponseEntity;

/**
 * REST Client to send SQL statement to Horizon service
 */
@InterfaceAudience.User
@InterfaceStability.Unstable
public class SqlHorizonClient extends SimpleHorizonClient {

  public SqlHorizonClient(String serviceUri) {
    super(serviceUri);
  }

  @Override
  public List<CarbonRow> sql(String sqlString) throws IOException {
    Objects.requireNonNull(sqlString);
    ResponseEntity<SqlResponse> response =
        restTemplate.postForEntity(serviceUri + "/table/sql", sqlString, SqlResponse.class);
    Object[][] rows = Objects.requireNonNull(response.getBody()).getRows();
    List<CarbonRow> output = new ArrayList<>(rows.length);
    for (Object[] row : rows) {
      output.add(new CarbonRow(row));
    }
    return output;
  }

}
