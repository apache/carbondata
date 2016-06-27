/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.carbondata.common.ext;

import java.util.UUID;

import org.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema;
import org.carbondata.core.service.ColumnUniqueIdService;

/**
 * It returns unique id given column
 */
public class ColumnUniqueIdGenerator implements ColumnUniqueIdService {

  private static ColumnUniqueIdService columnUniqueIdService = new ColumnUniqueIdGenerator();

  @Override public String generateUniqueId(String schemaName, ColumnSchema columnSchema) {
    return UUID.randomUUID().toString();
  }

  public static ColumnUniqueIdService getInstance() {
    return columnUniqueIdService;
  }

}
