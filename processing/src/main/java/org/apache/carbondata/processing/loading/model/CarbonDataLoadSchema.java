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

package org.apache.carbondata.processing.loading.model;

import java.io.Serializable;

import org.apache.carbondata.core.metadata.schema.table.CarbonTable;

/**
 * Wrapper Data Load Schema object which will be used to
 * support relation while data loading
 */
public class CarbonDataLoadSchema implements Serializable {

  /**
   * default serializer
   */
  private static final long serialVersionUID = 1L;

  /**
   * CarbonTable info
   */
  private CarbonTable carbonTable;

  /**
   * Used to determine if the dataTypes have already been updated or not.
   */
  private transient boolean updatedDataTypes;

  /**
   * CarbonDataLoadSchema constructor which takes CarbonTable
   *
   * @param carbonTable
   */
  public CarbonDataLoadSchema(CarbonTable carbonTable) {
    this.carbonTable = carbonTable;
  }

  /**
   * get carbontable
   *
   * @return carbonTable
   */
  public CarbonTable getCarbonTable() {
    if (!updatedDataTypes) {
      carbonTable = CarbonTable.buildFromTableInfo(carbonTable.getTableInfo());
      updatedDataTypes = true;
    }
    return carbonTable;
  }

}