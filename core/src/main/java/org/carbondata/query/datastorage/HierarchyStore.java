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

package org.carbondata.query.datastorage;

import org.carbondata.core.carbon.CarbonDef;
import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.query.datastorage.streams.DataInputStream;

public class HierarchyStore {
  /**
   *
   */
  private String hierName;

  /**
   *
   */
  private String tableName;

  /**
   * Fact table Name
   */
  private String factTableName;

  private HierarchyBTreeStore btreeStore;

  /**
   *
   */
  private CarbonDef.Hierarchy carbonHierarchy;

  /**
   *
   */
  private String dimeName;

  public HierarchyStore(CarbonDef.Hierarchy carbonHierarchy, String factTableName,
      String dimensionName) {
    this.dimeName = dimensionName;
    this.hierName = carbonHierarchy.name == null ? dimensionName : carbonHierarchy.name;

    this.carbonHierarchy = carbonHierarchy;
    tableName = hierName.replaceAll(" ", "_") + ".hierarchy";
    this.factTableName = factTableName;
  }

  /**
   * Getter for hierarchy
   */
  public CarbonDef.Hierarchy getCarbonHierarchy() {
    return carbonHierarchy;
  }

  /**
   * Getter for dimension name in which this hierarchy present
   */
  public String getDimensionName() {
    return dimeName;
  }

  /**
   * @param keyGen
   * @param factStream
   */
  public void build(KeyGenerator keyGen, DataInputStream factStream) {
    btreeStore = new HierarchyBTreeStore(keyGen);
    btreeStore.build(factStream);
  }

  /**
   * @return the hierName
   */
  public String getHierName() {
    return hierName;
  }

  /**
   * @return
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * @return Returns the factTableName.
   */
  public String getFactTableName() {
    return factTableName;
  }
}
