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

package org.carbondata.query.queryinterface.query.metadata;

/**
 * Represents DSL transformation which can be added to <code>CarbonQuery<code>
 */
public class DSLTransformation {
  /**
   * Name of the transformation.
   */
  private String name;

  /**
   * DSL script
   */
  private String dslExpression;

  /**
   * The new column name if the DSL script is adding one new column
   */
  private String newColumnName;

  /**
   * Flag to set if the transformation script will resulting to add a new column in
   * the original result.
   */
  private boolean addAsColumn;

  public DSLTransformation(String name, String dslExpression, String newColumnName,
      boolean addAsColumn) {
    this.name = name;
    this.dslExpression = dslExpression;
    this.newColumnName = newColumnName;
    this.addAsColumn = addAsColumn;
  }

  /**
   * @return Returns the name.
   */
  public String getName() {
    return name;
  }

  /**
   * @param name The name to set.
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * @return Returns the dslExpression.
   */
  public String getDslExpression() {
    return dslExpression;
  }

  /**
   * @param dslExpression The dslExpression to set.
   */
  public void setDslExpression(String dslExpression) {
    this.dslExpression = dslExpression;
  }

  /**
   * @return Returns the newColumnName.
   */
  public String getNewColumnName() {
    return newColumnName;
  }

  /**
   * @param newColumnName The newColumnName to set.
   */
  public void setNewColumnName(String newColumnName) {
    this.newColumnName = newColumnName;
  }

  /**
   * @return Returns the addAsColumn.
   */
  public boolean isAddAsColumn() {
    return addAsColumn;
  }

  /**
   * @param addAsColumn The addAsColumn to set.
   */
  public void setAddAsColumn(boolean addAsColumn) {
    this.addAsColumn = addAsColumn;
  }

}
