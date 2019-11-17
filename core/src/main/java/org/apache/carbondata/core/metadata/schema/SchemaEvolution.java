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

package org.apache.carbondata.core.metadata.schema;

import java.io.Serializable;
import java.util.List;

/**
 * Persisting schema restructuring information;
 */
public class SchemaEvolution implements Serializable {

  /**
   * serialization version
   */
  private static final long serialVersionUID = 8186224567517679868L;

  /**
   * list of schema evolution entry
   */
  private List<SchemaEvolutionEntry> schemaEvolutionEntryList;

  /**
   * @return the schemaEvolutionEntryList
   */
  public List<SchemaEvolutionEntry> getSchemaEvolutionEntryList() {
    return schemaEvolutionEntryList;
  }

  /**
   * @param schemaEvolutionEntryList the schemaEvolutionEntryList to set
   */
  public void setSchemaEvolutionEntryList(List<SchemaEvolutionEntry> schemaEvolutionEntryList) {
    this.schemaEvolutionEntryList = schemaEvolutionEntryList;
  }

}
