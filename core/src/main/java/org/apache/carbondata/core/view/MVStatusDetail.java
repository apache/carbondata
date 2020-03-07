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

package org.apache.carbondata.core.view;

import java.io.Serializable;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.core.metadata.schema.table.RelationIdentifier;

/**
 * Status of mv
 */
@InterfaceAudience.Internal
public class MVStatusDetail implements Serializable {

  private static final long serialVersionUID = 1570997199499681821L;

  private RelationIdentifier identifier;

  private MVStatus status;

  MVStatusDetail(RelationIdentifier identifier, MVStatus status) {
    this.identifier = identifier;
    this.status = status;
  }

  public RelationIdentifier getIdentifier() {
    return identifier;
  }

  public void setIdentifier(RelationIdentifier name) {
    this.identifier = name;
  }

  public MVStatus getStatus() {
    return status;
  }

  public boolean isEnabled() {
    return status == MVStatus.ENABLED;
  }

  public void setStatus(MVStatus status) {
    this.status = status;
  }
}
