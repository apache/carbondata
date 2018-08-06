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

package org.apache.carbondata.presto;
import java.util.List;

import static java.util.Objects.requireNonNull;

import org.apache.carbondata.presto.impl.CarbonLocalMultiBlockSplit;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

public class CarbondataSplit implements ConnectorSplit {

  private final String connectorId;
  private final SchemaTableName schemaTableName;
  private final TupleDomain<ColumnHandle> constraints;
  private final CarbonLocalMultiBlockSplit localInputSplit;
  private final List<CarbondataColumnConstraint> rebuildConstraints;
  private final ImmutableList<HostAddress> addresses;
  private final String queryId;
  private final long index;

  @JsonCreator public CarbondataSplit(@JsonProperty("connectorId") String connectorId,
      @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
      @JsonProperty("constraints") TupleDomain<ColumnHandle> constraints,
      @JsonProperty("localInputSplit") CarbonLocalMultiBlockSplit localInputSplit,
      @JsonProperty("rebuildConstraints") List<CarbondataColumnConstraint> rebuildConstraints,
      @JsonProperty("queryId") String queryId,
      @JsonProperty("index") long index) {
    this.connectorId = requireNonNull(connectorId, "connectorId is null");
    this.schemaTableName = requireNonNull(schemaTableName, "schemaTable is null");
    this.constraints = requireNonNull(constraints, "constraints is null");
    this.localInputSplit = requireNonNull(localInputSplit, "localInputSplit is null");
    this.rebuildConstraints = requireNonNull(rebuildConstraints, "rebuildConstraints is null");
    this.addresses = ImmutableList.of();
    this.queryId = queryId;
    this.index = index;
  }

  @JsonProperty public String getConnectorId() {
    return connectorId;
  }

  @JsonProperty public SchemaTableName getSchemaTableName() {
    return schemaTableName;
  }

  @JsonProperty public TupleDomain<ColumnHandle> getConstraints() {
    return constraints;
  }

  @JsonProperty public CarbonLocalMultiBlockSplit getLocalInputSplit() {
    return localInputSplit;
  }

  @JsonProperty public List<CarbondataColumnConstraint> getRebuildConstraints() {
    return rebuildConstraints;
  }

  @Override public boolean isRemotelyAccessible() {
    return true;
  }

  @Override public List<HostAddress> getAddresses() {
    return addresses;
  }

  @Override public Object getInfo() {
    return this;
  }

  @JsonProperty public String getQueryId() {
    return queryId;
  }

  @JsonProperty public long getIndex() {
    return index;
  }
}

