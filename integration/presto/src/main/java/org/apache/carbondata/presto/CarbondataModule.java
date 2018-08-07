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

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

import org.apache.carbondata.presto.impl.CarbonTableConfig;
import org.apache.carbondata.presto.impl.CarbonTableReader;

import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class CarbondataModule implements Module {

  private final String connectorId;
  private final TypeManager typeManager;

  public CarbondataModule(String connectorId, TypeManager typeManager) {
    this.connectorId = requireNonNull(connectorId, "connector id is null");
    this.typeManager = requireNonNull(typeManager, "typeManager is null");
  }

  @Override public void configure(Binder binder) {
    binder.bind(TypeManager.class).toInstance(typeManager);

    binder.bind(CarbondataConnectorId.class).toInstance(new CarbondataConnectorId(connectorId));
    binder.bind(CarbondataMetadata.class).in(Scopes.SINGLETON);
    binder.bind(CarbonTableReader.class).in(Scopes.SINGLETON);
    binder.bind(ConnectorSplitManager.class).to(CarbondataSplitManager.class).in(Scopes.SINGLETON);
    binder.bind(ConnectorPageSourceProvider.class).to(CarbondataPageSourceProvider.class)
        .in(Scopes.SINGLETON);
    binder.bind(CarbondataHandleResolver.class).in(Scopes.SINGLETON);
    configBinder(binder).bindConfig(CarbonTableConfig.class);
  }

  public static final class TypeDeserializer extends FromStringDeserializer<Type> {
    private final TypeManager typeManager;

    @Inject public TypeDeserializer(TypeManager typeManager) {
      super(Type.class);
      this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override protected Type _deserialize(String value, DeserializationContext context) {
      Type type = typeManager.getType(parseTypeSignature(value));
      checkArgument(type != null, "Unknown type %s", value);
      return type;
    }
  }
}
