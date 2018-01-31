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

package org.apache.carbondata.core.datamap.dev;

import java.util.List;
import java.util.Objects;

import org.apache.carbondata.core.indexstore.Blocklet;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;

public class DataMapLiteralNodeExpression extends DataMapExpression {
  protected Object value;
  protected DataType dataType;

  public DataMapLiteralNodeExpression (Object value, DataType dataType) {
    this.value = value;
    this.dataType = dataType;
  }

  public Object getValue () {
    return value;
  }

  public DataType getDataType () {
    return dataType;
  }

  @Override public DataMapExpressionType getDataMapExpressionType() {
    return DataMapExpressionType.LITERAL_DATAMAP;
  }

  @Override public List<Blocklet> prune(FilterResolverIntf filterExp) {
    return null;
  }

}
