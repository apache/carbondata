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

import org.apache.carbondata.core.indexstore.Blocklet;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;

public class DataMapAndNodeExpression extends DataMapExpression {
  protected DataMapExpression left;
  protected DataMapExpression right;

  public DataMapAndNodeExpression (DataMapExpression left, DataMapExpression right) {
    this.left = left;
    this.right = right;
  }

  public DataMapExpression getLeft () {
    return left;
  }

  public DataMapExpression getRight () {
    return right;
  }

  public void setLeft (DataMapExpression left) {
    this.left = left;
  }

  public void setRight (DataMapExpression right) {
    this.right = right;
  }

  @Override public DataMapExpressionType getDataMapExpressionType() {
    return DataMapExpressionType.AND_DATAMAP;
  }

  @Override public List<Blocklet> prune(FilterResolverIntf filterExp) {
    return null;
  }

}
