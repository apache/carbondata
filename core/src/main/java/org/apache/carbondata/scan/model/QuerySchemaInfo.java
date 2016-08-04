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
package org.apache.carbondata.scan.model;

import java.io.Serializable;

import org.apache.carbondata.core.keygenerator.KeyGenerator;

public class QuerySchemaInfo implements Serializable {

  private int[] maskedByteIndexes;

  private KeyGenerator keyGenerator;

  private QueryDimension[] queryDimensions;

  private QueryMeasure[] queryMeasures;

  private int[] queryOrder;

  private int[] queryReverseOrder;

  public int[] getMaskedByteIndexes() {
    return maskedByteIndexes;
  }

  public void setMaskedByteIndexes(int[] maskedByteIndexes) {
    this.maskedByteIndexes = maskedByteIndexes;
  }

  public KeyGenerator getKeyGenerator() {
    return keyGenerator;
  }

  public void setKeyGenerator(KeyGenerator keyGenerator) {
    this.keyGenerator = keyGenerator;
  }

  public QueryDimension[] getQueryDimensions() {
    return queryDimensions;
  }

  public void setQueryDimensions(QueryDimension[] queryDimensions) {
    this.queryDimensions = queryDimensions;
  }

  public QueryMeasure[] getQueryMeasures() {
    return queryMeasures;
  }

  public void setQueryMeasures(QueryMeasure[] queryMeasures) {
    this.queryMeasures = queryMeasures;
  }

  public int[] getQueryOrder() {
    return queryOrder;
  }

  public void setQueryOrder(int[] queryOrder) {
    this.queryOrder = queryOrder;
  }

  public int[] getQueryReverseOrder() {
    return queryReverseOrder;
  }

  public void setQueryReverseOrder(int[] queryReverseOrder) {
    this.queryReverseOrder = queryReverseOrder;
  }
}
