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
package org.carbondata.core.carbon.metadata.schema.aggregator;

import java.io.Serializable;

public class Aggregator implements Serializable{

  /**
   * serialization id
   */
  private static final long serialVersionUID = 8979853601293570345L;
  
  /**
   * type of aggregate function selected 
   */
  private AggregateFunction aggregateFunction;
  
  /**
   * custom class name if aggregate type is custom
   */
  private String customClassName;

  /**
   * @return the aggregateFunction
   */
  public AggregateFunction getAggregateFunction() {
    return aggregateFunction;
  }

  /**
   * @param aggregateFunction the aggregateFunction to set
   */
  public void setAggregateFunction(AggregateFunction aggregateFunction) {
    this.aggregateFunction = aggregateFunction;
  }

  /**
   * @return the customClassName
   */
  public String getCustomClassName() {
    return customClassName;
  }

  /**
   * @param customClassName the customClassName to set
   */
  public void setCustomClassName(String customClassName) {
    this.customClassName = customClassName;
  }
  

}
