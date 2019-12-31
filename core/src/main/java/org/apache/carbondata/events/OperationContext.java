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

package org.apache.carbondata.events;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * One OperationContext per one operation.
 * OperationContext active till operation execution completes
 */
public class OperationContext implements Serializable {

  private static final long serialVersionUID = -8808813829717624986L;

  private transient Map<String, Object> operationProperties = new HashMap<>();

  public Map<String, Object> getProperties() {
    return operationProperties;
  }

  public void setProperty(String key, Object value) {
    this.operationProperties.put(key, value);
  }

  public Object getProperty(String key) {
    return this.operationProperties.get(key);
  }
}
