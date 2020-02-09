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

/**
 * Event listener interface which describes the possible events
 */
public abstract class OperationEventListener {

  /**
   * Called on a specified event occurrence
   *
   * @param event
   * @param operationContext
   */
  protected abstract void onEvent(Event event, OperationContext operationContext);

  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof OperationEventListener)) {
      return false;
    }
    return getComparisonName().equals(((OperationEventListener) obj).getComparisonName());
  }

  private String getComparisonName() {
    return getClass().getName();
  }

  @Override
  public int hashCode() {
    return getClass().hashCode();
  }
}
