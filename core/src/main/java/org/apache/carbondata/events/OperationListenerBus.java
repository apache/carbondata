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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.carbondata.core.constants.CarbonCommonConstants;

/**
 * An event bus which posts events to its listeners.
 */
public class OperationListenerBus {

  /**
   * singleton instance
   */
  private static final OperationListenerBus INSTANCE = new OperationListenerBus();

  /**
   * Event map to hold all listeners corresponding to an event
   */
  protected Map<String, List<OperationEventListener>> eventMap =
      new ConcurrentHashMap<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

  /**
   * @return
   */
  public static OperationListenerBus getInstance() {
    return INSTANCE;
  }

  /**
   * Add a listener to listen events. This method is thread-safe and can be called in any thread.
   *
   * @param eventClass
   * @param operationEventListener
   */
  public OperationListenerBus addListener(Class<? extends Event> eventClass,
      OperationEventListener operationEventListener) {

    String eventType = eventClass.getName();
    List<OperationEventListener> operationEventListeners = eventMap.get(eventType);
    if (null == operationEventListeners) {
      operationEventListeners = new CopyOnWriteArrayList<>();
      eventMap.put(eventType, operationEventListeners);
    }
    operationEventListeners.add(operationEventListener);
    return INSTANCE;
  }

  /**
   * Removes a given map from the event. This method is thread-safe and can be called
   * in any thread.
   *
   * @param eventType
   */
  public void removeEvent(String eventType) {
    eventMap.remove(eventType);
  }

  /**
   * Removes a given listener for an event. This method is thread-safe and can be called
   * in any thread.
   *
   * @param eventType
   * @param operationEventListener
   */
  public void removeListener(String eventType, OperationEventListener operationEventListener) {
    List<OperationEventListener> operationEventListeners = eventMap.get(eventType);
    if (null != operationEventListeners) {
      operationEventListeners.remove(operationEventListener);
    }
  }

  /**
   * Notify all registered listeners on occurrence of an event
   * Should be used for stateless events which cannot be mapped to a operation
   *
   * @param event
   */
  public void fireEvent(Event event) throws Exception {
    fireEvent(event, new OperationContext());
  }

  /**
   * Notify all registered listeners on occurrence of an event
   *
   * @param event
   * @param operationContext
   */
  public void fireEvent(Event event, OperationContext operationContext) throws Exception {
    if (operationContext == null) {
      throw new Exception("OperationContext cannot be null");
    }
    List<OperationEventListener> operationEventListeners = eventMap.get(event.getEventType());
    if (null != operationEventListeners) {
      for (OperationEventListener operationEventListener : operationEventListeners) {
        operationEventListener.onEvent(event, operationContext);
      }
    }
  }
}
