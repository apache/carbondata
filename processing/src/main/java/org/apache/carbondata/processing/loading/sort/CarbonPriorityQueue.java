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

package org.apache.carbondata.processing.loading.sort;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Comparator;
import java.util.PriorityQueue;

import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException;

/**
 * This class is used to optimize sort step performance while getting row from heap,
 * including intermediate merge and final sort merge in loading process,
 * by accessing member/method in PriorityQueue
 */
public class CarbonPriorityQueue<E> extends PriorityQueue<E> {
  private transient Field queueField;
  private transient Method siftDownMethod;
  private static final long serialVersionUID = 1L;

  public CarbonPriorityQueue(int initialCapacity) {
    super(initialCapacity);
    init();
  }

  public CarbonPriorityQueue(int initialCapacity, Comparator<? super E> comparator) {
    super(initialCapacity, comparator);
    init();
  }

  private void init() {
    try {
      queueField = PriorityQueue.class.getDeclaredField("queue");
      queueField.setAccessible(true);
      siftDownMethod = PriorityQueue.class.getDeclaredMethod("siftDown", int.class, Object.class);
      siftDownMethod.setAccessible(true);
    } catch (ReflectiveOperationException e) {
      throw new CarbonDataLoadingException("Reflective operation failed", e);
    }
  }

  public void siftTopDown() {
    try {
      Object topNode = ((Object[]) queueField.get(this))[0];
      siftDownMethod.invoke(this, 0, topNode);
    } catch (ReflectiveOperationException e) {
      throw new CarbonDataLoadingException("Reflective operation failed", e);
    }
  }
}
