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
import java.util.PriorityQueue;

import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException;

/**
 * This class is used to optimize sort step performance while getting row from heap,
 * including intermediate merge and final sort merge in loading process,
 * by accessing member/method in PriorityQueue
 */
public class CarbonPriorityQueue<E> extends PriorityQueue<E> {
  private transient Object[] queue;
  private transient Method siftDownMethod;
  private static final long serialVersionUID = 1L;

  public CarbonPriorityQueue(int initialCapacity) {
    super(initialCapacity);

    try {
      Field field = PriorityQueue.class.getDeclaredField("queue");
      field.setAccessible(true);
      queue = (Object[]) field.get(this);
      siftDownMethod = PriorityQueue.class.getDeclaredMethod("siftDown", int.class, Object.class);
      siftDownMethod.setAccessible(true);
    } catch (ReflectiveOperationException e) {
      throw new CarbonDataLoadingException("Reflective operation failed", e);
    }
  }

  public void siftTopDown() {
    try {
      siftDownMethod.invoke(this, 0, queue[0]);
    } catch (ReflectiveOperationException e) {
      throw new CarbonDataLoadingException("Reflective operation failed", e);
    }
  }
}
