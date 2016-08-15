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

package org.apache.carbondata.processing.dataprocessor.queue;

public interface Queue<E> {
  /**
   * This method insert the element in the queue, and return false if it fails
   * to isert in the queue.
   *
   * @param obj
   * @return true if insererted properly false otherwise.
   */
  boolean offer(E obj);

  /**
   * This method get the element from the head of the queue and
   * remove the element from the queue.
   *
   * @return
   */
  E poll();

  /**
   * This method just return the element in the head of the queue.
   *
   * @return
   */
  E peek();

}
