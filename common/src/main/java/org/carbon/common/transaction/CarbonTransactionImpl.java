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
package org.carbon.common.transaction;

import java.util.ArrayList;
import java.util.List;

/**
 * Carbon transaction management
 * @param <V>
 */
public class CarbonTransactionImpl<V> implements CarbonTransaction<V> {

  private List<Task<V>> taskList = new ArrayList<>();
  private List<V> results = new ArrayList<>();

  @Override public void addTask(Task<V> task) {
    taskList.add(task);
  }

  @Override public void executeTasks() {
    for (Task<V> task : taskList) {
      results.add(task.execute());
    }
  }

  @Override public void commit() {
    for (Task<V> task : taskList) {
      task.commit();
    }
  }

  @Override public V get(int taskNo) {
    return results.get(taskNo);
  }

}
