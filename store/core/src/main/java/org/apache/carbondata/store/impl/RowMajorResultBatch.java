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

package org.apache.carbondata.store.impl;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.apache.carbondata.store.devapi.ResultBatch;

public class RowMajorResultBatch<T> implements ResultBatch<T> {

  private Iterator<T> iterator;

  RowMajorResultBatch(List<T> rows) {
    Objects.requireNonNull(rows);
    this.iterator = rows.iterator();
  }

  @Override
  public boolean isColumnar() {
    return false;
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public T next() {
    return iterator.next();
  }
}
