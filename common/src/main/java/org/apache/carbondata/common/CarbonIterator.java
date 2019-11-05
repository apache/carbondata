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

package org.apache.carbondata.common;

import java.util.Iterator;

/**
 * CarbonIterator adds default implement for remove. This is required for Java 7.
 * @param <E>
 */
public abstract class CarbonIterator<E> implements Iterator<E> {

  @Override
  public abstract boolean hasNext();

  @Override
  public abstract E next();

  @Override
  public void remove() {
    throw new UnsupportedOperationException("remove");
  }

  /**
   * Initialize the iterator
   */
  public void initialize() {
    // sub classes can overwrite to provide initialize logic to this method
  }

  /**
   * Close the resources
   */
  public void close() {
    // sub classes can overwrite to provide close logic to this method
  }

}
