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

package org.carbondata.processing.threadbasedmerger.iterator;

import org.carbondata.processing.threadbasedmerger.container.Container;

public class RecordIterator {
  /**
   * producer container
   */
  private final Container producerContainer;

  /**
   * record holder array
   */
  private Object[][] sortHolderArray;

  /**
   * record counter
   */
  private int counter;

  /**
   * holder size
   */
  private int size;

  /**
   * ProducerIterator constructor
   *
   * @param producerContainer
   */
  public RecordIterator(Container producerContainer) {
    this.producerContainer = producerContainer;
  }

  /**
   * below method will be used to increment the counter
   */
  public void next() {
    counter++;
  }

  /**
   * below method will be used to get the row from holder
   *
   * @return
   */
  public Object[] getRow() {
    return sortHolderArray[counter];
  }

  /**
   * has next method will be used to check any more row is present or not
   *
   * @return is row present
   */
  public boolean hasNext() {
    if (counter >= size) {
      if (!producerContainer.isDone()) {
        sortHolderArray = producerContainer.getContainerData();
        if (null == sortHolderArray) {
          return false;
        }
        counter = 0;
        size = sortHolderArray.length;
        synchronized (producerContainer) {
          this.producerContainer.setFilled(false);
          producerContainer.notifyAll();
        }
      } else {
        return false;
      }
    }
    return true;
  }
}
