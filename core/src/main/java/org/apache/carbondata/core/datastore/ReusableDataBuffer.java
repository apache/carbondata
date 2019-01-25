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

package org.apache.carbondata.core.datastore;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.core.constants.CarbonCommonConstants;

/**
 * class holds the reusable data buffer based on request it will resize.
 * If request size is less it will return the same buffer, in case it is more
 * it will resize and update the buffer
 */
@InterfaceAudience.Internal
@InterfaceStability.Evolving
public class ReusableDataBuffer {
  /**
   * reusable byte array
   */
  private byte[] dataBuffer;

  /**
   * current size of data buffer
   */
  private int size;

  /**
   * below method will be used to get the data buffer based on size
   * If requested size is less it will return same buffer, if size is more
   * it resize the buffer and return
   * @param requestedSize
   * @return databuffer
   */
  public byte[] getDataBuffer(int requestedSize) {
    if (dataBuffer == null || requestedSize > size) {
      // increase by 30% only if the requestedSize less than 10 MB
      // otherwise take the original requestedSize.
      if (requestedSize < CarbonCommonConstants.REQUESTED_PAGE_SIZE_MAX) {
        this.size = requestedSize + ((requestedSize * 30) / 100);
      } else {
        this.size = requestedSize;
      }
      dataBuffer = new byte[size];
    }
    return dataBuffer;
  }
}
