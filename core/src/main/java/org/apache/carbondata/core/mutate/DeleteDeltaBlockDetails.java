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

package org.apache.carbondata.core.mutate;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;

/**
 * This class stores the block details of delete delta file
 */
public class DeleteDeltaBlockDetails implements Serializable {

  private static final long serialVersionUID = 1206104914918495724L;

  private Map<String, DeleteDeltaBlockletDetails> blockletDetailsMap;
  private String blockName;

  /**
   * LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(DeleteDeltaBlockDetails.class.getName());

  public DeleteDeltaBlockDetails(String blockName) {
    this.blockName = blockName;
    blockletDetailsMap = new TreeMap<>();
  }

  @Override public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || !(obj instanceof DeleteDeltaBlockDetails)) return false;

    DeleteDeltaBlockDetails that = (DeleteDeltaBlockDetails) obj;

    return blockName.equals(that.blockName);

  }

  @Override public int hashCode() {
    return blockName.hashCode();
  }

  public List<DeleteDeltaBlockletDetails> getBlockletDetails() {

    List<DeleteDeltaBlockletDetails> deleteDeltaBlockletDetailsList = new ArrayList<>();
    Iterator<Map.Entry<String, DeleteDeltaBlockletDetails>> iterator =
        blockletDetailsMap.entrySet().iterator();
    while (iterator.hasNext()) {
      deleteDeltaBlockletDetailsList.add(iterator.next().getValue());
    }
    return deleteDeltaBlockletDetailsList;
  }

  public boolean addBlockletDetails(DeleteDeltaBlockletDetails blocklet) {
    DeleteDeltaBlockletDetails deleteDeltaBlockletDetails =
        blockletDetailsMap.get(blocklet.getBlockletKey());
    if (null == deleteDeltaBlockletDetails) {
      blockletDetailsMap.put(blocklet.getBlockletKey(), blocklet);
      return true;
    } else {
      deleteDeltaBlockletDetails.addDeletedRows(blocklet.getDeletedRows());
      return true;
    }
  }

  public boolean addBlocklet(String blockletId, String offset, Integer pageId) throws Exception {
    DeleteDeltaBlockletDetails blocklet = new DeleteDeltaBlockletDetails(blockletId, pageId);
    try {
      blocklet.addDeletedRow(CarbonUpdateUtil.getIntegerValue(offset));
      return addBlockletDetails(blocklet);
    } catch (Exception e) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(e.getMessage());
      }
      throw e;
    }

  }
}
