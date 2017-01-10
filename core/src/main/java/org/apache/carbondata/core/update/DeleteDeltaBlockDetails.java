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

package org.apache.carbondata.core.update;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;

/**
 * This class stores the block details of delete delta file
 */
public class DeleteDeltaBlockDetails implements Serializable {

  private static final long serialVersionUID = 1206104914918495724L;

  private List<DeleteDeltaBlockletDetails> blockletDetails;
  private String blockName;

  /**
   * LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(DeleteDeltaBlockDetails.class.getName());

  public DeleteDeltaBlockDetails(String blockName) {
    this.blockName = blockName;
    blockletDetails = new ArrayList<DeleteDeltaBlockletDetails>();
  }

  public String getBlockName() {
    return blockName;
  }

  public void setBlockName(String blockName) {
    this.blockName = blockName;
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
    return blockletDetails;
  }

  public boolean addBlockletDetails(DeleteDeltaBlockletDetails blocklet) {
    int index = blockletDetails.indexOf(blocklet);
    if (blockletDetails.isEmpty() || index == -1) {
      return blockletDetails.add(blocklet);
    } else {
      return blockletDetails.get(index).addDeletedRows(blocklet.getDeletedRows());
    }
  }

  public boolean addBlocklet(String blockletId, String offset) throws Exception {
    DeleteDeltaBlockletDetails blocklet = new DeleteDeltaBlockletDetails(blockletId);
    try {
      blocklet.addDeletedRow(CarbonUpdateUtil.getIntegerValue(offset));
      return addBlockletDetails(blocklet);
    } catch (Exception e) {
      LOGGER.debug(e.getMessage());
      throw e;
    }

  }
}
