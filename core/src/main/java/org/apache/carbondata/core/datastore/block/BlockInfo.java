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
package org.apache.carbondata.core.datastore.block;

import java.io.Serializable;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.path.CarbonTablePath;

/**
 * Below class will be used to store table block info
 * As in blocklet distribution we are dividing the same block
 * in parts but in case of block loading blocklets belongs to same
 * block will be loaded together. This class will be used to store table block info
 * and equals and hash code method is used to identify blocklet belongs to same block
 */
public class BlockInfo implements Serializable {

  /**
   * table block info, stores all the details
   * about the block
   */
  private TableBlockInfo info;
  /**
   * unique blockName
   */
  private String blockUniqueName;

  /**
   * Constructor
   *
   * @param info
   */
  public BlockInfo(TableBlockInfo info) {
    this.info = info;
    init();
  }

  /**
   * init the block unique name
   */
  private void init() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(this.info.getSegmentId());
    stringBuilder.append(CarbonCommonConstants.FILE_SEPARATOR);
    stringBuilder.append(CarbonTablePath.getCarbonDataFileName(this.info.getFilePath()));
    this.blockUniqueName = stringBuilder.toString();
  }

  /**
   * @return table Block info
   */
  public TableBlockInfo getTableBlockInfo() {
    return info;
  }

  /**
   * To set the table block info
   *
   * @param info
   */
  public void setTableBlockInfo(TableBlockInfo info) {
    this.info = info;
  }

  /**
   * method to get the hash code
   */
  @Override public int hashCode() {
    int result = info.getFilePath().hashCode();
    result = 31 * result + (int) (info.getBlockOffset() ^ (info.getBlockOffset() >>> 32));
    result = 31 * result + (int) (info.getBlockLength() ^ (info.getBlockLength() >>> 32));
    result = 31 * result + info.getSegmentId().hashCode();
    return result;
  }

  /**
   * To check the equality
   *
   * @param obj
   */
  @Override public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof BlockInfo)) {
      return false;
    }
    BlockInfo other = (BlockInfo) obj;
    if (!info.getSegmentId().equals(other.info.getSegmentId())) {
      return false;
    }
    if (info.getBlockOffset() != other.info.getBlockOffset()) {
      return false;
    }
    if (info.getBlockLength() != other.info.getBlockLength()) {
      return false;
    }

    if (info.getFilePath() == null && other.info.getFilePath() != null) {
      return false;
    } else if (info.getFilePath() != null && other.info.getFilePath() == null) {
      return false;
    } else if (!info.getFilePath().equals(other.info.getFilePath())) {
      return false;
    }
    return true;
  }

  /**
   * returns unique blockname
   * @return
   */
  public String getBlockUniqueName() {
    return blockUniqueName;
  }
}
