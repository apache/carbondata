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

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;

/**
 * Class : Holds the info to uniquely identify a blocks
 */
public class TableBlockUniqueIdentifier {

  /**
   * table fully qualified name
   */
  private AbsoluteTableIdentifier absoluteTableIdentifier;

  /**
   * table block info
   */
  private TableBlockInfo tableBlockInfo;

  public TableBlockUniqueIdentifier(AbsoluteTableIdentifier absoluteTableIdentifier,
      TableBlockInfo tableBlockInfo) {
    this.absoluteTableIdentifier = absoluteTableIdentifier;
    this.tableBlockInfo = tableBlockInfo;
  }

  public AbsoluteTableIdentifier getAbsoluteTableIdentifier() {
    return absoluteTableIdentifier;
  }

  public void setAbsoluteTableIdentifier(AbsoluteTableIdentifier absoluteTableIdentifier) {
    this.absoluteTableIdentifier = absoluteTableIdentifier;
  }

  public TableBlockInfo getTableBlockInfo() {
    return tableBlockInfo;
  }

  public void setTableBlockInfo(TableBlockInfo tableBlockInfo) {
    this.tableBlockInfo = tableBlockInfo;
  }

  @Override public int hashCode() {
    return this.absoluteTableIdentifier.hashCode() + this.tableBlockInfo.hashCode();
  }

  @Override public boolean equals(Object other) {
    if (this == other) return true;
    if (other == null || getClass() != other.getClass()) return false;
    TableBlockUniqueIdentifier tableBlockUniqueIdentifier = (TableBlockUniqueIdentifier) other;
    return this.absoluteTableIdentifier.equals(tableBlockUniqueIdentifier.absoluteTableIdentifier)
        && this.tableBlockInfo.equals(tableBlockUniqueIdentifier.tableBlockInfo);
  }

  /**
   * returns the String value to uniquely identify a block
   *
   * @return
   */
  public String getUniqueTableBlockName() {
    BlockInfo blockInfo = new BlockInfo(this.tableBlockInfo);
    CarbonTableIdentifier carbonTableIdentifier =
        this.absoluteTableIdentifier.getCarbonTableIdentifier();
    return carbonTableIdentifier.getDatabaseName()
        + CarbonCommonConstants.FILE_SEPARATOR + carbonTableIdentifier
        .getDatabaseName() + CarbonCommonConstants.FILE_SEPARATOR
        + this.tableBlockInfo.getSegmentId()
        + CarbonCommonConstants.FILE_SEPARATOR + blockInfo.hashCode();
  }
}
