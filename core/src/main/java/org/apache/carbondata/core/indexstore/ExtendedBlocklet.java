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
package org.apache.carbondata.core.indexstore;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.indexstore.row.DataMapRow;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.stream.ExtendedByteArrayOutputStream;
import org.apache.carbondata.hadoop.CarbonInputSplit;

/**
 * Detailed blocklet information
 */
public class ExtendedBlocklet extends Blocklet {

  private String dataMapUniqueId;

  private CarbonInputSplit inputSplit;

  public ExtendedBlocklet() {

  }

  public ExtendedBlocklet(String filePath, String blockletId,
      boolean compareBlockletIdForObjectMatching, ColumnarFormatVersion version) {
    super(filePath, blockletId, compareBlockletIdForObjectMatching);
    try {
      this.inputSplit = CarbonInputSplit.from(null, blockletId, filePath, 0, -1, version, null);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public ExtendedBlocklet(String filePath, String blockletId, ColumnarFormatVersion version) {
    this(filePath, blockletId, true, version);
  }

  public BlockletDetailInfo getDetailInfo() {
    return this.inputSplit.getDetailInfo();
  }

  public void setDataMapRow(DataMapRow dataMapRow) {
    this.inputSplit.setDataMapRow(dataMapRow);
  }

  public String[] getLocations() {
    try {
      return this.inputSplit.getLocations();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public long getLength() {
    return this.inputSplit.getLength();
  }

  public String getSegmentId() {
    return this.inputSplit.getSegmentId();
  }

  public Segment getSegment() {
    return this.inputSplit.getSegment();
  }
  public void setSegment(Segment segment) {
    this.inputSplit.setSegment(segment);
  }

  public String getPath() {
    return getFilePath();
  }

  public String getDataMapWriterPath() {
    return this.inputSplit.getDataMapWritePath();
  }

  public void setDataMapWriterPath(String dataMapWriterPath) {
    this.inputSplit.setDataMapWritePath(dataMapWriterPath);
  }

  public String getDataMapUniqueId() {
    return dataMapUniqueId;
  }

  public void setDataMapUniqueId(String dataMapUniqueId) {
    this.dataMapUniqueId = dataMapUniqueId;
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) {
      return false;
    }

    ExtendedBlocklet that = (ExtendedBlocklet) o;
    return inputSplit.getSegmentId() != null ?
        inputSplit.getSegmentId().equals(that.inputSplit.getSegmentId()) :
        that.inputSplit.getSegmentId() == null;
  }

  @Override public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (inputSplit.getSegmentId() != null ?
        inputSplit.getSegmentId().hashCode() :
        0);
    return result;
  }

  public CarbonInputSplit getInputSplit() {
    return inputSplit;
  }

  public void setColumnCardinality(int[] cardinality) {
    inputSplit.setColumnCardinality(cardinality);
  }

  public void setLegacyStore(boolean isLegacyStore) {
    inputSplit.setLegacyStore(isLegacyStore);
  }

  public void setUseMinMaxForPruning(boolean useMinMaxForPruning) {
    this.inputSplit.setUseMinMaxForPruning(useMinMaxForPruning);
  }

  public void setIsBlockCache(boolean isBlockCache) {
    this.inputSplit.setIsBlockCache(isBlockCache);
  }

  public void setColumnSchema(List<ColumnSchema> columnSchema) {
    this.inputSplit.setColumnSchema(columnSchema);
  }

  /**
   * Method to seralize extended blocklet and inputsplit for index server
   * DataFormat
   * <Extended Blocklet data><Carbon input split serializeData lenght><CarbonInputSplitData>
   * @param out
   * @param uniqueLocation
   * @throws IOException
   */
  public void serializeData(DataOutput out, Map<String, Short> uniqueLocation)
      throws IOException {
    super.write(out);
    if (dataMapUniqueId == null) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      out.writeUTF(dataMapUniqueId);
    }
    out.writeBoolean(inputSplit != null);
    if (inputSplit != null) {
      // creating byte array output stream to get the size of input split serializeData size
      ExtendedByteArrayOutputStream ebos = new ExtendedByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(ebos);
      inputSplit.setFilePath(null);
      inputSplit.setBucketId(null);
      if (inputSplit.isBlockCache()) {
        inputSplit.updateFooteroffset();
        inputSplit.updateBlockLength();
        inputSplit.setWriteDetailInfo(false);
      }
      inputSplit.serializeFields(dos, uniqueLocation);
      out.writeInt(ebos.size());
      out.write(ebos.getBuffer(), 0 , ebos.size());
    }
  }

  /**
   * Method to deseralize extended blocklet and inputsplit for index server
   * @param in
   * @param locations
   * @param tablePath
   * @throws IOException
   */
  public void deserializeFields(DataInput in, String[] locations, String tablePath)
      throws IOException {
    super.readFields(in);
    if (in.readBoolean()) {
      dataMapUniqueId = in.readUTF();
    }
    setFilePath(tablePath + getPath());
    boolean isSplitPresent = in.readBoolean();
    if (isSplitPresent) {
      // getting the length of the data
      final int serializeLen = in.readInt();
      this.inputSplit =
          new CarbonInputSplit(serializeLen, in, getFilePath(), locations, getBlockletId());
    }
  }
}
