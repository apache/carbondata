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

/**
 *
 */
package org.apache.carbondata.spark.splits;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.spark.partition.api.Partition;

import org.apache.hadoop.io.Writable;


/**
 * It represents one region server as one split.
 */
public class TableSplit implements Serializable, Writable {
  private static final long serialVersionUID = -8058151330863145575L;

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(TableSplit.class.getName());
  private List<String> locations = new ArrayList<String>(CarbonCommonConstants.CONSTANT_SIZE_TEN);

  private Partition partition;

  /**
   * @return the locations
   */
  public List<String> getLocations() {
    return locations;
  }

  /**
   * @param locations the locations to set
   */
  public void setLocations(List<String> locations) {
    this.locations = locations;
  }

  /**
   * @return Returns the partitions.
   */
  public Partition getPartition() {
    return partition;
  }

  /**
   * @param partition The partitions to set.
   */
  public void setPartition(Partition partition) {
    this.partition = partition;
  }

  @Override public void readFields(DataInput in) throws IOException {

    int sizeLoc = in.readInt();
    for (int i = 0; i < sizeLoc; i++) {
      byte[] b = new byte[in.readInt()];
      in.readFully(b);
      locations.add(new String(b, Charset.defaultCharset()));
    }

    byte[] buf = new byte[in.readInt()];
    in.readFully(buf);
    ByteArrayInputStream bis = new ByteArrayInputStream(buf);
    ObjectInputStream ois = new ObjectInputStream(bis);
    try {
      partition = (Partition) ois.readObject();
    } catch (ClassNotFoundException e) {
      LOGGER.error(e, e.getMessage());
    }
    ois.close();
  }

  @Override public void write(DataOutput out) throws IOException {

    int sizeLoc = locations.size();
    out.writeInt(sizeLoc);
    for (int i = 0; i < sizeLoc; i++) {
      byte[] bytes = locations.get(i).getBytes(Charset.defaultCharset());
      out.writeInt(bytes.length);
      out.write(bytes);
    }

    ByteArrayOutputStream bos = new ByteArrayOutputStream();

    ObjectOutputStream obs = new ObjectOutputStream(bos);
    obs.writeObject(partition);
    obs.close();
    byte[] byteArray = bos.toByteArray();
    out.writeInt(byteArray.length);
    out.write(byteArray);
  }

  public String toString() {
    return partition.getUniqueID() + ' ' + locations;
  }
}
