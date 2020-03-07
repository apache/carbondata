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

package org.apache.carbondata.core.datamap.dev;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.core.datamap.dev.fgdatamap.FineGrainBlocklet;
import org.apache.carbondata.core.datastore.impl.FileFactory;

/**
 * A serializer/deserializer for {@link FineGrainBlocklet}, it is used after prune the data
 * by {@link FineGrainIndex}
 */
@InterfaceAudience.Internal
public class BlockletSerializer {

  /**
   * Serialize and write blocklet to the file.
   * @param grainBlocklet
   * @param writePath
   * @throws IOException
   */
  public void serializeBlocklet(FineGrainBlocklet grainBlocklet, String writePath)
      throws IOException {
    DataOutputStream dataOutputStream =
        FileFactory.getDataOutputStream(writePath);
    try {
      grainBlocklet.write(dataOutputStream);
    } finally {
      dataOutputStream.close();
    }
  }

  /**
   * Read data from filepath and deserialize blocklet.
   * @param writePath
   * @return
   * @throws IOException
   */
  public FineGrainBlocklet deserializeBlocklet(String writePath) throws IOException {
    DataInputStream inputStream =
        FileFactory.getDataInputStream(writePath);
    FineGrainBlocklet blocklet = new FineGrainBlocklet();
    try {
      blocklet.readFields(inputStream);
    } finally {
      inputStream.close();
    }
    return blocklet;
  }

}
