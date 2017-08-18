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

package org.apache.carbondata.core.datastore.page.encoding;

import java.io.IOException;

import org.apache.carbondata.core.datastore.TableSpec;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.metadata.ValueEncoderMeta;
import org.apache.carbondata.format.Encoding;

/**
 * Base class for encoding strategy implementation.
 */
public abstract class EncodingStrategy {

  /**
   * Return new encoder for specified column
   */
  public abstract ColumnPageEncoder createEncoder(TableSpec.ColumnSpec columnSpec,
      ColumnPage inputPage);

  /**
   * Return new decoder for column. Decoder is created based on specified metadata
   */
  public abstract ColumnPageDecoder createDecoder(Encoding encoding,
      ValueEncoderMeta meta) throws IOException;
}
