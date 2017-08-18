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

import java.util.Map;

/**
 *  Codec for a column page data.
 */
public interface ColumnPageCodec {
  /**
   * Return the codec name
   */
  String getName();

  /**
   * Return a new Encoder which will be used to encode one column page.
   * This will be called for every column page
   */
  ColumnPageEncoder createEncoder(Map<String, String> parameter);

  /**
   * Return a new Decoder with specified metadata.
   * This will be called for every column page
   */
  ColumnPageDecoder createDecoder(ColumnPageEncoderMeta meta);
}
